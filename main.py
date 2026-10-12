"""GitHub relay: watches DATA_REPO/requests/*.json, fetches each URL,
and commits the output to DATA_REPO/results/.

Request file  requests/<id>.json : {"url": "https://...", "render": false}
Result files  results/<id>.json  (metadata, written LAST = "done" marker)
              results/<id>.html + results/<id>.txt   (pages)
              results/<id>.<ext>                      (other content)
"""
import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import os
import re
import socket
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Request

GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
WEBHOOK_SECRET = os.environ["WEBHOOK_SECRET"].encode()
DATA_REPO = os.environ["DATA_REPO"]  # e.g. "GaweshGee/relay-data"
BRANCH = os.environ.get("DATA_BRANCH", "main")
MAX_BYTES = int(os.environ.get("MAX_BYTES", 5_000_000))

API = "https://api.github.com"
GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
}
UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)
SAFE_NAME = re.compile(r"^[A-Za-z0-9._-]+\.json$")

app = FastAPI()
lock = asyncio.Lock()


# ---------- safety ----------
def host_is_safe(host: str) -> bool:
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return False
    for info in infos:
        ip = ipaddress.ip_address(info[4][0].split("%")[0])
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_reserved or ip.is_multicast or ip.is_unspecified):
            return False
    return True


async def url_is_safe(url: str) -> bool:
    if url.startswith(("data:", "blob:")):
        return True
    p = urlparse(url)
    if p.scheme not in ("http", "https") or not p.hostname:
        return False
    return await asyncio.to_thread(host_is_safe, p.hostname)


# ---------- GitHub helpers ----------
def _contents_url(path: str) -> str:
    return f"{API}/repos/{DATA_REPO}/contents/{path}"


async def gh_list(directory: str):
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(_contents_url(directory), headers=GH_HEADERS,
                        params={"ref": BRANCH})
        if r.status_code == 404:
            return []
        r.raise_for_status()
        return [i["name"] for i in r.json() if i["type"] == "file"]


async def gh_get(path: str) -> bytes:
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.get(_contents_url(path), headers=GH_HEADERS,
                        params={"ref": BRANCH})
        r.raise_for_status()
        return base64.b64decode(r.json()["content"])


async def gh_put(path: str, data: bytes, message: str):
    async with httpx.AsyncClient(timeout=60) as c:
        for _ in range(3):
            sha = None
            g = await c.get(_contents_url(path), headers=GH_HEADERS,
                            params={"ref": BRANCH})
            if g.status_code == 200:
                sha = g.json()["sha"]
            body = {"message": message,
                    "content": base64.b64encode(data).decode(),
                    "branch": BRANCH}
            if sha:
                body["sha"] = sha
            r = await c.put(_contents_url(path), headers=GH_HEADERS, json=body)
            if r.status_code in (200, 201):
                return
            if r.status_code in (409, 422):
                await asyncio.sleep(1)
                continue
            r.raise_for_status()
        raise RuntimeError(f"could not write {path}")


# ---------- fetchers ----------
async def fetch_http(url: str):
    async with httpx.AsyncClient(timeout=30, headers={"User-Agent": UA}) as c:
        for _ in range(6):
            if not await url_is_safe(url):
                raise ValueError("blocked url (non-public or invalid)")
            async with c.stream("GET", url) as r:
                if r.is_redirect and r.headers.get("location"):
                    url = str(httpx.URL(url).join(r.headers["location"]))
                    continue
                buf = bytearray()
                truncated = False
                async for chunk in r.aiter_bytes():
                    buf += chunk
                    if len(buf) > MAX_BYTES:
                        truncated = True
                        break
                return (r.status_code, url, r.headers.get("content-type", ""),
                        bytes(buf[:MAX_BYTES]), truncated)
        raise ValueError("too many redirects")


async def fetch_render(url: str):
    from playwright.async_api import async_playwright

    if not await url_is_safe(url):
        raise ValueError("blocked url (non-public or invalid)")
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            args=["--no-sandbox", "--disable-dev-shm-usage"])
        try:
            ctx = await browser.new_context(user_agent=UA)
            page = await ctx.new_page()

            async def guard(route):
                if await url_is_safe(route.request.url):
                    await route.continue_()
                else:
                    await route.abort()

            await page.route("**/*", guard)
            resp = await page.goto(url, wait_until="domcontentloaded",
                                   timeout=45000)
            try:
                await page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            html = (await page.content()).encode()
            return (resp.status if resp else None, page.url,
                    "text/html; charset=utf-8", html[:MAX_BYTES],
                    len(html) > MAX_BYTES)
        finally:
            await browser.close()


def html_to_text(html: bytes) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    text = soup.get_text("\n")
    return re.sub(r"\n\s*\n+", "\n\n", text).strip()


def ext_for(ctype: str, body: bytes) -> str:
    c = ctype.lower()
    if "html" in c or body[:200].lstrip().lower().startswith((b"<!doctype html", b"<html")):
        return "html"
    if "json" in c:
        return "json.txt"  # avoid clashing with <id>.json metadata
    if "xml" in c:
        return "xml"
    if c.startswith("text/"):
        return "txt"
    if "pdf" in c:
        return "pdf"
    if "png" in c:
        return "png"
    if "jpeg" in c or "jpg" in c:
        return "jpg"
    return "bin"


# ---------- core ----------
async def process(name: str):
    rid = name[:-5]
    meta = {"id": rid, "fetched_at": datetime.now(timezone.utc).isoformat()}
    written = []
    try:
        req = json.loads(await gh_get(f"requests/{name}"))
        url = req["url"]
        render = bool(req.get("render", False))
        meta.update(url=url, render=render)
        if render:
            status, final, ctype, body, trunc = await fetch_render(url)
        else:
            status, final, ctype, body, trunc = await fetch_http(url)
        ext = ext_for(ctype, body)
        meta.update(status=status, final_url=final, content_type=ctype,
                    bytes=len(body), truncated=trunc)
        files = {f"{rid}.{ext}": body}
        if ext == "html":
            files[f"{rid}.txt"] = html_to_text(body).encode()
        for fname, data in files.items():
            await gh_put(f"results/{fname}", data, f"result: {fname}")
            written.append(fname)
    except Exception as e:  # noqa: BLE001
        meta["error"] = f"{type(e).__name__}: {e}"
    meta["files"] = written
    await gh_put(f"results/{rid}.json",
                 json.dumps(meta, indent=2).encode(), f"result: {rid}.json")


async def sweep():
    async with lock:
        try:
            reqs = [n for n in await gh_list("requests") if SAFE_NAME.match(n)]
            done = set(await gh_list("results"))
            for name in reqs:
                if name not in done:  # results/<id>.json is the done marker
                    await process(name)
        except Exception as e:  # noqa: BLE001
            print("sweep error:", repr(e))


# ---------- routes ----------
@app.on_event("startup")
async def on_startup():
    asyncio.create_task(sweep())  # catches requests missed while asleep


@app.get("/health")
async def health():
    return {"ok": True}


@app.post("/webhook")
async def webhook(request: Request):
    raw = await request.body()
    sig = request.headers.get("X-Hub-Signature-256", "")
    expected = "sha256=" + hmac.new(WEBHOOK_SECRET, raw, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="bad signature")
    event = request.headers.get("X-GitHub-Event", "")
    if event == "ping":
        return {"pong": True}
    if event == "push":
        asyncio.create_task(sweep())
    return {"accepted": True}


@app.post("/sweep")
async def manual_sweep(request: Request):
    if not hmac.compare_digest(request.headers.get("X-Relay-Secret", ""),
                               WEBHOOK_SECRET.decode()):
        raise HTTPException(status_code=401, detail="unauthorized")
    asyncio.create_task(sweep())
    return {"accepted": True}
