"""
NewPipe Service v2.0 — P-Stream Trailer & Video Extractor
==========================================================
yt-dlp powered FastAPI microservice. No API keys. No quotas.

Endpoints:
  GET /health              — liveness check
  GET /search              — search YouTube for trailers
  GET /extract             — extract stream URL from video page
  GET /trailer             — search + extract in ONE call (primary path)
  GET /info                — metadata only (fast)
  POST /batch              — extract multiple URLs concurrently
  GET /cache/stats         — cache hit/miss statistics
  GET /admin               — HTML admin dashboard
"""

from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import asyncio
import functools
import re
import os
import time
import logging
import importlib.metadata
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("newpipe")

# Log version at startup so HF Space logs show what yt-dlp is running
try:
    _YTDLP_VER = importlib.metadata.version("yt-dlp")
except Exception:
    _YTDLP_VER = getattr(getattr(yt_dlp, "version", None), "__version__", "unknown")

log.info(f"🚀 NewPipe Service starting — yt-dlp {_YTDLP_VER}")

# Thread pool — yt-dlp is blocking; run in executor to not block event loop
_executor = ThreadPoolExecutor(max_workers=4)

app = FastAPI(
    title="P-Stream NewPipe Service",
    description="yt-dlp powered video/trailer extractor for P-Stream",
    version="2.0.0",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = [
    "https://pstream.watch",
    "https://www.pstream.watch",
    "https://pstream-frontend.pages.dev",
    "https://ibrahimar397-pstream-giga.hf.space",
    "http://localhost:5173",
    "http://localhost:3000",
    "http://localhost:4173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ─── Cache ────────────────────────────────────────────────────────────────────
_cache: dict[str, tuple[float, any]] = {}
_stats = {"hits": 0, "misses": 0, "errors": 0, "requests": 0}
CACHE_TTL        = 3600   # 1 hour for search results
EXTRACT_CACHE_TTL = 1800  # 30 min for stream URLs (they expire)

def cache_get(key: str, ttl: int = CACHE_TTL):
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < ttl:
            _stats["hits"] += 1
            return val
    _stats["misses"] += 1
    return None

def cache_set(key: str, val, ttl_override: int | None = None):
    _cache[key] = (time.time(), val)
    if len(_cache) > 500:
        oldest = sorted(_cache.items(), key=lambda x: x[1][0])[:100]
        for k, _ in oldest:
            del _cache[k]

# In-flight dedup — prevents duplicate yt-dlp calls for same URL
_inflight: dict[str, asyncio.Future] = {}

# ─── yt-dlp helpers ───────────────────────────────────────────────────────────

# Optional proxy — set PROXY_URL secret in HF Space settings to bypass HF's
# YouTube network block (SSL: UNEXPECTED_EOF_WHILE_READING).
# Format: http://user:pass@host:port  OR  socks5://host:port
_PROXY_URL = os.environ.get("PROXY_URL", "").strip() or None
if _PROXY_URL:
    log.info(f"🌐 Using proxy: {_PROXY_URL[:30]}...")
else:
    log.warning("⚠️  No PROXY_URL set — HF may block direct YouTube connections")

def _ydl_opts_base() -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "socket_timeout": 15,
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        },
    }
    if _PROXY_URL:
        opts["proxy"] = _PROXY_URL
    return opts

async def _run(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, functools.partial(fn, *args))

def _search_youtube(query: str, max_results: int = 8) -> list[dict]:
    opts = _ydl_opts_base()
    opts["extract_flat"] = True
    opts["playlistend"] = max_results
    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
        entries = info.get("entries") or []
        return [
            {
                "id":       e.get("id"),
                "title":    e.get("title"),
                "url":      e.get("url") or f"https://www.youtube.com/watch?v={e.get('id')}",
                "duration": e.get("duration"),
                "views":    e.get("view_count"),
                "channel":  e.get("channel") or e.get("uploader"),
                "thumb":    e.get("thumbnail") or (f"https://i.ytimg.com/vi/{e.get('id')}/hqdefault.jpg" if e.get("id") else None),
            }
            for e in entries if e
        ]

def _extract_stream(url: str) -> dict:
    opts = _ydl_opts_base()
    # Prefer progressive mp4 (audio+video in one file — works in <video> tag directly)
    opts["format"] = (
        "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]"
        "/bestvideo[height<=720][ext=mp4]+bestaudio/best[height<=720]/best"
    )
    opts["merge_output_format"] = "mp4"
    opts["writesubtitles"]      = True
    opts["writeautomaticsub"]   = True
    opts["subtitleslangs"]      = ["en", "en-US", "en-GB"]

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        if not info:
            return {}

        formats = info.get("formats") or []

        # 1. Try progressive mp4 (video+audio combined — browser native compatible)
        progressive = [
            f for f in formats
            if f.get("vcodec") not in (None, "none")
            and f.get("acodec") not in (None, "none")
            and f.get("ext") == "mp4"
        ]
        progressive.sort(key=lambda f: (f.get("height") or 0), reverse=True)

        # 2. Fallback: best mp4 regardless of codec split
        best_mp4 = next(
            (f for f in sorted(formats, key=lambda f: f.get("height") or 0, reverse=True)
             if f.get("ext") == "mp4" and f.get("url")), None
        )

        best = progressive[0] if progressive else best_mp4
        stream_url = (best or {}).get("url") or info.get("url")
        height     = (best or {}).get("height") or info.get("height")

        # Subtitle collection
        raw_subs  = info.get("subtitles") or {}
        auto_subs = info.get("automatic_captions") or {}
        subtitles: dict = {}

        def _pick_vtt(tracks: list) -> str | None:
            for t in tracks:
                if t.get("ext") == "vtt":
                    return t.get("url")
            return tracks[0].get("url") if tracks else None

        for lang in ["en", "en-US", "en-GB"]:
            if lang in raw_subs and raw_subs[lang]:
                url_vtt = _pick_vtt(raw_subs[lang])
                if url_vtt:
                    subtitles[lang] = {"url": url_vtt, "auto": False}
                    continue
            if lang in auto_subs and auto_subs[lang]:
                url_vtt = _pick_vtt(auto_subs[lang])
                if url_vtt:
                    subtitles[lang] = {"url": url_vtt, "auto": True}

        return {
            "id":         info.get("id"),
            "title":      info.get("title"),
            "stream_url": stream_url,
            "quality":    f"{height}p" if height else "unknown",
            "ext":        (best or {}).get("ext") or "mp4",
            "duration":   info.get("duration"),
            "thumb":      info.get("thumbnail"),
            "channel":    info.get("channel") or info.get("uploader"),
            "platform":   info.get("extractor_key") or "unknown",
            "subtitles":  subtitles,
        }

# ─── Trailer scoring ──────────────────────────────────────────────────────────

def _score(result: dict, title: str, year: str | None, media_type: str) -> float:
    score = 0.0
    t = (result.get("title") or "").lower()
    tl = title.lower()
    if tl in t:            score += 10
    if "official trailer" in t: score += 8
    elif "trailer" in t:   score += 5
    if "teaser" in t:      score += 3
    if year and year in t: score += 3
    if media_type == "tv" and any(w in t for w in ["season", "series"]): score += 1
    dur = result.get("duration") or 0
    if 45 <= dur <= 300:   score += 3
    elif dur > 600:        score -= 4
    views = result.get("views") or 0
    if views > 5_000_000:  score += 3
    elif views > 1_000_000: score += 1
    ch = (result.get("channel") or "").lower()
    if any(w in ch for w in ["official", "pictures", "entertainment", "studios", "films"]): score += 2
    return score

# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "ok":              True,
        "service":         "newpipe",
        "version":         "2.0.0",
        "yt_dlp_version":  _YTDLP_VER,
        "cache_entries":   len(_cache),
        "cache_hits":      _stats["hits"],
        "cache_misses":    _stats["misses"],
        "uptime_requests": _stats["requests"],
    }

@app.get("/search")
async def search(
    q:     str        = Query(...),
    type:  str        = Query("movie"),
    year:  str | None = Query(None),
    limit: int        = Query(5, ge=1, le=10),
):
    _stats["requests"] += 1
    ck = f"search:{q}:{type}:{year}"
    if (cached := cache_get(ck)):
        return cached
    try:
        query = q if "trailer" in q.lower() else f"{q} official trailer"
        raw   = await _run(_search_youtube, query, 12)
    except Exception as e:
        _stats["errors"] += 1
        raise HTTPException(502, f"yt-dlp search failed: {e}")

    title_guess = re.sub(r"\d{4}.*$", "", q).strip()
    scored = sorted(raw, key=lambda r: _score(r, title_guess, year, type), reverse=True)
    result = {"results": scored[:limit], "query": q, "total": len(scored)}
    cache_set(ck, result)
    return result

@app.get("/extract")
async def extract(url: str = Query(...)):
    _stats["requests"] += 1
    ck = f"extract:{url}"
    if (cached := cache_get(ck, EXTRACT_CACHE_TTL)):
        return cached

    # In-flight dedup
    if ck in _inflight:
        return await _inflight[ck]

    loop = asyncio.get_event_loop()
    fut  = loop.create_future()
    _inflight[ck] = fut
    try:
        info = await _run(_extract_stream, url)
    except yt_dlp.utils.DownloadError as e:
        _stats["errors"] += 1
        _inflight.pop(ck, None)
        raise HTTPException(502, f"Extraction failed: {e}")
    except Exception as e:
        _stats["errors"] += 1
        _inflight.pop(ck, None)
        raise HTTPException(500, f"Internal error: {e}")

    if not info.get("stream_url"):
        _inflight.pop(ck, None)
        raise HTTPException(404, "No stream URL found")

    cache_set(ck, info)
    fut.set_result(info)
    _inflight.pop(ck, None)
    return info

@app.get("/trailer")
async def trailer(
    title: str        = Query(..., description="Movie or show title"),
    year:  str | None = Query(None),
    type:  str        = Query("movie", description="'movie' or 'tv'"),
):
    """
    Combined search + extract in ONE call.
    Primary endpoint for NativeTrailerPlayer — avoids 2 round trips.
    """
    _stats["requests"] += 1
    ck = f"trailer:{title}:{year}:{type}"
    if (cached := cache_get(ck, EXTRACT_CACHE_TTL)):
        return cached

    # Step 1: search
    query = f"{title} {year or ''} official trailer".strip()
    try:
        raw = await _run(_search_youtube, query, 8)
    except Exception as e:
        _stats["errors"] += 1
        raise HTTPException(502, f"Search failed: {e}")

    title_guess = title
    scored = sorted(raw, key=lambda r: _score(r, title_guess, year, type), reverse=True)
    if not scored:
        raise HTTPException(404, "No trailer candidates found")

    top = scored[0]
    video_url = top.get("url") or f"https://www.youtube.com/watch?v={top['id']}"

    # Step 2: extract
    try:
        info = await _run(_extract_stream, video_url)
    except Exception as e:
        _stats["errors"] += 1
        raise HTTPException(502, f"Extraction failed: {e}")

    if not info.get("stream_url"):
        raise HTTPException(404, "No stream URL extracted")

    result = {**info, "search_title": top.get("title"), "search_rank": 0}
    cache_set(ck, result)
    return result

@app.post("/batch")
async def batch(urls: list[str]):
    """Extract multiple URLs concurrently (max 5)."""
    _stats["requests"] += 1
    if len(urls) > 5:
        raise HTTPException(400, "Max 5 URLs per batch")

    async def _do(url):
        try:
            ck = f"extract:{url}"
            if (c := cache_get(ck, EXTRACT_CACHE_TTL)):
                return {"url": url, "ok": True, **c}
            info = await _run(_extract_stream, url)
            cache_set(ck, info)
            return {"url": url, "ok": True, **info}
        except Exception as e:
            return {"url": url, "ok": False, "error": str(e)}

    results = await asyncio.gather(*[_do(u) for u in urls])
    return {"results": list(results)}

@app.get("/info")
async def info(url: str = Query(...)):
    _stats["requests"] += 1
    ck = f"info:{url}"
    if (cached := cache_get(ck)):
        return cached
    try:
        opts = _ydl_opts_base()
        opts["extract_flat"] = True
        with yt_dlp.YoutubeDL(opts) as ydl:
            d = ydl.extract_info(url, download=False)
        meta = {"id": d.get("id"), "title": d.get("title"), "duration": d.get("duration"),
                "views": d.get("view_count"), "channel": d.get("channel") or d.get("uploader"),
                "thumb": d.get("thumbnail"), "platform": d.get("extractor_key") or "unknown",
                "tags": (d.get("tags") or [])[:10]}
    except Exception as e:
        raise HTTPException(502, f"Info fetch failed: {e}")
    cache_set(ck, meta)
    return meta

@app.get("/cache/stats")
def cache_stats():
    now = time.time()
    active = sum(1 for ts, _ in _cache.values() if now - ts < CACHE_TTL)
    return {**_stats, "total_entries": len(_cache), "active_entries": active,
            "cache_ttl_s": CACHE_TTL, "extract_ttl_s": EXTRACT_CACHE_TTL}

@app.delete("/cache")
def cache_clear():
    _cache.clear()
    return {"ok": True, "message": "Cache cleared"}

# ─── Admin HTML Dashboard ─────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin():
    h = _stats["hits"]; m = _stats["misses"]; total = h + m
    hit_rate = round(h / total * 100) if total else 0
    return HTMLResponse(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NewPipe Service — Admin</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:#0a0a0a;color:#e5e5e5;font-family:'Consolas',monospace;min-height:100vh;padding:32px 24px}}
  h1{{color:#e50914;font-size:1.4rem;font-weight:700;letter-spacing:-.5px;margin-bottom:4px}}
  .sub{{color:#555;font-size:.75rem;margin-bottom:32px}}
  .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:32px}}
  .card{{background:#141414;border:1px solid #1f1f1f;border-radius:12px;padding:20px}}
  .card .label{{font-size:.65rem;color:#555;text-transform:uppercase;letter-spacing:2px;margin-bottom:8px}}
  .card .val{{font-size:2rem;font-weight:700;color:#fff}}
  .card .val.red{{color:#e50914}}
  .card .val.green{{color:#22c55e}}
  table{{width:100%;border-collapse:collapse;background:#141414;border-radius:12px;overflow:hidden;font-size:.78rem}}
  th{{background:#1a1a1a;color:#555;text-align:left;padding:10px 14px;font-size:.65rem;text-transform:uppercase;letter-spacing:1.5px}}
  td{{padding:10px 14px;border-top:1px solid #1a1a1a;color:#aaa}}
  td code{{color:#e50914;background:#1a1a1a;padding:2px 6px;border-radius:4px;font-size:.7rem}}
  .badge{{display:inline-block;padding:2px 8px;border-radius:20px;font-size:.65rem;font-weight:600}}
  .badge.ok{{background:#14532d;color:#4ade80}}.badge.info{{background:#1e3a5f;color:#60a5fa}}
  h2{{font-size:.8rem;color:#555;text-transform:uppercase;letter-spacing:2px;margin:24px 0 12px}}
  .refresh{{float:right;font-size:.7rem;color:#333;margin-top:-20px}}
</style>
</head>
<body>
<h1>⚙ NewPipe Service</h1>
<div class="sub">yt-dlp {yt_dlp.version.__version__} · v2.0.0 · P-Stream microservice</div>
<div class="grid">
  <div class="card"><div class="label">Total Requests</div><div class="val">{_stats['requests']}</div></div>
  <div class="card"><div class="label">Cache Hits</div><div class="val green">{_stats['hits']}</div></div>
  <div class="card"><div class="label">Cache Misses</div><div class="val">{_stats['misses']}</div></div>
  <div class="card"><div class="label">Hit Rate</div><div class="val {'green' if hit_rate > 60 else 'red'}">{hit_rate}%</div></div>
  <div class="card"><div class="label">Errors</div><div class="val {'red' if _stats['errors'] > 0 else ''}">{_stats['errors']}</div></div>
  <div class="card"><div class="label">Cache Entries</div><div class="val">{len(_cache)}</div></div>
</div>
<h2>Endpoints <a href="javascript:location.reload()" class="refresh">↻ Refresh</a></h2>
<table>
<thead><tr><th>Method</th><th>Path</th><th>Purpose</th><th>Cached</th></tr></thead>
<tbody>
  <tr><td><span class="badge ok">GET</span></td><td><code>/health</code></td><td>Liveness + stats</td><td>—</td></tr>
  <tr><td><span class="badge ok">GET</span></td><td><code>/trailer</code></td><td>Search + extract (primary)</td><td>30 min</td></tr>
  <tr><td><span class="badge ok">GET</span></td><td><code>/search</code></td><td>Search YouTube</td><td>1 hr</td></tr>
  <tr><td><span class="badge ok">GET</span></td><td><code>/extract</code></td><td>Extract stream URL</td><td>30 min</td></tr>
  <tr><td><span class="badge ok">GET</span></td><td><code>/info</code></td><td>Metadata only (fast)</td><td>1 hr</td></tr>
  <tr><td><span class="badge info">POST</span></td><td><code>/batch</code></td><td>Multi-extract (max 5)</td><td>30 min</td></tr>
  <tr><td><span class="badge ok">GET</span></td><td><code>/cache/stats</code></td><td>Cache metrics</td><td>—</td></tr>
  <tr><td><span class="badge info">DELETE</span></td><td><code>/cache</code></td><td>Clear all cache</td><td>—</td></tr>
</tbody>
</table>
<h2>Try it</h2>
<table>
<thead><tr><th>Example</th><th>URL</th></tr></thead>
<tbody>
  <tr><td>Oppenheimer trailer</td><td><code><a style="color:#e50914" href="/trailer?title=Oppenheimer&year=2023&type=movie">/trailer?title=Oppenheimer&year=2023</a></code></td></tr>
  <tr><td>Search Dune 2024</td><td><code><a style="color:#e50914" href="/search?q=Dune+2024&type=movie">/search?q=Dune+2024</a></code></td></tr>
  <tr><td>Cache stats</td><td><code><a style="color:#e50914" href="/cache/stats">/cache/stats</a></code></td></tr>
</tbody>
</table>
</body></html>""")

# ─── Startup ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 7861))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, workers=1)
