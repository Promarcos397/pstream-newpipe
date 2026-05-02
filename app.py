"""
NewPipe Service — P-Stream Trailer & Video Extractor
=====================================================
A lightweight yt-dlp-powered FastAPI microservice that replaces the YouTube
Data API v3 for trailer search and stream URL extraction.

Why this exists:
  - No API keys or quotas
  - Works with 1000+ platforms (YouTube, Vimeo, Dailymotion, etc.)
  - Returns direct CDN stream URLs — no embeds, no iframes
  - We own the extraction stack entirely

Endpoints:
  GET /search?q=...&type=movie|tv&year=...   — search for trailers
  GET /extract?url=...                        — extract stream URL from video page
  GET /info?url=...                           — get video metadata (no stream)
  GET /health                                 — service health check

Deploy on Hugging Face Spaces (Python SDK) as a separate Space.
Connect to frontend via VITE_NEWPIPE_URL env var.
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import yt_dlp
import asyncio
import functools
import re
import os
import time
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("newpipe")

app = FastAPI(
    title="P-Stream NewPipe Service",
    description="yt-dlp powered video/trailer extractor for P-Stream",
    version="1.0.0",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = [
    "https://pstream.watch",
    "https://www.pstream.watch",
    "https://pstream-frontend.pages.dev",
    "https://ibrahimar397-pstream-giga.hf.space",
    "http://localhost:5173",
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─── Simple in-memory cache ────────────────────────────────────────────────────
_cache: dict[str, tuple[float, any]] = {}
CACHE_TTL = 3600  # 1 hour

def cache_get(key: str):
    if key in _cache:
        ts, val = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return val
    return None

def cache_set(key: str, val):
    _cache[key] = (time.time(), val)
    # Evict oldest entries if cache grows too large
    if len(_cache) > 500:
        oldest = sorted(_cache.items(), key=lambda x: x[1][0])[:100]
        for k, _ in oldest:
            del _cache[k]

# ─── yt-dlp helpers ───────────────────────────────────────────────────────────

def _ydl_opts_base(quiet: bool = True) -> dict:
    """Base yt-dlp options shared across all operations."""
    return {
        "quiet": quiet,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "socket_timeout": 15,
        "http_headers": {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
    }


def _run_in_thread(fn, *args):
    """Run a blocking yt-dlp call in a thread pool to not block the event loop."""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, functools.partial(fn, *args))


def _search_youtube(query: str, max_results: int = 8) -> list[dict]:
    """Search YouTube for the given query using yt-dlp's ytsearch extractor."""
    opts = _ydl_opts_base()
    opts["extract_flat"] = True
    opts["playlistend"] = max_results

    search_url = f"ytsearch{max_results}:{query}"

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(search_url, download=False)
        entries = info.get("entries") or []
        results = []
        for e in entries:
            if not e:
                continue
            results.append({
                "id":       e.get("id"),
                "title":    e.get("title"),
                "url":      e.get("url") or f"https://www.youtube.com/watch?v={e.get('id')}",
                "duration": e.get("duration"),
                "views":    e.get("view_count"),
                "channel":  e.get("channel") or e.get("uploader"),
                "thumb":    e.get("thumbnail") or (f"https://i.ytimg.com/vi/{e.get('id')}/hqdefault.jpg" if e.get("id") else None),
            })
        return results


def _extract_stream(url: str) -> dict:
    """Extract the best direct stream URL + subtitle tracks from a video page."""
    opts = _ydl_opts_base()
    opts["format"] = "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best"
    opts["merge_output_format"] = "mp4"
    opts["writesubtitles"]      = True
    opts["writeautomaticsub"]   = True
    opts["subtitleslangs"]      = ["en", "en-US", "en-GB"]

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        if not info:
            return {}

        # Find best progressive mp4 (video+audio in one file — browser compatible)
        formats    = info.get("formats") or []
        progressive = [
            f for f in formats
            if f.get("vcodec") != "none"
            and f.get("acodec") != "none"
            and f.get("ext") == "mp4"
        ]
        progressive.sort(key=lambda f: f.get("height") or 0, reverse=True)
        best = progressive[0] if progressive else None

        stream_url = (best or {}).get("url") or info.get("url")
        height     = (best or {}).get("height") or info.get("height")

        # Collect subtitle tracks — prefer manual, fallback to automatic captions
        raw_subs  = info.get("subtitles") or {}
        auto_subs = info.get("automatic_captions") or {}
        subtitles: dict = {}

        def _pick_vtt(tracks: list) -> str | None:
            """Pick the WebVTT url from a list of format dicts."""
            for t in tracks:
                if t.get("ext") == "vtt":
                    return t.get("url")
            # fallback: first available
            return (tracks[0].get("url") if tracks else None)

        for lang in ["en", "en-US", "en-GB"]:
            # Manual captions take priority over auto-generated
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


def _get_info(url: str) -> dict:
    """Get metadata only — no stream URL extraction."""
    opts = _ydl_opts_base()
    opts["extract_flat"] = True

    with yt_dlp.YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=False)
        return {
            "id":       info.get("id"),
            "title":    info.get("title"),
            "duration": info.get("duration"),
            "views":    info.get("view_count"),
            "channel":  info.get("channel") or info.get("uploader"),
            "thumb":    info.get("thumbnail"),
            "platform": info.get("extractor_key") or "unknown",
            "tags":     (info.get("tags") or [])[:10],
        }

# ─── Trailer scoring ───────────────────────────────────────────────────────────

def _score_trailer(result: dict, title: str, year: str | None, media_type: str) -> float:
    """
    Score a search result for trailer relevance.
    Higher = better. Used to pick the best candidate from ytsearch results.
    """
    score = 0.0
    t = (result.get("title") or "").lower()
    title_l = title.lower()

    # Must contain the title
    if title_l in t:
        score += 10

    # Trailer keywords
    for kw in ["official trailer", "trailer", "teaser", "official teaser"]:
        if kw in t:
            score += 5 if "official" in kw else 3

    # Year match
    if year and year in t:
        score += 3

    # Type match
    if media_type == "tv" and any(w in t for w in ["season", "series", "episode"]):
        score += 1

    # Prefer shorter videos (trailers are 1-3 min, not 2h movies)
    dur = result.get("duration") or 0
    if 60 <= dur <= 300:
        score += 2
    elif dur > 600:
        score -= 3

    # High view count = likely official
    views = result.get("views") or 0
    if views > 5_000_000:
        score += 3
    elif views > 1_000_000:
        score += 1

    # Official channel keywords
    channel = (result.get("channel") or "").lower()
    for kw in ["official", "pictures", "entertainment", "studios", "films"]:
        if kw in channel:
            score += 2
            break

    return score


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"ok": True, "service": "newpipe", "yt_dlp_version": yt_dlp.version.__version__}


@app.get("/search")
async def search(
    q:     str           = Query(..., description="Search query, e.g. 'Inception 2010 trailer'"),
    type:  str           = Query("movie", description="'movie' or 'tv'"),
    year:  str | None    = Query(None, description="Release year for scoring"),
    limit: int           = Query(5, ge=1, le=10),
):
    """
    Search YouTube for trailers matching the query.
    Returns results scored by relevance — index [0] is the best candidate.
    """
    cache_key = f"search:{q}:{type}:{year}"
    cached = cache_get(cache_key)
    if cached:
        log.info(f"[Search] Cache HIT: {q}")
        return cached

    log.info(f"[Search] Querying yt-dlp: {q!r}")
    try:
        # Auto-append "official trailer" if not already a URL or specific query
        search_query = q if "trailer" in q.lower() else f"{q} official trailer"
        raw = await _run_in_thread(_search_youtube, search_query, 12)
    except Exception as e:
        raise HTTPException(502, f"yt-dlp search failed: {e}")

    # Extract title from query for scoring (q format: "Title Year")
    title_guess = re.sub(r"\d{4}.*$", "", q).strip()

    scored = sorted(raw, key=lambda r: _score_trailer(r, title_guess, year, type), reverse=True)
    result = {
        "results": scored[:limit],
        "query":   q,
        "total":   len(scored),
    }
    cache_set(cache_key, result)
    return result


@app.get("/extract")
async def extract(
    url: str = Query(..., description="YouTube or any yt-dlp-supported video URL"),
):
    """
    Extract a direct stream URL from a video page.
    Returns a direct CDN URL — no embed, no iframe.
    """
    cache_key = f"extract:{url}"
    cached = cache_get(cache_key)
    if cached:
        log.info(f"[Extract] Cache HIT: {url}")
        return cached

    log.info(f"[Extract] {url}")
    try:
        info = await _run_in_thread(_extract_stream, url)
    except yt_dlp.utils.DownloadError as e:
        raise HTTPException(502, f"Extraction failed: {e}")
    except Exception as e:
        raise HTTPException(500, f"Internal error: {e}")

    if not info.get("stream_url"):
        raise HTTPException(404, "No stream URL found")

    # Cache for 30 min only (stream URLs expire)
    _cache[cache_key] = (time.time() - (CACHE_TTL - 1800), info)
    return info


@app.get("/info")
async def info(
    url: str = Query(..., description="Video URL"),
):
    """Get video metadata without extracting stream URLs (fast)."""
    cache_key = f"info:{url}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    try:
        meta = await _run_in_thread(_get_info, url)
    except Exception as e:
        raise HTTPException(502, f"Info fetch failed: {e}")

    cache_set(cache_key, meta)
    return meta


# ─── Startup ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 7861))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
