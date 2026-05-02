FROM python:3.11-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install Python deps — yt-dlp pinned to latest at build time
RUN pip install --no-cache-dir -r requirements.txt

# Force fresh yt-dlp nightly from pip (most up to date, bypasses release lag)
RUN pip install --upgrade --force-reinstall "yt-dlp>=2025.1.1"

COPY app.py .

EXPOSE 7861

# Update yt-dlp at runtime too (catches weekly YouTube extractor changes)
# Falls back silently if network is unavailable
CMD ["sh", "-c", "pip install -q --upgrade yt-dlp 2>/dev/null || true && uvicorn app:app --host 0.0.0.0 --port 7861 --workers 1"]
