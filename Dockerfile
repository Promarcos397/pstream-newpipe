FROM python:3.11-slim

WORKDIR /app

# Install ffmpeg (needed by yt-dlp for merging adaptive streams)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 7861

# yt-dlp extractor signatures change weekly. We update at RUNTIME (not build)
# so the Space doesn't need a redeploy every week to keep working.
# The update takes ~3s and runs before the server starts.
CMD ["sh", "-c", "yt-dlp -U --quiet || true && uvicorn app:app --host 0.0.0.0 --port 7861 --workers 1"]
