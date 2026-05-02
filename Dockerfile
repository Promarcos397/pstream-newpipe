FROM python:3.11-slim

WORKDIR /app

# Install system deps for yt-dlp (ffmpeg for merging streams)
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Keep yt-dlp up to date on start (extractor signatures change weekly)
RUN yt-dlp --update-to nightly || true

COPY app.py .

EXPOSE 7861

# Run with 2 workers — enough for HF free tier (2 vCPU, 16GB RAM)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7861", "--workers", "2"]
