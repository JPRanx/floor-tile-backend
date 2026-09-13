FROM python:3.11-slim

# Install system dependencies for OCR
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    tesseract-ocr-spa \
    tesseract-ocr-eng \
    poppler-utils \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first (for caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# The platform supplies PORT. Runtime configuration and secrets are injected
# by the host; none are baked into this image.
EXPOSE 8000
CMD ["sh", "-c", "exec uvicorn clean_v1.sailing_api:app --host 0.0.0.0 --port $PORT --no-proxy-headers"]
