# Start from a lightweight, secure base image
FROM python:3.10-slim

# Set environment variables for safety & performance
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Set working directory
WORKDIR /app

# Install system dependencies (optional: remove if not needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better Docker caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY consumer.py .

# Non-root user for security
RUN useradd -m appuser
USER appuser

# Default command
CMD ["python", "consumer.py"]
