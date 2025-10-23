# Use Python 3.10 to support modern type annotations
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies needed for Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set environment variables for Python optimization
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Environment variables for Azure authentication
# These will be provided at runtime via Kubernetes secrets or local environment
ENV AZURE_TENANT_ID="" \
    AZURE_CLIENT_ID="" \
    AZURE_CLIENT_SECRET=""

# Expose port if needed (optional, adjust based on your app)
# EXPOSE 8000

# Run the application
CMD ["python", "main.py"]
