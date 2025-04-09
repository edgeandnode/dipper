FROM python:3.12-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY app/ ./app/

# Expose port for API
EXPOSE 8080

# Set environment variables - Ensures that python logs are not buffered
ENV PYTHONUNBUFFERED=1

# Run the service
CMD ["python", "-m", "app.main"] 