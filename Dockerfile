# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Install system dependencies required for Python packages
# gcc and other build tools are needed for some packages like cryptography
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the ingester script
COPY ingester.py .

# Set environment variables (these should be overridden at runtime)
ENV CLICKHOUSE_HOST=localhost
ENV CLICKHOUSE_PORT=8123
ENV CLICKHOUSE_USER=default
ENV CLICKHOUSE_PASS=

ENV MQTT_HOST=localhost
ENV MQTT_PORT=1883
ENV MQTT_USER=meshcore
ENV MQTT_PASS=meshcore
ENV MQTT_TOPIC=meshcore

ENV QUEUE_DB_PATH=/app/data/queue.db

# Create directory for persistent queue database
RUN mkdir -p /app/data

# Run the ingester when container launches
CMD ["python", "-u", "ingester.py"]
