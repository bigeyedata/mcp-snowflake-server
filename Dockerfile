# Use Python 3.12 as the base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file if it exists, otherwise install dependencies directly
COPY requirements.txt* ./
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; \
    else pip install --no-cache-dir mcp snowflake-connector-python[pandas] snowflake-snowpark-python cryptography python-dotenv sqlparse; fi

# Copy application files
COPY server.py .
COPY config.py .
COPY src/ src/
COPY config.json* ./

# Create a non-root user
RUN useradd -m -u 1000 mcp && chown -R mcp:mcp /app

# Create directories for logs and configuration
RUN mkdir -p /home/mcp/.snowflake-mcp /app/logs && \
    chown -R mcp:mcp /home/mcp/.snowflake-mcp /app/logs

# Switch to non-root user
USER mcp

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command - run the server directly
CMD ["python", "server.py"] 
