# Use Python 3.12 as the base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY pyproject.toml .
COPY README.md .
COPY runtime_config.json .
COPY src/ src/

# Install project dependencies
RUN pip install --no-cache-dir .

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

# Expose the stdio interface
EXPOSE 5000

# Default entrypoint - can be overridden
ENTRYPOINT ["mcp_snowflake_server"] 
