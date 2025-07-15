# Use Python 3.11 slim image (distroless would be more secure but requires more setup)
FROM python:3.11-slim-bookworm

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files
COPY requirements.txt pyproject.toml uv.lock ./

# Install uv for faster package management
RUN pip install uv

# Install dependencies using uv
RUN uv pip install --system -r requirements.txt

# Copy source code
COPY src/ ./src/
COPY setup.py ./

# Install the package
RUN pip install -e .

# Create a non-root user
RUN useradd --create-home --shell /bin/bash mcp
USER mcp

# Expose port (if your MCP server needs to listen on a port)
# EXPOSE 8000

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "-m", "simple_snowflake_mcp"]
