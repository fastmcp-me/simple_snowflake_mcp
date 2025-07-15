#!/bin/bash

# Setup script for simple-snowflake-mcp Docker environment

set -e

echo "🐋 Setting up simple-snowflake-mcp Docker environment..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp .env.example .env
    echo "✅ .env file created. Please edit it with your Snowflake credentials."
    echo "   Required variables: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT"
else
    echo "✅ .env file already exists"
fi

# Create logs directory
mkdir -p logs
echo "✅ Logs directory created"

# Build the Docker image
echo "🔨 Building Docker image..."
docker-compose build

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env file with your Snowflake credentials"
echo "2. Run 'docker-compose up -d' or 'make up' to start the service"
echo "3. Run 'docker-compose logs -f' or 'make logs' to view logs"
echo ""
echo "Available commands:"
echo "  make help       - Show all available commands"
echo "  make up         - Start in development mode"
echo "  make prod-up    - Start in production mode"
echo "  make dev-up     - Start with code volume mounts"
echo "  make logs       - View logs"
echo "  make down       - Stop the service"
echo ""
