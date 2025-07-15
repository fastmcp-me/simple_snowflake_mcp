# Setup script for simple-snowflake-mcp Docker environment (PowerShell)

Write-Host "🐋 Setting up simple-snowflake-mcp Docker environment..." -ForegroundColor Cyan

# Check if Docker is installed
try {
    docker --version | Out-Null
    Write-Host "✅ Docker is installed" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker is not installed. Please install Docker Desktop first." -ForegroundColor Red
    exit 1
}

# Check if Docker Compose is installed
try {
    docker-compose --version | Out-Null
    Write-Host "✅ Docker Compose is installed" -ForegroundColor Green
} catch {
    Write-Host "❌ Docker Compose is not installed. Please install Docker Compose first." -ForegroundColor Red
    exit 1
}

# Create .env file if it doesn't exist
if (!(Test-Path .env)) {
    Write-Host "📝 Creating .env file from template..." -ForegroundColor Yellow
    Copy-Item .env.example .env
    Write-Host "✅ .env file created. Please edit it with your Snowflake credentials." -ForegroundColor Green
    Write-Host "   Required variables: SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT" -ForegroundColor Yellow
} else {
    Write-Host "✅ .env file already exists" -ForegroundColor Green
}

# Create logs directory
if (!(Test-Path logs)) {
    New-Item -ItemType Directory -Path logs | Out-Null
    Write-Host "✅ Logs directory created" -ForegroundColor Green
} else {
    Write-Host "✅ Logs directory already exists" -ForegroundColor Green
}

# Build the Docker image
Write-Host "🔨 Building Docker image..." -ForegroundColor Yellow
docker-compose build

Write-Host ""
Write-Host "🎉 Setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Cyan
Write-Host "1. Edit .env file with your Snowflake credentials"
Write-Host "2. Run 'docker-compose up -d' to start the service"
Write-Host "3. Run 'docker-compose logs -f' to view logs"
Write-Host ""
Write-Host "Available commands:" -ForegroundColor Cyan
Write-Host "  docker-compose up -d                    - Start in development mode"
Write-Host "  docker-compose --profile dev up -d      - Start with code volume mounts"
Write-Host "  docker-compose logs -f                  - View logs"
Write-Host "  docker-compose down                     - Stop the service"
Write-Host ""
Write-Host "Or use the Makefile commands if you have make installed:" -ForegroundColor Yellow
Write-Host "  make help       - Show all available commands"
Write-Host "  make up         - Start in development mode"
Write-Host "  make prod-up    - Start in production mode"
Write-Host "  make dev-up     - Start with code volume mounts"
Write-Host ""
