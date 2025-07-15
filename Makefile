# Makefile for simple-snowflake-mcp Docker operations

.PHONY: help build up down logs shell clean dev-up dev-down prod-up prod-down

# Default target
help:
	@echo "Available commands:"
	@echo "  build      - Build the Docker image"
	@echo "  up         - Start the service in development mode"
	@echo "  down       - Stop the service"
	@echo "  logs       - View service logs"
	@echo "  shell      - Open a shell in the running container"
	@echo "  clean      - Remove containers and images"
	@echo "  dev-up     - Start the service in development mode with volume mounts"
	@echo "  dev-down   - Stop the development service"
	@echo "  prod-up    - Start the service in production mode"
	@echo "  prod-down  - Stop the production service"
	@echo "  test       - Run tests in Docker"

# Build Docker image
build:
	docker-compose build

# Start development service (default)
up:
	docker-compose up -d

# Production mode with enhanced security
prod-up:
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Stop production service
prod-down:
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml down

# Stop service
down:
	docker-compose down

# View logs
logs:
	docker-compose logs -f

# Open shell in running container
shell:
	docker-compose exec simple-snowflake-mcp /bin/bash

# Clean up Docker resources
clean:
	docker-compose down --rmi all --volumes --remove-orphans

# Development mode with volume mounts
dev-up:
	docker-compose --profile dev up simple-snowflake-mcp-dev -d

# Stop development service
dev-down:
	docker-compose --profile dev down

# Run tests in Docker
test:
	docker-compose run --rm simple-snowflake-mcp python -m pytest tests/

# Quick restart
restart: down up
