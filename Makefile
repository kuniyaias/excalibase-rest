# Makefile for Excalibase REST API

.PHONY: help build test run clean package docker-build docker-run

# Default goal
.DEFAULT_GOAL := help

# Variables
APP_NAME := excalibase-rest
VERSION := 1.0.0
PORT := 8080

help: ## Show this help message
	@echo "Available commands:"
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

build: ## Build the application
	@echo "Building $(APP_NAME)..."
	mvn clean compile

test: ## Run all tests
	@echo "Running tests..."
	mvn test

test-integration: ## Run integration tests only
	@echo "Running integration tests..."
	mvn test -Dtest="*IntegrationTest"

package: ## Package the application
	@echo "Packaging $(APP_NAME)..."
	mvn clean package -DskipTests

install: ## Build and install the application
	@echo "Installing $(APP_NAME)..."
	mvn clean install

run: ## Run the application locally
	@echo "Starting $(APP_NAME) on port $(PORT)..."
	mvn spring-boot:run

dev: ## Run in development mode with hot reload
	@echo "Starting $(APP_NAME) in development mode..."
	mvn spring-boot:run -Dspring.profiles.active=dev

clean: ## Clean build artifacts
	@echo "Cleaning build artifacts..."
	mvn clean

coverage: ## Generate test coverage report
	@echo "Generating test coverage report..."
	mvn clean test jacoco:report
	@echo "Coverage report available at: target/site/jacoco/index.html"

javadoc: ## Generate Javadoc
	@echo "Generating Javadoc..."
	mvn javadoc:javadoc
	@echo "Javadoc available at: target/site/apidocs/index.html"

format: ## Format code using Maven formatter
	@echo "Formatting code..."
	mvn spotless:apply 2>/dev/null || echo "Spotless not configured, skipping..."

lint: ## Run code quality checks
	@echo "Running code quality checks..."
	mvn verify -DskipTests

# Docker targets
docker-build: ## Build Docker image
	@echo "Building Docker image..."
	docker build -t $(APP_NAME):$(VERSION) .
	docker tag $(APP_NAME):$(VERSION) $(APP_NAME):latest

docker-run: ## Run application in Docker
	@echo "Running $(APP_NAME) in Docker..."
	docker run -p $(PORT):$(PORT) --name $(APP_NAME) -d $(APP_NAME):latest

docker-stop: ## Stop Docker container
	@echo "Stopping $(APP_NAME) container..."
	docker stop $(APP_NAME) || true
	docker rm $(APP_NAME) || true

# Database targets
db-start: ## Start PostgreSQL with Docker
	@echo "Starting PostgreSQL database..."
	docker run --name excalibase-postgres -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:15
	@echo "Database will be available in a few seconds at localhost:5432"
	@echo "Default credentials: postgres/postgres"

db-stop: ## Stop PostgreSQL container
	@echo "Stopping PostgreSQL database..."
	docker stop excalibase-postgres || true
	docker rm excalibase-postgres || true

db-shell: ## Connect to database shell
	@echo "Connecting to database..."
	docker exec -it excalibase-postgres psql -U postgres

# API testing targets
api-test: ## Test API endpoints (requires running application)
	@echo "Testing API endpoints..."
	@echo "Testing schema endpoint..."
	curl -s http://localhost:$(PORT)/api/v1 | jq '.' || curl -s http://localhost:$(PORT)/api/v1
	@echo "\nTesting OpenAPI spec..."
	curl -s http://localhost:$(PORT)/api/v1/openapi.json | jq '.info' || echo "Application may not be running"

health-check: ## Check application health
	@echo "Checking application health..."
	curl -s http://localhost:$(PORT)/actuator/health | jq '.' || echo "Application may not be running on port $(PORT)"

# Development workflow
dev-setup: db-start ## Setup development environment
	@echo "Setting up development environment..."
	@echo "Waiting for database to be ready..."
	sleep 10
	@echo "Development environment ready!"
	@echo "Start the application with: make run"

dev-teardown: db-stop docker-stop ## Teardown development environment
	@echo "Development environment cleaned up"

# Quick start
quick-start: dev-setup run ## Quick start with database and application

# Show application info
info: ## Show application information
	@echo "Application: $(APP_NAME)"
	@echo "Version: $(VERSION)"
	@echo "Port: $(PORT)"
	@echo "API Base URL: http://localhost:$(PORT)/api/v1"
	@echo "OpenAPI Spec: http://localhost:$(PORT)/api/v1/openapi.json"
	@echo "Health Check: http://localhost:$(PORT)/actuator/health"
	@echo "Documentation: http://localhost:$(PORT)/api/v1/docs"