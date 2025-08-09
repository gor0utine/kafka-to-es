# Variables
BINARY_NAME=kafka-es-bulker
CONFIG_FILE=config.yaml
DOCKER_COMPOSE=docker-compose.yml

# Default target
.PHONY: all
all: up build run

# Start Kafka + Elasticsearch + Kibana
.PHONY: up
up:
	@echo ">>> Starting local Kafka + Elasticsearch + Kibana..."
	docker compose -f $(DOCKER_COMPOSE) up -d
	@echo ">>> Waiting for services to be ready..."
	@sleep 10
	@echo ">>> Creating Kafka topics..."
	docker exec -it redpanda rpk topic create topic-a topic-b || true

# Stop and remove containers
.PHONY: down
down:
	@echo ">>> Stopping containers..."
	docker compose -f $(DOCKER_COMPOSE) down -v

# Build Go service
.PHONY: build
build:
	@echo ">>> Building $(BINARY_NAME)..."
	go mod tidy
	go build -o $(BINARY_NAME) ./cmd/service

# Run the service
.PHONY: run
run:
	@echo ">>> Running $(BINARY_NAME) with config $(CONFIG_FILE)..."
	./$(BINARY_NAME)

# Clean binary
.PHONY: clean
clean:
	@echo ">>> Removing binary..."
	rm -f $(BINARY_NAME)

# Restart entire stack
.PHONY: restart
restart: down up build run
