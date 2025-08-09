BINARY_CONSUMER=consumer
BINARY_PRODUCER=producer
CONFIG_FILE=config.yaml
DOCKER_COMPOSE=docker-compose.yml

# Build & run everything
.PHONY: run-docker
run-docker:
	@echo ">>> Building and starting full Docker stack..."
	docker compose -f $(DOCKER_COMPOSE) up --build

# Start stack without rebuilding
.PHONY: up
up:
	@echo ">>> Starting existing stack..."
	docker compose -f $(DOCKER_COMPOSE) up

# Stop stack
.PHONY: down
down:
	@echo ">>> Stopping and removing containers..."
	docker compose -f $(DOCKER_COMPOSE) down -v

# Build only producer & consumer images
.PHONY: build-docker
build-docker:
	@echo ">>> Building producer and consumer images..."
	docker compose -f $(DOCKER_COMPOSE) build producer consumer

# View logs from consumer
.PHONY: logs-consumer
logs-consumer:
	docker compose -f $(DOCKER_COMPOSE) logs -f consumer

# View logs from producer
.PHONY: logs-producer
logs-producer:
	docker compose -f $(DOCKER_COMPOSE) logs -f producer

# Recreate Kafka topics inside Redpanda
.PHONY: create-topics
create-topics:
	@echo ">>> Creating Kafka topics topic-a and topic-b..."
	docker exec -it redpanda rpk topic create topic-a topic-b || true

# Clean everything (containers + volumes + images)
.PHONY: clean
clean:
	@echo ">>> Cleaning all containers, volumes, and images..."
	docker compose -f $(DOCKER_COMPOSE) down -v --rmi local
