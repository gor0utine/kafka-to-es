# Service names
COMPOSE_FILE=docker-compose.yml
CONSUMER_IMAGE=kafka-es-consumer
PRODUCER_IMAGE=kafka-producer

# Default target: run everything in Docker
.PHONY: run-docker
run-docker: build-docker up

# Build Docker images for consumer and producer
.PHONY: build-docker
build-docker:
	@echo ">>> Building consumer image..."
	docker compose -f $(COMPOSE_FILE) build consumer
	@echo ">>> Building producer image..."
	docker compose -f $(COMPOSE_FILE) build producer

# Start full stack (Redpanda, ES, Kibana, Kafka UI, Consumer, Producer)
.PHONY: up
up:
	@echo ">>> Starting full Docker stack..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ">>> Waiting for services to initialize..."
	sleep 10
	@echo ">>> Creating Kafka topics..."
	docker exec -it redpanda rpk topic create topic-a topic-b || true

# Stop and remove everything
.PHONY: down
down:
	@echo ">>> Stopping and removing containers..."
	docker compose -f $(COMPOSE_FILE) down -v

# View logs of all services
.PHONY: logs
logs:
	docker compose -f $(COMPOSE_FILE) logs -f

# View logs for consumer
.PHONY: logs-consumer
logs-consumer:
	docker compose -f $(COMPOSE_FILE) logs -f consumer

# View logs for producer
.PHONY: logs-producer
logs-producer:
	docker compose -f $(COMPOSE_FILE) logs -f producer
