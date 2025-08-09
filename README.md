# kafka-to-es

A Go service that consumes messages from Kafka, processes them, and indexes them into Elasticsearch.

## Features

- Consumes messages from specified Kafka topics
- Maps and transforms messages using configurable mappings
- Batches and flushes data to Elasticsearch efficiently
- Graceful shutdown and configurable worker pool

## Requirements

- Go 1.18+
- Kafka cluster
- Elasticsearch 8.x

## Configuration

Configuration is provided via a YAML file (e.g., `config.yaml`). Example:

```yaml
kafka:
  brokers:
    - "localhost:9092"
  group_id: "my-group"
  topics:
    - "my-topic"

es:
  addresses:
    - "http://localhost:9200"
  username: ""
  password: ""

worker:
  num_workers: 4
  batch_size: 500
  batch_bytes: 1048576
  flush_interval_seconds: "2s"

mappings:
  # Define your field mappings here
