# kafka-to-es

A Go service that consumes messages from Kafka topics and indexes them into Elasticsearch.

## Features

- Consumes messages from configurable Kafka topics
- Processes and transforms messages before indexing
- Efficient batching and error handling
- Configurable via environment variables

## Topic Mapping

You can configure how Kafka topics are mapped to Elasticsearch indices using the `mappings` section in your `config.yaml` file.

**Example:**
```yaml
mappings:
  topic-a: "index-a"
  topic-b: "index-b"
```

- Each Kafka topic (e.g., `topic-a`) is mapped to a target Elasticsearch index (e.g., `index-a`).
- To add or change mappings, edit the `mappings` section in your configuration file.

## Installation

Clone the repository and build the binary:

```sh
git clone github.com/gor0utine/kafka-to-es
```

## Development

The project provides a Makefile for common development tasks:

- **Build & run everything with Docker Compose:**
  ```sh
  make run-docker
  ```
- **Start the stack without rebuilding:**
  ```sh
  make up
  ```
- **Stop and remove containers:**
  ```sh
  make down
  ```
- **Build only producer & consumer images:**
  ```sh
  make build-docker
  ```
- **View logs from consumer:**
  ```sh
  make logs-consumer
  ```
- **View logs from producer:**
  ```sh
  make logs-producer
  ```
- **Recreate Kafka topics inside Redpanda:**
  ```sh
  make create-topics
  ```
- **Clean all containers, volumes, and images:**
  ```sh
  make clean
  ```

## License

MIT License. See [LICENSE](LICENSE) for details.
