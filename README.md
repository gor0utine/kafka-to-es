# kafka-to-es

A Go service that consumes messages from Kafka, processes them, and indexes them into Elasticsearch.

## Features

- Consumes messages from specified Kafka topics
- Maps and transforms messages using configurable mappings
- Batches and flushes data to Elasticsearch efficiently
- Graceful shutdown and configurable worker pool
