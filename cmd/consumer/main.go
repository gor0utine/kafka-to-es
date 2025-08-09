package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/elastic/go-elasticsearch/v8"

	"github.com/gor0utine/kafka-to-es/internal/config"
	"github.com/gor0utine/kafka-to-es/internal/indexer"
	"github.com/gor0utine/kafka-to-es/internal/kafka"
	"github.com/gor0utine/kafka-to-es/internal/mapper"
	"github.com/gor0utine/kafka-to-es/internal/worker"
)

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	esCfg := elasticsearch.Config{
		Addresses: cfg.ES.Addresses,
	}
	if cfg.ES.Username != "" {
		esCfg.Username = cfg.ES.Username
		esCfg.Password = cfg.ES.Password
	}
	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		log.Fatalf("es client: %v", err)
	}

	// Prepare consumer config
	consumerCfg := kafka.ConsumerConfig{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.GroupID,
		Topics:  cfg.Kafka.Topics,
	}
	inCh := make(chan *kafka.Message, 10000)

	consumer := kafka.NewConsumerManager(consumerCfg)
	bulker := indexer.NewBulker(
		es,
		cfg.Worker.NumWorkers,
		cfg.Worker.BatchBytes,
		cfg.Worker.FlushInterval,
	)
	mapper := mapper.New(cfg.Mappings)
	wp := worker.NewWorkerPool(bulker, mapper, inCh, cfg.Worker.NumWorkers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer.Start(ctx, inCh)
	wp.Start(ctx)

	// graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	<-sigs
	log.Println("received shutdown signal, draining...")

	cancel()
	shutdownCtx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	if err := bulker.Close(shutdownCtx); err != nil {
		log.Printf("error closing bulker: %v", err)
	}
	consumer.Close()
	close(inCh)
	log.Println("shutdown complete")
}
