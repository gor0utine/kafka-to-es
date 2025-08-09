package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/gor0utine/kafka-to-es/internal/config"
)

// Event represents a message to be sent to Kafka.
type Event struct {
	ID        int       `json:"id"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

// createWriters initializes a writer for each topic.
func createWriters(brokers, topics []string) map[string]*kafka.Writer {
	writers := make(map[string]*kafka.Writer, len(topics))
	for _, t := range topics {
		writers[t] = &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        t,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		}
	}
	return writers
}

// closeWriters closes all Kafka writers.
func closeWriters(writers map[string]*kafka.Writer) {
	var wg sync.WaitGroup
	for _, w := range writers {
		wg.Add(1)
		go func(writer *kafka.Writer) {
			defer wg.Done()
			_ = writer.Close()
		}(w)
	}
	wg.Wait()
}

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	writers := createWriters(cfg.Kafka.Brokers, cfg.Kafka.Topics)
	defer closeWriters(writers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT/SIGTERM for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received shutdown signal...")
		cancel()
	}()

	log.Println("Kafka producer started. Sending messages...")

	rand.Seed(time.Now().UnixNano())
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	id := 1
	for {
		select {
		case <-ctx.Done():
			log.Println("Producer shutting down.")
			return
		case <-ticker.C:
			topic := cfg.Kafka.Topics[rand.Intn(len(cfg.Kafka.Topics))]
			event := Event{
				ID:        id,
				Message:   fmt.Sprintf("Hello from %s!", topic),
				Timestamp: time.Now(),
			}
			id++

			value, err := json.Marshal(event)
			if err != nil {
				log.Printf("Failed to marshal event: %v", err)
				continue
			}

			err = writers[topic].WriteMessages(ctx, kafka.Message{
				Key:   []byte(fmt.Sprintf("%d", event.ID)),
				Value: value,
			})
			if err != nil {
				log.Printf("Failed to write message: %v", err)
			} else {
				log.Printf("Produced to %s: %+v", topic, event)
			}
		}
	}
}
