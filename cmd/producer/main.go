package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/gor0utine/kafka-to-es/internal/config"
)

type Event struct {
	ID        int       `json:"id"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	cfg, err := config.Load("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Create writers for each topic
	writers := make(map[string]*kafka.Writer)
	for _, t := range cfg.Kafka.Topics {
		writers[t] = &kafka.Writer{
			Addr:         kafka.TCP(cfg.Kafka.Brokers...),
			Topic:        t,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
		}
		defer writers[t].Close()
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Handle SIGINT/SIGTERM for graceful shutdown
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
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
			// Pick a random topic
			topic := cfg.Kafka.Topics[rand.Intn(len(cfg.Kafka.Topics))]

			// Create event
			event := Event{
				ID:        id,
				Message:   fmt.Sprintf("Hello from %s!", topic),
				Timestamp: time.Now(),
			}
			id++

			// Serialize event to JSON
			value, err := json.Marshal(event)
			if err != nil {
				log.Printf("Failed to marshal event: %v", err)
				continue
			}

			// Send to Kafka
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
