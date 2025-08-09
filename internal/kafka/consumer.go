package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// Message wraps kafka.Message with topic info
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []kafka.Header
	Time    time.Time
}

// ConsumerConfig holds configuration for the consumer manager
type ConsumerConfig struct {
	Brokers       []string
	GroupID       string
	Topics        []string
	MinBytes      int
	MaxBytes      int
	RetryInterval time.Duration
	CommitSync    bool
}

// DefaultConsumerConfig returns sensible defaults for ConsumerConfig
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		MinBytes:      1,
		MaxBytes:      10e6, // 10MB
		RetryInterval: time.Second,
		CommitSync:    true,
	}
}

// ConsumerManager reads from a set of topics and pushes messages into outCh.
type ConsumerManager struct {
	readers []*kafka.Reader
	config  ConsumerConfig
}

// NewConsumerManager creates a new consumer manager with the given configuration.
func NewConsumerManager(config ConsumerConfig) *ConsumerManager {
	if config.MinBytes <= 0 {
		config.MinBytes = DefaultConsumerConfig().MinBytes
	}
	if config.MaxBytes <= 0 {
		config.MaxBytes = DefaultConsumerConfig().MaxBytes
	}
	if config.RetryInterval <= 0 {
		config.RetryInterval = DefaultConsumerConfig().RetryInterval
	}

	var readers []*kafka.Reader
	for _, t := range config.Topics {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  config.Brokers,
			GroupID:  config.GroupID,
			Topic:    t,
			MinBytes: config.MinBytes,
			MaxBytes: config.MaxBytes,
		})
		readers = append(readers, r)
	}
	return &ConsumerManager{
		readers: readers,
		config:  config,
	}
}

// Start consumes messages and sends to outCh. Each reader runs in its goroutine.
func (cm *ConsumerManager) Start(ctx context.Context, outCh chan<- *Message) {
	for _, r := range cm.readers {
		go cm.consumeMessages(ctx, r, outCh)
	}
}

// consumeMessages handles the message consumption loop for a single reader
func (cm *ConsumerManager) consumeMessages(ctx context.Context, r *kafka.Reader, outCh chan<- *Message) {
	topic := r.Config().Topic
	logger := slog.With("topic", topic)
	logger.Info("starting consumer")

	for {
		select {
		case <-ctx.Done():
			logger.Info("stopping consumer", "reason", ctx.Err())
			return
		default:
			// Continue processing
		}

		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			logger.Error("failed to fetch message", "error", err)
			time.Sleep(cm.config.RetryInterval)
			continue
		}

		msg := &Message{
			Topic:   topic,
			Key:     m.Key,
			Value:   m.Value,
			Headers: m.Headers,
			Time:    m.Time,
		}

		select {
		case outCh <- msg:
			// Message sent successfully
		case <-ctx.Done():
			logger.Info("context canceled during send", "reason", ctx.Err())
			return
		}

		// Commit after handing off
		if err := r.CommitMessages(ctx, m); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			logger.Error("failed to commit message", "error", err, "offset", m.Offset)
		}
	}
}

// Close gracefully closes all Kafka readers
func (cm *ConsumerManager) Close() error {
	var lastErr error
	for i, r := range cm.readers {
		if err := r.Close(); err != nil {
			lastErr = err
			slog.Error("failed to close reader", "index", i, "error", err)
		}
	}
	return lastErr
}

// Topics returns the list of topics this consumer is subscribed to
func (cm *ConsumerManager) Topics() []string {
	topics := make([]string, 0, len(cm.readers))
	for _, r := range cm.readers {
		topics = append(topics, r.Config().Topic)
	}
	return topics
}
