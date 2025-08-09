package kafka

import (
	"context"
	"log"
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

// ConsumerManager reads from a set of topics and pushes messages into outCh.
type ConsumerManager struct {
	readers []*kafka.Reader
}

func NewConsumerManager(brokers []string, groupID string, topics []string) *ConsumerManager {
	var readers []*kafka.Reader
	for _, t := range topics {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    t,
			MinBytes: 1,
			MaxBytes: 10e6, // 10MB
		})
		readers = append(readers, r)
	}
	return &ConsumerManager{readers: readers}
}

// Start consumes messages and sends to outCh. Each reader runs in its goroutine.
func (cm *ConsumerManager) Start(ctx context.Context, outCh chan<- *Message) {
	for _, r := range cm.readers {
		go func(r *kafka.Reader) {
			for {
				m, err := r.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					log.Printf("kafka fetch error: %v", err)
					time.Sleep(time.Second)
					continue
				}
				msg := &Message{
					Topic:   r.Config().Topic,
					Key:     m.Key,
					Value:   m.Value,
					Headers: m.Headers,
					Time:    m.Time,
				}
				select {
				case outCh <- msg:
				case <-ctx.Done():
					return
				}
				// commit after handing off
				if err := r.CommitMessages(ctx, m); err != nil {
					log.Printf("commit error: %v", err)
				}
			}
		}(r)
	}
}

func (cm *ConsumerManager) Close() {
	for _, r := range cm.readers {
		r.Close()
	}
}
