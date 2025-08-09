// internal/worker/pool.go
package worker

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"

	"github.com/gor0utine/kafka-to-es/internal/indexer"
	"github.com/gor0utine/kafka-to-es/internal/kafka"
)

type Bulker interface {
	Add(ctx context.Context, item indexer.Item) error
}

type Mapper interface {
	IndexForTopic(topic string) string
}

type Pool struct {
	bulker Bulker
	mapper Mapper
	inCh   <-chan *kafka.Message
	num    int
}

func NewWorkerPool(b Bulker, m Mapper, in <-chan *kafka.Message, num int) *Pool {
	return &Pool{bulker: b, mapper: m, inCh: in, num: num}
}

func (wp *Pool) Start(ctx context.Context) {
	for i := 0; i < wp.num; i++ {
		go wp.run(ctx, i)
	}
}

func (wp *Pool) run(ctx context.Context, id int) {
	log.Printf("worker %d started", id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d shutting down", id)
			return
		case msg, ok := <-wp.inCh:
			if !ok || msg == nil {
				log.Printf("worker %d input channel closed", id)
				return
			}
			idx := wp.mapper.IndexForTopic(msg.Topic)
			doc := map[string]interface{}{
				"payload": json.RawMessage(msg.Value),
				"key":     string(msg.Key),
				"ts":      msg.Time,
				"topic":   msg.Topic,
			}
			b, err := json.Marshal(doc)
			if err != nil {
				log.Printf("worker %d marshal error: %v", id, err)
				continue
			}
			docID := uuid.New().String()
			item := indexer.Item{
				Index: idx,
				ID:    docID,
				Body:  b,
			}
			if err := wp.bulker.Add(ctx, item); err != nil {
				log.Printf("worker %d failed to add to bulker: %v", id, err)
			}
		}
	}
}
