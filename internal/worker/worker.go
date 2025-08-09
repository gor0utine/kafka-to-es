package worker

import (
	"context"
	"encoding/json"
	"log"

	"github.com/google/uuid"

	"github.com/gor0utine/kafka-to-es/internal/indexer"
	"github.com/gor0utine/kafka-to-es/internal/kafka"
	"github.com/gor0utine/kafka-to-es/internal/mapper"
)

type Pool struct {
	bulker *indexer.Bulker
	mapper *mapper.Mapper
	inCh   <-chan *kafka.Message
	num    int
}

func NewWorkerPool(b *indexer.Bulker, m *mapper.Mapper, in <-chan *kafka.Message, num int) *Pool {
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
			return
		case msg := <-wp.inCh:
			if msg == nil {
				// channel closed
				return
			}
			// Map topic -> index
			idx := wp.mapper.IndexForTopic(msg.Topic)
			// Optionally transform message (parse, enrich, etc)
			doc := map[string]interface{}{
				"payload": json.RawMessage(msg.Value),
				"key":     string(msg.Key),
				"ts":      msg.Time,
				"topic":   msg.Topic,
			}
			b, err := json.Marshal(doc)
			if err != nil {
				log.Printf("marshal error: %v", err)
				continue
			}
			// Use UUID as doc ID (or derive from message)
			id := uuid.New().String()
			item := indexer.Item{
				Index: idx,
				ID:    id,
				Body:  b,
			}
			if err := wp.bulker.Add(ctx, item); err != nil {
				log.Printf("failed to add to bulker: %v", err)
			}
		}
	}
}
