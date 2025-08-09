package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gor0utine/kafka-to-es/internal/indexer"
	"github.com/gor0utine/kafka-to-es/internal/kafka"
)

type mockBulker struct {
	mu    sync.Mutex
	items []indexer.Item
	err   error
}

func (m *mockBulker) Add(ctx context.Context, item indexer.Item) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, item)
	return m.err
}

type mockMapper struct {
	index string
}

func (m *mockMapper) IndexForTopic(topic string) string {
	return m.index
}

func TestWorkerPoolProcessesMessages(t *testing.T) {
	bulker := &mockBulker{}
	mapper := &mockMapper{index: "test-index"}
	inCh := make(chan *kafka.Message, 2)
	wp := NewWorkerPool(bulker, mapper, inCh, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)

	msg := &kafka.Message{
		Topic: "topic1",
		Key:   []byte("key1"),
		Value: []byte(`{"foo":"bar"}`),
		Time:  time.Now(),
	}
	inCh <- msg
	close(inCh)
	time.Sleep(100 * time.Millisecond)

	bulker.mu.Lock()
	defer bulker.mu.Unlock()
	if len(bulker.items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(bulker.items))
	}
	if bulker.items[0].Index != "test-index" {
		t.Errorf("expected index 'test-index', got %q", bulker.items[0].Index)
	}
}

func TestWorkerPoolBulkerError(t *testing.T) {
	bulker := &mockBulker{err: errors.New("fail")}
	mapper := &mockMapper{index: "idx"}
	inCh := make(chan *kafka.Message, 1)
	wp := NewWorkerPool(bulker, mapper, inCh, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.Start(ctx)

	inCh <- &kafka.Message{
		Topic: "topic",
		Key:   []byte("k"),
		Value: []byte(`{}`),
		Time:  time.Now(),
	}
	close(inCh)
	time.Sleep(50 * time.Millisecond)
	// No panic or deadlock expected
}

func TestWorkerPoolShutdown(t *testing.T) {
	bulker := &mockBulker{}
	mapper := &mockMapper{index: "idx"}
	inCh := make(chan *kafka.Message)
	wp := NewWorkerPool(bulker, mapper, inCh, 1)

	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)
	cancel()
	time.Sleep(20 * time.Millisecond)
	// Should exit cleanly
}
