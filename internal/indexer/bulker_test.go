package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type mockBulkIndexer struct {
	mu       sync.Mutex
	added    []esutil.BulkIndexerItem
	closeOk  bool
	closeErr error
}

func (m *mockBulkIndexer) Add(ctx context.Context, item esutil.BulkIndexerItem) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.added = append(m.added, item)
	return nil
}
func (m *mockBulkIndexer) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closeErr != nil {
		return m.closeErr
	}
	m.closeOk = true
	return nil
}

// The following methods are required to satisfy the interface but are not used in Bulker.
func (m *mockBulkIndexer) Stats() esutil.BulkIndexerStats  { return esutil.BulkIndexerStats{} }
func (m *mockBulkIndexer) Flush(ctx context.Context) error { return nil }

func TestBulker_AddAndClose(t *testing.T) {
	es := &elasticsearch.Client{} // not used in test
	b := NewBulker(es, 1, 1024, time.Second)

	// Patch getIndexerForIndex to use our mock
	mockIdx := &mockBulkIndexer{}
	b.indexers["test-index"] = mockIdx

	body, _ := json.Marshal(map[string]string{"foo": "bar"})
	item := Item{
		Index: "test-index",
		ID:    "id1",
		Body:  body,
	}
	ctx := context.Background()
	err := b.Add(ctx, item)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	mockIdx.mu.Lock()
	if len(mockIdx.added) != 1 {
		t.Errorf("expected 1 item added, got %d", len(mockIdx.added))
	}
	mockIdx.mu.Unlock()

	// Test Close
	err = b.Close(ctx)
	if err != nil {
		t.Errorf("Close() error = %v", err)
	}
	mockIdx.mu.Lock()
	if !mockIdx.closeOk {
		t.Errorf("expected closeOk true")
	}
	mockIdx.mu.Unlock()
}

func TestBulker_getIndexerForIndexCreatesNew(t *testing.T) {
	es := &elasticsearch.Client{}
	b := NewBulker(es, 1, 1024, time.Second)
	// Remove all indexers to force creation
	b.indexers = make(map[string]esutil.BulkIndexer)
	// Should create a new indexer (real one, but we just check error)
	_, err := b.getIndexerForIndex("new-index")
	if err != nil {
		t.Errorf("getIndexerForIndex() error = %v", err)
	}
}

func TestBulker_CloseError(t *testing.T) {
	es := &elasticsearch.Client{}
	b := NewBulker(es, 1, 1024, time.Second)
	mockIdx := &mockBulkIndexer{closeErr: errors.New("close fail")}
	b.indexers["fail-index"] = mockIdx
	ctx := context.Background()
	err := b.Close(ctx)
	if err == nil || err.Error() != "close fail" {
		t.Errorf("expected close fail error, got %v", err)
	}
}

func TestBulker_AddWithRealIndexer(t *testing.T) {
	// This test ensures Add works with a real BulkIndexer (no ES connection needed)
	es := &elasticsearch.Client{}
	b := NewBulker(es, 1, 1024, time.Millisecond)
	body := json.RawMessage(`{"foo":"bar"}`)
	item := Item{Index: "real-index", ID: "id2", Body: body}
	ctx := context.Background()
	// Should not panic or error
	_ = b.Add(ctx, item)
}
