package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

// Item represents a document to index
type Item struct {
	Index string
	ID    string
	Body  json.RawMessage
}

// Bulker manages bulk indexing for multiple indices.
type Bulker struct {
	es         *elasticsearch.Client
	indexers   map[string]esutil.BulkIndexer
	mu         sync.RWMutex
	numWorkers int
	flushBytes int
	flushIntv  time.Duration
}

// NewBulker creates a new Bulker with configurable options.
func NewBulker(es *elasticsearch.Client, numWorkers, flushBytes int, flushIntv time.Duration) *Bulker {
	return &Bulker{
		es:         es,
		indexers:   make(map[string]esutil.BulkIndexer),
		numWorkers: numWorkers,
		flushBytes: flushBytes,
		flushIntv:  flushIntv,
	}
}

// getIndexerForIndex returns or creates a BulkIndexer for a specific index.
func (b *Bulker) getIndexerForIndex(index string) (esutil.BulkIndexer, error) {
	b.mu.RLock()
	bi, ok := b.indexers[index]
	b.mu.RUnlock()
	if ok {
		return bi, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	// Double-check after acquiring write lock
	if bi, ok := b.indexers[index]; ok {
		return bi, nil
	}
	cfg := esutil.BulkIndexerConfig{
		Client:        b.es,
		Index:         index,
		NumWorkers:    b.numWorkers,
		FlushBytes:    b.flushBytes,
		FlushInterval: b.flushIntv,
	}
	bi, err := esutil.NewBulkIndexer(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create bulk indexer for %s: %w", index, err)
	}
	b.indexers[index] = bi
	return bi, nil
}

// Add adds an item to the bulk queue for indexing.
func (b *Bulker) Add(ctx context.Context, it Item) error {
	bi, err := b.getIndexerForIndex(it.Index)
	if err != nil {
		return err
	}
	return bi.Add(ctx, esutil.BulkIndexerItem{
		Action:     "index",
		DocumentID: it.ID,
		Body:       bytes.NewReader(it.Body),
		OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
			slog.Info("bulk index success",
				"index", it.Index,
				"id", it.ID,
				"version", res.Version,
			)
		},
		OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
			slog.Error("bulk index failure",
				"index", it.Index,
				"id", it.ID,
				"error", err,
				"response", resp,
			)
		},
	})
}

// Close flushes and closes all bulk indexers.
func (b *Bulker) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for idx, bi := range b.indexers {
		if err := bi.Close(ctx); err != nil {
			slog.Error("error closing bulk indexer", "index", idx, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
