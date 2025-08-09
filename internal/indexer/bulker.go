package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
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

type Bulker struct {
	es       *elasticsearch.Client
	indexers map[string]esutil.BulkIndexer // one per index (optional) -- or a single global BulkIndexer
	mu       sync.Mutex
}

func NewBulker(es *elasticsearch.Client) *Bulker {
	return &Bulker{
		es:       es,
		indexers: make(map[string]esutil.BulkIndexer),
	}
}

// Get or create a BulkIndexer for specific index
func (b *Bulker) getIndexerForIndex(index string) (esutil.BulkIndexer, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if bi, ok := b.indexers[index]; ok {
		return bi, nil
	}
	cfg := esutil.BulkIndexerConfig{
		Client:        b.es,
		Index:         index, // default index for this indexer
		NumWorkers:    2,
		FlushBytes:    5_000_000,
		FlushInterval: 2 * time.Second,
	}
	bi, err := esutil.NewBulkIndexer(cfg)
	if err != nil {
		return nil, err
	}
	b.indexers[index] = bi
	return bi, nil
}

// Add item to bulk queue
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
			// optional hook for metrics
			log.Printf("bulk success: index=%s id=%s version=%d", it.Index, it.ID, res.Version)
		},
		OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
			log.Printf("bulk failure: index=%s id=%s err=%v resp=%v", it.Index, it.ID, err, resp)
		},
	})
}

// Close flushes and closes all bulk indexers
func (b *Bulker) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for idx, bi := range b.indexers {
		if err := bi.Close(ctx); err != nil {
			log.Printf("error closing bulk indexer for %s: %v", idx, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}
