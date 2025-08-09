package mapper

import "fmt"

// Mapper provides mapping from Kafka topics to Elasticsearch indices.
// It allows configuration of custom topic->index mappings and handles
// fallback scenarios when a topic has no explicit mapping.
type Mapper struct {
	mappings map[string]string
	fallback func(string) string // Custom fallback strategy
}

// Option represents a configuration option for the Mapper.
type Option func(*Mapper)

// WithFallbackStrategy sets a custom fallback strategy for unmapped topics.
// If not set, the default strategy uses the topic name as the index name.
func WithFallbackStrategy(strategy func(topic string) string) Option {
	return func(m *Mapper) {
		m.fallback = strategy
	}
}

// New creates a Mapper with the given topic->index mappings and options.
func New(mappings map[string]string, opts ...Option) *Mapper {
	m := &Mapper{
		mappings: make(map[string]string, len(mappings)),
		fallback: func(topic string) string { return topic }, // Default fallback
	}

	// Copy mappings to prevent external modification
	for k, v := range mappings {
		m.mappings[k] = v
	}

	// Apply options
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// IndexForTopic returns the index name for a given topic.
// If no mapping exists, it uses the fallback strategy.
func (m *Mapper) IndexForTopic(topic string) string {
	if idx, ok := m.mappings[topic]; ok {
		return idx
	}
	return m.fallback(topic)
}

// AddMapping adds or updates a topic->index mapping.
func (m *Mapper) AddMapping(topic, index string) {
	m.mappings[topic] = index
}

// GetMappings returns a copy of the current mappings.
func (m *Mapper) GetMappings() map[string]string {
	result := make(map[string]string, len(m.mappings))
	for k, v := range m.mappings {
		result[k] = v
	}
	return result
}

// String implements the Stringer interface.
func (m *Mapper) String() string {
	return fmt.Sprintf("Mapper{mappings: %v}", m.mappings)
}
