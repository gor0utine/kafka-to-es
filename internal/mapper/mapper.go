package mapper

// Simple in-memory topic -> index mapper

type Mapper struct {
	mapping map[string]string
}

func New(mapping map[string]string) *Mapper {
	return &Mapper{mapping: mapping}
}

func (m *Mapper) IndexForTopic(topic string) string {
	if idx, ok := m.mapping[topic]; ok {
		return idx
	}
	// fallback to topic name
	return topic
}
