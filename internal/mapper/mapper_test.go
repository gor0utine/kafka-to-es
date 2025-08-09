package mapper

import (
	"testing"
)

func TestIndexForTopic(t *testing.T) {
	m := New(map[string]string{
		"topic1": "index1",
		"topic2": "index2",
	})

	tests := []struct {
		topic    string
		expected string
	}{
		{"topic1", "index1"},
		{"topic2", "index2"},
		{"unknown", "unknown"}, // default fallback returns topic name
	}

	for _, tt := range tests {
		got := m.IndexForTopic(tt.topic)
		if got != tt.expected {
			t.Errorf("IndexForTopic(%q) = %q, want %q", tt.topic, got, tt.expected)
		}
	}
}

func TestFallbackStrategy(t *testing.T) {
	m := New(
		map[string]string{"foo": "bar"},
		WithFallbackStrategy(func(topic string) string { return "default-index" }),
	)

	if got := m.IndexForTopic("not-mapped"); got != "default-index" {
		t.Errorf("Fallback strategy failed: got %q, want %q", got, "default-index")
	}
}

func TestAddMapping(t *testing.T) {
	m := New(map[string]string{})
	m.AddMapping("t", "i")
	if got := m.IndexForTopic("t"); got != "i" {
		t.Errorf("AddMapping failed: got %q, want %q", got, "i")
	}
}

func TestGetMappingsReturnsCopy(t *testing.T) {
	m := New(map[string]string{"a": "b"})
	cpy := m.GetMappings()
	cpy["a"] = "changed"
	if m.mappings["a"] == "changed" {
		t.Error("GetMappings did not return a copy")
	}
}

func TestStringer(t *testing.T) {
	m := New(map[string]string{"x": "y"})
	s := m.String()
	if s == "" {
		t.Error("String() returned empty string")
	}
}
