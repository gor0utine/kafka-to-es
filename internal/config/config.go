package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the root configuration for the application.
type Config struct {
	Kafka    KafkaConfig       `yaml:"kafka"`
	ES       ESConfig          `yaml:"es"`
	Mappings map[string]string `yaml:"mappings"`
	Worker   WorkerConfig      `yaml:"worker"`
}

// KafkaConfig holds Kafka connection and consumer settings.
type KafkaConfig struct {
	Brokers []string `yaml:"brokers"`
	GroupID string   `yaml:"group_id"`
	Topics  []string `yaml:"topics"`
}

// ESConfig holds Elasticsearch connection settings.
type ESConfig struct {
	Addresses []string `yaml:"addresses"`
	Username  string   `yaml:"username"`
	Password  string   `yaml:"password"`
}

// WorkerConfig holds worker and batching settings.
type WorkerConfig struct {
	NumWorkers        int           `yaml:"num_workers"`
	BatchSize         int           `yaml:"batch_size"`
	BatchBytes        int           `yaml:"batch_bytes"`
	FlushIntervalSecs int           `yaml:"flush_interval_seconds"`
	FlushInterval     time.Duration `yaml:"-"`
}

// SetDefaults sets sensible defaults for missing config values.
func (c *Config) SetDefaults() {
	if c.Worker.NumWorkers == 0 {
		c.Worker.NumWorkers = 4
	}
	if c.Worker.BatchSize == 0 {
		c.Worker.BatchSize = 500
	}
	if c.Worker.BatchBytes == 0 {
		c.Worker.BatchBytes = 5_000_000
	}
	if c.Worker.FlushIntervalSecs == 0 {
		c.Worker.FlushIntervalSecs = 2
	}
	c.Worker.FlushInterval = time.Duration(c.Worker.FlushIntervalSecs) * time.Second
}

// Load reads and parses the YAML config file at the given path.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}
	c.SetDefaults()
	return &c, nil
}
