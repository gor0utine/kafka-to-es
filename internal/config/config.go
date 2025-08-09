package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		GroupID string   `yaml:"group_id"`
		Topics  []string `yaml:"topics"`
	} `yaml:"kafka"`
	ES struct {
		Addresses []string `yaml:"addresses"`
		Username  string   `yaml:"username"`
		Password  string   `yaml:"password"`
	} `yaml:"es"`
	Mappings map[string]string `yaml:"mappings"`
	Worker   struct {
		NumWorkers    int           `yaml:"num_workers"`
		BatchSize     int           `yaml:"batch_size"`
		BatchBytes    int           `yaml:"batch_bytes"`
		FlushInterval time.Duration `yaml:"flush_interval_seconds"`
	} `yaml:"worker"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	// defaults
	if c.Worker.NumWorkers == 0 {
		c.Worker.NumWorkers = 4
	}
	if c.Worker.BatchSize == 0 {
		c.Worker.BatchSize = 500
	}
	if c.Worker.BatchBytes == 0 {
		c.Worker.BatchBytes = 5_000_000
	}
	if c.Worker.FlushInterval == 0 {
		c.Worker.FlushInterval = 2
	}
	return &c, nil
}
