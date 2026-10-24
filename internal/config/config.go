// Package config validates plugin-owned YAML configuration.
package config

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Driver             string `yaml:"driver"`
	DSN                string `yaml:"dsn"`
	WriteTimeout       string `yaml:"write_timeout"`
	DrainTimeout       string `yaml:"drain_timeout"`
	MaxRequestBytes    int    `yaml:"max_request_bytes"`
	MaxBufferBytes     int    `yaml:"max_buffer_bytes"`
	MaxPendingRequests int    `yaml:"max_pending_requests"`
	QueueSize          int    `yaml:"queue_size"`
	RequestTTL         string `yaml:"request_ttl"`
}

func Parse(raw string) (Config, error) {
	c := Config{Driver: "sqlite", WriteTimeout: "5s", DrainTimeout: "5s", MaxRequestBytes: 64 << 20, MaxBufferBytes: 256 << 20, MaxPendingRequests: 1024, QueueSize: 32, RequestTTL: "30m"}
	// enabled and priority are host-owned; unknown fields remain compatible.
	if err := yaml.Unmarshal([]byte(raw), &c); err != nil {
		return Config{}, fmt.Errorf("invalid plugin YAML: %w", err)
	}
	c.Driver = strings.ToLower(strings.TrimSpace(c.Driver))
	if c.Driver != "sqlite" && c.Driver != "postgres" {
		return Config{}, fmt.Errorf("driver must be sqlite or postgres")
	}
	if strings.TrimSpace(c.DSN) == "" {
		if c.Driver == "postgres" {
			return Config{}, fmt.Errorf("postgres requires dsn")
		}
		c.DSN = "cpa-distill.db"
	}
	d, err := time.ParseDuration(c.WriteTimeout)
	if err != nil || d <= 0 || d > time.Minute {
		return Config{}, fmt.Errorf("write_timeout must be positive and at most 1m")
	}
	drain, err := time.ParseDuration(c.DrainTimeout)
	if err != nil || drain <= 0 || drain > time.Minute {
		return Config{}, fmt.Errorf("drain_timeout must be positive and at most 1m")
	}
	ttl, err := time.ParseDuration(c.RequestTTL)
	if err != nil || ttl <= 0 {
		return Config{}, fmt.Errorf("request_ttl must be positive")
	}
	if c.MaxRequestBytes <= 0 || c.MaxBufferBytes < c.MaxRequestBytes || c.MaxPendingRequests <= 0 || c.QueueSize <= 0 || c.QueueSize > 4096 {
		return Config{}, fmt.Errorf("invalid buffer or queue limits")
	}
	return c, nil
}

func (c Config) Timeout() time.Duration     { d, _ := time.ParseDuration(c.WriteTimeout); return d }
func (c Config) DrainBudget() time.Duration { d, _ := time.ParseDuration(c.DrainTimeout); return d }
func (c Config) TTL() time.Duration         { d, _ := time.ParseDuration(c.RequestTTL); return d }
