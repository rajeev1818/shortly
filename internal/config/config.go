package config

import (
	"fmt"

	"github.com/caarlos0/env"
)

type ShortenerConfig struct {
	Port         int      `env:"PORT" envDefault:"9090"`
	DatabaseURL  string   `env:"DATABASE_URL,required"`
	RedisURL     string   `env:"REDIS_URL" envDefault:"redis://localhost:6379"`
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:","`
	KafkaTopic   string   `env:"KAFKA_TOPIC" envDefault:"click.events"`
}

type GatewayConfig struct {
	Port          int    `env:"PORT" envDefault:"8080"`
	ShortenerAddr string `env:"SHORTENER_ADDR" envDefault:"localhost:9090"`
	RedisURL      string `env:"REDIS_URL" envDefault:"redis://localhost:6379"`
	RateLimit     int    `env:"RATE_LIMIT" envDefault:"10"`      // requests per window
	RateWindow    int    `env:"RATE_WINDOW_SEC" envDefault:"60"` // window in seconds
}

type AnalyticsConfig struct {
	DatabaseURL  string   `env:"DATABASE_URL,required"`
	KafkaBrokers []string `env:"KAFKA_BROKERS" envSeparator:","`
	KafkaTopic   string   `env:"KAFKA_TOPIC" envDefault:"click.events"`
	KafkaGroup   string   `env:"KAFKA_GROUP" envDefault:"analytics"`
}

func LoadShortenerConfig() (*ShortenerConfig, error) {
	cfg := &ShortenerConfig{}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing shortener config: %w", err)
	}
	return cfg, nil
}

func LoadGatewayConfig() (*GatewayConfig, error) {
	cfg := &GatewayConfig{}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing gateway config: %w", err)
	}
	return cfg, nil
}

func LoadAnalyticsConfig() (*AnalyticsConfig, error) {
	cfg := &AnalyticsConfig{}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing analytics config: %w", err)
	}
	return cfg, nil
}
