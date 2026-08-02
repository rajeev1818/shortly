package config

import (
	"fmt"

	"github.com/caarlos0/env"
)

type ShortenerConfig struct {
	Port        int    `env:"PORT" envDefault:"9090"`
	DatabaseURL string `env:"DATABASE_URL,required"`
	RedisURL    string `env:"REDIS_URL" envDefault:"redis://localhost:6379"`
}

type GatewayConfig struct {
	Port          int    `env:"PORT" envDefault:"8080"`
	ShortenerAddr string `env:"SHORTENER_ADDR" envDefault:"localhost:9090"`
	RedisURL      string `env:"REDIS_URL" envDefault:"redis://localhost:6379"`
	RateLimit     int    `env:"RATE_LIMIT" envDefault:"10"`      // requests per window
	RateWindow    int    `env:"RATE_WINDOW_SEC" envDefault:"60"` // window in seconds
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
