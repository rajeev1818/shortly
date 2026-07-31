package config

import (
	"fmt"

	"github.com/caarlos0/env"
)

type Config struct {
	Port        int    `env:"PORT" envDefault:"8080"`
	DatabaseURL string `env:"DATABASE_URL,required"`
	// JWTSecret   string `env:"JWT_SECRET,required"`
	Environment string `env:"ENVIRONMENT" envDefault:"development"`
	RedisURL    string `env:"REDIS_URL" envDefault:"redis://localhost:6379"`
}

func Load() (*Config, error) {
	cfg := &Config{}

	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return cfg, nil
}
