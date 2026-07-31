package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNotFound = errors.New("cache miss")
var ErrNegative = errors.New("cache not found")

const (
	defaultTTL  = 1 * time.Hour
	negativeTTL = 5 * time.Minute
	negativeVal = "__not_found__"
)

type RedisCache struct {
	client *redis.Client
}

func NewRedisCache(client *redis.Client) *RedisCache {
	return &RedisCache{
		client: client,
	}
}

func (r *RedisCache) Get(ctx context.Context, code string) (string, error) {
	val, err := r.client.Get(ctx, code).Result()

	if errors.Is(err, redis.Nil) {
		return "", ErrNotFound
	}

	if err != nil {
		return "", fmt.Errorf("redis get error: %w", err)
	}

	if val == negativeVal {
		return "", ErrNegative
	}

	return val, nil
}

func (r *RedisCache) Set(ctx context.Context, code, longURL string) error {
	err := r.client.Set(ctx, code, longURL, defaultTTL).Err()

	if err != nil {
		return fmt.Errorf("redis set error: %w", err)
	}
	return nil
}

func (r *RedisCache) SetNegative(ctx context.Context, code string) error {
	err := r.client.Set(ctx, code, negativeVal, negativeTTL).Err()

	if err != nil {
		return fmt.Errorf("redis set negative error: %w", err)
	}
	return nil
}
