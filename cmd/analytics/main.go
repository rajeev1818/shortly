package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rajeev1818/shortly/internal/analytics/consumer"
	"github.com/rajeev1818/shortly/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg, err := config.LoadAnalyticsConfig()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)

	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	c := consumer.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, pool)
	defer c.Close()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.Info("shutting down...")
		cancel()
	}()

	if err := c.Run(ctx); err != nil {
		slog.Error("consumer error", "error", err)
	}

}
