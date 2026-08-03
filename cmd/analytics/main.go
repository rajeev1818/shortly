package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rajeev1818/shortly/internal/analytics/consumer"
	grpchandler "github.com/rajeev1818/shortly/internal/analytics/grpc"
	"github.com/rajeev1818/shortly/internal/analytics/repository"
	"github.com/rajeev1818/shortly/internal/config"
	analyticsv1 "github.com/rajeev1818/shortly/proto/analyticsv1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
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

	opt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		slog.Error("failed to parse redis url", "error", err)
		os.Exit(1)
	}
	redisClient := redis.NewClient(opt)
	defer redisClient.Close()

	repo := repository.NewStatsRepo(pool)
	handler := grpchandler.NewServer(repo, redisClient)

	grpcServer := grpc.NewServer()
	analyticsv1.RegisterAnalyticsServiceServer(grpcServer, handler)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("analytics grpc server starting", "port", cfg.Port)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("grpc server error", "error", err)
		}
	}()

	c := consumer.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup, pool)
	defer c.Close()
	defer grpcServer.GracefulStop()

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
