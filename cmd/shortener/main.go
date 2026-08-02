package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rajeev1818/shortly/internal/config"
	urlcache "github.com/rajeev1818/shortly/internal/shortener/cache"
	grpchandler "github.com/rajeev1818/shortly/internal/shortener/grpc"
	"github.com/rajeev1818/shortly/internal/shortener/repository"
	"github.com/rajeev1818/shortly/internal/shortener/service"
	shortenerv1 "github.com/rajeev1818/shortly/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg, err := config.LoadShortenerConfig()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
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

	if err := redisClient.Ping(ctx).Err(); err != nil {
		slog.Error("failed to connect to redis", "error", err)
		os.Exit(1)
	}
	slog.Info("redis connected")

	migrationSQL, err := os.ReadFile("migrations/001_url.sql")

	if err != nil {
		slog.Error("failed to read migration", "error", err)
		os.Exit(1)
	}

	if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
		slog.Error("failed to run migration", "error", err)
		os.Exit(1)
	}
	slog.Info("migrations applied")

	repo := repository.NewURLRepository(pool)
	redisCache := urlcache.NewRedisCache(redisClient)
	svc := service.NewURLService(repo, redisCache)

	grpcHandler := grpchandler.NewServer(svc)
	grpcServer := grpc.NewServer(grpc.ChainUnaryInterceptor(grpchandler.RecoveryInterceptor, grpchandler.LoggingInterceptor))
	shortenerv1.RegisterShortenerServiceServer(grpcServer, grpcHandler)

	lis, err := net.Listen("tcp", ":9090")

	if err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("grpc server starting", "port", 9090)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("grpc server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	slog.Info("shutting down grpc server...")
	grpcServer.GracefulStop()
	slog.Info("server stopped")
}
