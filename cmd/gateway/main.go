package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rajeev1818/shortly/cmd/gateway/handler"
	"github.com/rajeev1818/shortly/internal/config"
	shortenerv1 "github.com/rajeev1818/shortly/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	cfg, err := config.LoadGatewayConfig()

	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	conn, err := grpc.NewClient(cfg.ShortenerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		slog.Error("failed to connect to shortener", "error", err)
		os.Exit(1)
	}
	defer conn.Close()

	shortenerClient := shortenerv1.NewShortenerServiceClient(conn)

	h := handler.NewHandler(shortenerClient)

	r := chi.NewRouter()

	r.Use(middleware.RequestID, middleware.Logger, middleware.Recoverer)

	r.Post("/shorten", h.Shorten)
	r.Get("/{code}", h.Redirect)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("server starting", "port", cfg.Port)
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		slog.Error("forced shutdown", "error", err)
	}

	slog.Info("server stopped")

}
