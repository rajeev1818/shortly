package grpc

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func LoggingInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	resp, err := handler(ctx, req)

	if err != nil {
		slog.Error("grpc call", "method", info.FullMethod, "duration", time.Since(start), "error", err)
	} else {
		slog.Info("grpc call", "method", info.FullMethod, "duration", time.Since(start))
	}

	return resp, err
}

func RecoveryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("panic recovered", "method", info.FullMethod, "error", r)
			err = status.Errorf(codes.Internal, "panic: %v", r)
		}
	}()
	return handler(ctx, req)
}
