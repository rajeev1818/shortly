package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/rajeev1818/shortly/internal/analytics/repository"
	"github.com/rajeev1818/shortly/proto/analyticsv1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	analyticsv1.UnimplementedAnalyticsServiceServer
	repo  *repository.StatsRepo
	cache *redis.Client
}

func NewServer(repo *repository.StatsRepo, cache *redis.Client) *Server {
	return &Server{repo: repo, cache: cache}
}

func (s *Server) GetStats(ctx context.Context, req *analyticsv1.GetStatsRequest) (*analyticsv1.GetStatsResponse, error) {
	key := "stats:" + req.GetShortCode()
	val, err := s.cache.Get(ctx, key).Result()

	var resp analyticsv1.GetStatsResponse

	if err = json.Unmarshal([]byte(val), &resp); err != nil {
		slog.Info("cache miss fallback to db")
		stats, err := s.repo.GetStats(ctx, req.GetShortCode())
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "url not found")
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Internal server error")
		}

		hourly := make([]*analyticsv1.HourlyBucket, len(stats.Hourly))
		for i, b := range stats.Hourly {
			hourly[i] = &analyticsv1.HourlyBucket{
				Hour:   b.Hour.Format(time.RFC3339),
				Clicks: b.Clicks,
			}
		}
		resp := analyticsv1.GetStatsResponse{
			ShortCode:   stats.ShortCode,
			TotalClicks: stats.TotalClicks,
			Hourly:      hourly,
		}
		bytes, _ := json.Marshal(&resp)

		if err := s.cache.Set(ctx, key, bytes, 30*time.Second).Err(); err != nil {
			slog.Warn("failed to cache stats", "error", err)
		}
		return &resp, nil
	}
	return &resp, nil
}
