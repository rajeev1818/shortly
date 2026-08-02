package grpc

import (
	"context"
	"errors"

	"github.com/rajeev1818/shortly/internal/shortener/repository"
	"github.com/rajeev1818/shortly/internal/shortener/service"
	shortenerv1 "github.com/rajeev1818/shortly/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	shortenerv1.UnimplementedShortenerServiceServer
	svc *service.URLService
}

func NewServer(svc *service.URLService) *Server {
	return &Server{svc: svc}
}

func (s *Server) Shorten(ctx context.Context, r *shortenerv1.ShortenRequest) (*shortenerv1.ShortenResponse, error) {
	val, err := s.svc.Shorten(ctx, r.GetLongUrl())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to shorten url: %v", err)
	}
	return &shortenerv1.ShortenResponse{ShortCode: val}, nil
}

func (s *Server) Resolve(ctx context.Context, r *shortenerv1.ResolveRequest) (*shortenerv1.ResolveResponse, error) {
	val, err := s.svc.GetByCode(ctx, r.GetShortCode())
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "code not found: %s", r.GetShortCode())
		}
		return nil, status.Errorf(codes.Internal, "failed to resolve url: %v", err)
	}
	return &shortenerv1.ResolveResponse{LongUrl: val.LongURL}, nil
}
