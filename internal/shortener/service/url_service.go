package service

import (
	"context"
	"errors"

	"github.com/rajeev1818/shortly/internal/codec"
	"github.com/rajeev1818/shortly/internal/shortener/cache"
	"github.com/rajeev1818/shortly/internal/shortener/domain"
	"github.com/rajeev1818/shortly/internal/shortener/repository"
	"golang.org/x/sync/singleflight"
)

type URLStore interface {
	Insert(ctx context.Context, shortCode, longURL string) error
	Get(ctx context.Context, shortCode string) (domain.URLData, error)
}

type URLCache interface {
	Get(ctx context.Context, code string) (string, error)
	Set(ctx context.Context, code, longURL string) error
	SetNegative(ctx context.Context, code string) error
}

type URLService struct {
	store URLStore
	cache URLCache
	group singleflight.Group
}

func NewURLService(store URLStore, cache URLCache) *URLService {
	return &URLService{store: store, cache: cache}
}

func (s *URLService) Shorten(ctx context.Context, longURL string) (string, error) {

	for i := 0; i < 5; i++ {
		shortUrl, err := codec.RandomCode(7)
		if err != nil {
			return "", err
		}

		err = s.store.Insert(ctx, shortUrl, longURL)

		if err == nil {
			s.cache.Set(ctx, shortUrl, longURL)
			return shortUrl, nil
		}

		if errors.Is(err, repository.ErrDuplicateCode) {
			continue
		}

		return "", err
	}

	return "", errors.New("all attempts failed")

}

func (s *URLService) GetByCode(ctx context.Context, shortCode string) (domain.URLData, error) {
	val, err := s.cache.Get(ctx, shortCode)

	if err == nil {
		return domain.URLData{LongURL: val}, nil
	} else if errors.Is(err, cache.ErrNegative) {
		return domain.URLData{}, errors.New("url not found")
	}

	result, err, _ := s.group.Do(shortCode, func() (any, error) {

		dbData, err := s.store.Get(ctx, shortCode)
		if errors.Is(err, repository.ErrNotFound) {
			s.cache.SetNegative(ctx, shortCode)
			return domain.URLData{}, err
		}
		if err != nil {
			return domain.URLData{}, err
		}
		s.cache.Set(ctx, shortCode, dbData.LongURL)
		return dbData, nil
	})

	if err != nil {
		return domain.URLData{}, err
	}

	return result.(domain.URLData), nil

}
