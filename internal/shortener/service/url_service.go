package service

import (
	"context"
	"errors"

	"github.com/rajeev1818/shortly/internal/codec"
	"github.com/rajeev1818/shortly/internal/shortener/domain"
	"github.com/rajeev1818/shortly/internal/shortener/repository"
)

type URLStore interface {
	Insert(ctx context.Context, shortCode, longURL string) error
	Get(ctx context.Context, shortCode string) (domain.URLData, error)
}

type URLService struct {
	store URLStore
}

func NewURLService(store URLStore) *URLService {
	return &URLService{store: store}
}

func (s *URLService) Shorten(ctx context.Context, longURL string) (string, error) {

	for i := 0; i < 5; i++ {
		shortUrl, err := codec.RandomCode(7)
		if err != nil {
			return "", err
		}

		err = s.store.Insert(ctx, shortUrl, longURL)

		if err == nil {
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
	return s.store.Get(ctx, shortCode)
}
