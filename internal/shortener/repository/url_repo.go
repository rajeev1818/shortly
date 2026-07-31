package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rajeev1818/shortly/internal/shortener/domain"
)

type URLRepository struct {
	db *pgxpool.Pool
}

var ErrDuplicateCode = errors.New("duplicate short code")

func NewURLRepository(db *pgxpool.Pool) *URLRepository {
	return &URLRepository{db: db}
}

func (r *URLRepository) Insert(ctx context.Context, shortCode string, longURL string) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO urls(short_code, long_url) VALUES($1, $2);
	`, shortCode, longURL)

	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ErrDuplicateCode
		}
		return fmt.Errorf("inserting url: %w", err)
	}

	return nil
}

func (r *URLRepository) Get(ctx context.Context, shortCode string) (domain.URLData, error) {
	var u domain.URLData
	err := r.db.QueryRow(ctx, `SELECT id, short_code, long_url, created_at FROM urls 
		where short_code = $1`, shortCode).Scan(&u.ID, &u.ShortCode, &u.LongURL, &u.CreatedAt)

	if err == pgx.ErrNoRows {
		return domain.URLData{}, fmt.Errorf("url not found: %s", shortCode)
	}

	if err != nil {
		return domain.URLData{}, fmt.Errorf("querying url: %w", err)
	}
	return u, nil
}
