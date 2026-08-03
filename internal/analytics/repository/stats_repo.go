package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type StatsRepo struct {
	db *pgxpool.Pool
}

type HourlyBucket struct {
	Hour   time.Time
	Clicks int64
}

type Stats struct {
	ShortCode   string
	TotalClicks int64
	Hourly      []HourlyBucket
}

func NewStatsRepo(db *pgxpool.Pool) *StatsRepo {
	return &StatsRepo{db: db}
}

var ErrNotFound = errors.New("stats not found")

func (r *StatsRepo) GetStats(ctx context.Context, shortCode string) (Stats, error) {
	rows, err := r.db.Query(ctx, `
		SELECT bucket_hour, clicks FROM click_stats WHERE short_code = $1
		ORDER BY bucket_hour DESC
		LIMIT 48
	`, shortCode)

	var total int64
	var hourly []HourlyBucket

	if err != nil {
		return Stats{}, fmt.Errorf("querying click_stats: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		var bucket HourlyBucket
		err := rows.Scan(&bucket.Hour, &bucket.Clicks)

		if err != nil {
			return Stats{}, fmt.Errorf("scanning row: %w", err)
		}
		total += bucket.Clicks
		hourly = append(hourly, bucket)
	}

	if err := rows.Err(); err != nil {
		return Stats{}, fmt.Errorf("iterating rows: %w", err)
	}

	if len(hourly) == 0 {
		return Stats{}, ErrNotFound
	}
	return Stats{ShortCode: shortCode, TotalClicks: total, Hourly: hourly}, nil
}
