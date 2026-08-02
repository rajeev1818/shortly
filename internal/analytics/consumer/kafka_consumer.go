package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rajeev1818/shortly/internal/analytics/event"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	db     *pgxpool.Pool
}

func NewConsumer(brokers []string, topic, groupID string, db *pgxpool.Pool) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			GroupID:  groupID,
			Topic:    topic,
			MinBytes: 1,
			MaxBytes: 10e6,
			MaxWait:  500 * time.Millisecond,
		}),
		db: db,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		batch := []kafka.Message{}

		flushCtx, flushCancel := context.WithTimeout(ctx, 2*time.Second)
		for len(batch) < 100 {
			msg, err := c.reader.FetchMessage(flushCtx)
			if err != nil {
				if ctx.Err() != nil {
					flushCancel()
					return nil
				}
				break // flush timeout or transient error — process what we have
			}
			batch = append(batch, msg)
		}
		flushCancel()

		if len(batch) == 0 {
			continue
		}

		tx, err := c.db.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}

		for _, msg := range batch {

			var clickEvent event.ClickEvent
			err := json.Unmarshal(msg.Value, &clickEvent)
			if err != nil {
				tx.Rollback(ctx)
				return err
			}
			bucketHour := clickEvent.Timestamp.UTC().Truncate(time.Hour)
			_, err = tx.Exec(ctx, `INSERT INTO click_stats(short_code, bucket_hour, clicks)
                     VALUES($1, $2, 1)
                     ON CONFLICT (short_code, bucket_hour)
                     DO UPDATE SET clicks = click_stats.clicks + excluded.clicks`, clickEvent.ShortCode, bucketHour)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Errorf("exec insert: %w", err)
			}

		}
		if err := tx.Commit(ctx); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("commit batch: %w", err)
		}

		if err := c.reader.CommitMessages(ctx, batch...); err != nil {
			slog.Error("failed to commit kafka offsets", "error", err)
		}

	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
