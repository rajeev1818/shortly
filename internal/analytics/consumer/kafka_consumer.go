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
			Brokers:     brokers,
			GroupID:     groupID,
			Topic:       topic,
			MinBytes:    1,
			MaxBytes:    10e6,
			MaxWait:     500 * time.Millisecond,
			StartOffset: kafka.FirstOffset,
		}),
		db: db,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	msgs := make(chan kafka.Message, 200)

	// Fetch goroutine uses parent ctx — never cancels until shutdown.
	// This keeps the consumer group connection stable.
	go func() {
		for {
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				slog.Error("fetch message error", "error", err)
				continue
			}
			select {
			case msgs <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var batch []kafka.Message

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgs:
			batch = append(batch, msg)
			if len(batch) < 100 {
				continue
			}
			if err := c.processBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}
			if err := c.processBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
}

func (c *Consumer) processBatch(ctx context.Context, batch []kafka.Message) error {
	tx, err := c.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	for _, msg := range batch {
		var clickEvent event.ClickEvent
		if err := json.Unmarshal(msg.Value, &clickEvent); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("unmarshal event: %w", err)
		}
		bucketHour := clickEvent.Timestamp.UTC().Truncate(time.Hour)
		_, err = tx.Exec(ctx, `INSERT INTO click_stats(short_code, bucket_hour, clicks)
                     VALUES($1, $2, 1)
                     ON CONFLICT (short_code, bucket_hour)
                     DO UPDATE SET clicks = click_stats.clicks + excluded.clicks`,
			clickEvent.ShortCode, bucketHour)
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

	slog.Info("processed batch", "count", len(batch))
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
