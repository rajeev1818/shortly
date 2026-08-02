package producer

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/rajeev1818/shortly/internal/analytics/event"
	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
	ch     chan event.ClickEvent
	done   chan struct{}
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	p := &KafkaProducer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
			Async:    true,
		},
		ch:   make(chan event.ClickEvent, 1000),
		done: make(chan struct{}),
	}
	go p.start()

	return p
}

func (p *KafkaProducer) start() {
	defer close(p.done)
	for event := range p.ch {
		eventBytes, err := json.Marshal(event)
		if err != nil {
			slog.Error("Failed to marshal event", "error", err)
			continue
		}
		err = p.writer.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(event.ShortCode),
			Value: eventBytes,
		})
		if err != nil {
			slog.Error("Failed to write message to Kafka", "error", err)
			continue
		}

	}
}

func (p *KafkaProducer) Publish(event event.ClickEvent) error {
	select {
	case p.ch <- event:

	default:
		slog.Warn("Kafka producer channel is full, dropping event")

	}
	return nil
}

func (p *KafkaProducer) Close() {
	close(p.ch)
	<-p.done
	p.writer.Close()
}
