package sink

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/BABTUNA/superbartie/internal/events"
)

// KafkaPublisher writes change events to one topic, keyed by table+PK so a
// row's events stay ordered within a partition.
type KafkaPublisher struct {
	w *kafka.Writer
}

func NewKafkaPublisher(broker, topic string) *KafkaPublisher {
	return &KafkaPublisher{
		w: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Topic:                  topic,
			Balancer:               &kafka.Hash{}, // partition by message key
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
			// kafka-go defaults BatchTimeout to 1s, which makes a synchronous
			// per-transaction publish crawl at ~1 txn/s. CDC wants the events
			// on the wire now; 10ms only coalesces genuinely concurrent sends.
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// Publish blocks until the broker acknowledges every message. The reader's
// LSN ack depends on that guarantee.
func (p *KafkaPublisher) Publish(ctx context.Context, evts []events.ChangeEvent) error {
	msgs := make([]kafka.Message, 0, len(evts))
	for _, evt := range evts {
		key, err := evt.Key()
		if err != nil {
			return err
		}
		value, err := evt.Marshal()
		if err != nil {
			return err
		}
		msgs = append(msgs, kafka.Message{Key: key, Value: value})
	}
	if err := p.w.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("write %d messages to kafka: %w", len(msgs), err)
	}
	return nil
}

func (p *KafkaPublisher) Close() error {
	return p.w.Close()
}
