package writer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/BABTUNA/bartie/internal/config"
	"github.com/BABTUNA/bartie/internal/events"
	"github.com/BABTUNA/bartie/internal/metrics"
)

const (
	flushMaxEvents = 500
	flushInterval  = 2 * time.Second
)

type Writer struct {
	consumer *kafka.Reader
	pool     *pgxpool.Pool
	ddl      *ddlManager
	buffer   *Buffer
}

func New(ctx context.Context, cfg config.Config) (*Writer, error) {
	pool, err := pgxpool.New(ctx, cfg.DestDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to destination: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping destination: %w", err)
	}

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{cfg.KafkaBroker},
		GroupID:     "bartie-writer",
		Topic:       cfg.Topic,
		StartOffset: kafka.FirstOffset,
		// A kill -9'd writer never leaves the group; the broker only evicts it
		// after the session timeout, and the restarted writer waits out that
		// rebalance. Short timeouts keep crash recovery snappy.
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	})

	return &Writer{consumer: consumer, pool: pool, ddl: newDDLManager(), buffer: NewBuffer()}, nil
}

func (w *Writer) Close() error {
	w.pool.Close()
	return w.consumer.Close()
}

// Run buffers events and flushes on size or age. The crash-safety contract:
// the Kafka offsets are committed only after every flush transaction has
// committed on the destination. A crash anywhere re-delivers the batch, and
// the merge re-asserts the same final states.
func (w *Writer) Run(ctx context.Context) error {
	metrics.SetPhase("running")
	for {
		// Paused (via the metrics control endpoint): stop consuming so backlog
		// visibly builds on the broker. Anything already buffered waits too.
		if metrics.Paused() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}

		fetchCtx := ctx
		var cancel context.CancelFunc
		if !w.buffer.Empty() {
			// Something is waiting: fetch only until the buffer is due.
			due := flushInterval - w.buffer.Age()
			fetchCtx, cancel = context.WithTimeout(ctx, max(due, time.Millisecond))
		}

		msg, err := w.consumer.FetchMessage(fetchCtx)
		if cancel != nil {
			cancel()
		}
		switch {
		case err == nil:
			evt, decodeErr := events.Decode(msg.Value)
			if decodeErr != nil {
				return fmt.Errorf("offset %d: %w", msg.Offset, decodeErr)
			}
			w.buffer.Add(evt, msg)
			if w.buffer.Count() < flushMaxEvents {
				continue
			}
		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
			// Buffer aged out with no new messages: flush what we have.
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			return fmt.Errorf("fetch message: %w", err)
		}

		if err := w.flushAll(ctx); err != nil {
			return err
		}
	}
}

func (w *Writer) flushAll(ctx context.Context) error {
	if w.buffer.Empty() {
		return nil
	}
	tables, msgs := w.buffer.TakeAll()

	total := 0
	for table, evts := range tables {
		start := time.Now()
		if err := flushTable(ctx, w.pool, w.ddl, table, evts); err != nil {
			metrics.RecordError(table, "flush failed", err)
			return err
		}
		metrics.RecordFlush(table, len(evts), time.Since(start))
		total += len(evts)
	}

	// Only now: every table's transaction is durable on the destination.
	if err := w.consumer.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("commit offsets after flush: %w", err)
	}
	slog.Info("flushed", "events", total, "tables", len(tables))
	return nil
}
