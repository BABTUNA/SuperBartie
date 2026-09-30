// Package vecwriter is Bartie's second destination: the same change stream,
// consumed by its own group, turned into one embedding per row in a pgvector
// table. It exists to make Artie's "real-time data for AI" argument concrete:
// an AI reading this table is seconds behind the source, not a night.
//
// Same crash contract as the writer: offsets commit only after the pgvector
// transaction commits. Embedding happens before the transaction, so a crash
// in between re-embeds on replay. Idempotent, slightly wasteful, correct.
package vecwriter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/BABTUNA/bartie/internal/batch"
	"github.com/BABTUNA/bartie/internal/config"
	"github.com/BABTUNA/bartie/internal/embed"
	"github.com/BABTUNA/bartie/internal/events"
	"github.com/BABTUNA/bartie/internal/metrics"
)

const (
	GroupID        = "bartie-vecwriter"
	Table          = "bartie_vectors"
	flushMaxEvents = 200
	flushInterval  = 2 * time.Second
	embedChunk     = 64
)

type VecWriter struct {
	consumer *kafka.Reader
	pool     *pgxpool.Pool
	emb      embed.Embedder
	buffer   *batch.Buffer
}

func New(ctx context.Context, cfg config.Config, emb embed.Embedder) (*VecWriter, error) {
	if emb == nil {
		return nil, errors.New("no embedder configured")
	}
	pool, err := pgxpool.New(ctx, cfg.DestDSN)
	if err != nil {
		return nil, fmt.Errorf("connect to destination: %w", err)
	}
	if err := ensureSchema(ctx, pool, emb.Dim()); err != nil {
		pool.Close()
		return nil, err
	}
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           []string{cfg.KafkaBroker},
		GroupID:           GroupID,
		Topic:             cfg.Topic,
		StartOffset:       kafka.FirstOffset,
		SessionTimeout:    10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	})
	return &VecWriter{consumer: consumer, pool: pool, emb: emb, buffer: batch.NewBuffer()}, nil
}

func (v *VecWriter) Close() error {
	v.pool.Close()
	return v.consumer.Close()
}

// ensureSchema creates the vector table. The dimension is baked into the
// column type, so a provider change means dropping this table and letting the
// consumer group replay from the start; it is derived data.
func ensureSchema(ctx context.Context, pool *pgxpool.Pool, dim int) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS vector`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  "table"             text NOT NULL,
  pk                  jsonb NOT NULL,
  text                text NOT NULL,
  embedding           vector(%d) NOT NULL,
  __bartie_commit_ts  timestamptz,
  __bartie_updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("table", pk)
)`, Table, dim),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_embedding_idx ON %s USING hnsw (embedding vector_cosine_ops)`, Table, Table),
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			return fmt.Errorf("vector schema: %w", err)
		}
	}
	var existing int
	err := pool.QueryRow(ctx, `SELECT atttypmod FROM pg_attribute WHERE attrelid = $1::regclass AND attname = 'embedding'`, Table).Scan(&existing)
	if err == nil && existing > 0 && existing != dim {
		return fmt.Errorf("%s has vector(%d) but embedder produces %d dims: drop the table to re-embed", Table, existing, dim)
	}
	return nil
}

// Run is the writer's loop with a different flush. Kept separate rather than
// generic: two copies of 40 lines beat an abstraction with two callers.
func (v *VecWriter) Run(ctx context.Context) error {
	metrics.SetPhase("running")
	for {
		fetchCtx := ctx
		var cancel context.CancelFunc
		if !v.buffer.Empty() {
			due := flushInterval - v.buffer.Age()
			fetchCtx, cancel = context.WithTimeout(ctx, max(due, time.Millisecond))
		}
		msg, err := v.consumer.FetchMessage(fetchCtx)
		if cancel != nil {
			cancel()
		}
		switch {
		case err == nil:
			evt, decodeErr := events.Decode(msg.Value)
			if decodeErr != nil {
				return fmt.Errorf("offset %d: %w", msg.Offset, decodeErr)
			}
			v.buffer.Add(evt, msg)
			if v.buffer.Count() < flushMaxEvents {
				continue
			}
		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			return fmt.Errorf("fetch message: %w", err)
		}
		if err := v.flushAll(ctx); err != nil {
			return err
		}
	}
}

type upsert struct {
	doc      document
	commitTS time.Time
}

func (v *VecWriter) flushAll(ctx context.Context) error {
	if v.buffer.Empty() {
		return nil
	}
	tables, msgs := v.buffer.TakeAll()
	start := time.Now()
	lk := newLookups(v.pool)

	var ups []upsert
	var dels []document
	for _, evts := range tables {
		for _, evt := range batch.Dedupe(evts) {
			if _, embedded := embeddedColumns[evt.Table]; !embedded {
				continue
			}
			pk := batch.PKKey(evt)
			if evt.Op == events.OpDelete {
				dels = append(dels, document{table: evt.Table, pk: pk})
				continue
			}
			doc, ok := render(ctx, lk, evt)
			if !ok {
				continue
			}
			doc.pk = pk
			ups = append(ups, upsert{doc: doc, commitTS: evt.CommitTS})
		}
	}

	if len(ups) == 0 && len(dels) == 0 {
		// Nothing embeddable in this batch (e.g. only watering_holes): still
		// advance offsets so the group does not re-read forever.
		return v.commit(ctx, msgs, 0, start)
	}

	// Embed outside the transaction: a slow provider must not hold locks.
	vectors := make([][]float32, len(ups))
	for i := 0; i < len(ups); i += embedChunk {
		end := min(i+embedChunk, len(ups))
		texts := make([]string, 0, end-i)
		for _, u := range ups[i:end] {
			texts = append(texts, u.doc.text)
		}
		vecs, err := v.emb.Embed(ctx, texts, false)
		if err != nil {
			metrics.RecordError(Table, "embed failed", err)
			return err
		}
		copy(vectors[i:end], vecs)
	}

	tx, err := v.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin vector tx: %w", err)
	}
	defer tx.Rollback(ctx)

	b := &pgx.Batch{}
	for i, u := range ups {
		b.Queue(fmt.Sprintf(`
INSERT INTO %s ("table", pk, text, embedding, __bartie_commit_ts, __bartie_updated_at)
VALUES ($1, $2::jsonb, $3, $4::vector, $5, statement_timestamp())
ON CONFLICT ("table", pk) DO UPDATE SET
  text = EXCLUDED.text, embedding = EXCLUDED.embedding,
  __bartie_commit_ts = EXCLUDED.__bartie_commit_ts, __bartie_updated_at = statement_timestamp()`, Table),
			u.doc.table, u.doc.pk, u.doc.text, embed.Literal(vectors[i]), nullable(u.commitTS))
	}
	for _, d := range dels {
		b.Queue(fmt.Sprintf(`DELETE FROM %s WHERE "table" = $1 AND pk = $2::jsonb`, Table), d.table, d.pk)
	}
	if err := tx.SendBatch(ctx, b).Close(); err != nil {
		metrics.RecordError(Table, "vector upsert failed", err)
		return fmt.Errorf("vector batch: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit vector tx: %w", err)
	}
	return v.commit(ctx, msgs, len(ups)+len(dels), start)
}

func (v *VecWriter) commit(ctx context.Context, msgs []kafka.Message, applied int, start time.Time) error {
	if err := v.consumer.CommitMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("commit offsets after vector flush: %w", err)
	}
	metrics.RecordFlush(Table, applied, time.Since(start))
	slog.Info("vector flush", "rows", applied, "ms", time.Since(start).Milliseconds())
	return nil
}

func nullable(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
