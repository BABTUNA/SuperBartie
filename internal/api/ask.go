package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/bartie/internal/embed"
	"github.com/BABTUNA/bartie/internal/llm"
	"github.com/BABTUNA/bartie/internal/vecwriter"
)

// RAG answers questions from the vector destination. Two modes:
//
//	live   reads bartie_vectors, which the vecwriter keeps seconds behind the source
//	batch  reads a snapshot copy refreshed on a timer, standing in for nightly ETL
//
// Same question, same model, two freshness regimes. That contrast is the demo.
type RAG struct {
	pool     *pgxpool.Pool
	emb      embed.Embedder
	llm      llm.Client
	interval time.Duration
	topK     int
}

const snapshotTable = vecwriter.Table + "_snapshot"

func NewRAG(ctx context.Context, destDSN string, emb embed.Embedder, model llm.Client, snapshotEvery time.Duration) (*RAG, error) {
	pool, err := pgxpool.New(ctx, destDSN)
	if err != nil {
		return nil, fmt.Errorf("connect dest for rag: %w", err)
	}
	r := &RAG{pool: pool, emb: emb, llm: model, interval: snapshotEvery, topK: 5}
	go r.snapshotLoop(ctx)
	return r, nil
}

type Source struct {
	Table     string          `json:"table"`
	PK        json.RawMessage `json:"pk"`
	Text      string          `json:"text"`
	Score     float64         `json:"score"`
	UpdatedAt time.Time       `json:"updatedAt"`
}

type Answer struct {
	Question   string     `json:"question"`
	Mode       string     `json:"mode"`    // live | batch
	AsOf       *time.Time `json:"asOf"`    // live: newest source row; batch: snapshot time
	StaleBy    *float64   `json:"staleBy"` // batch only: seconds since the snapshot
	Answer     string     `json:"answer"`
	Sources    []Source   `json:"sources"`
	Model      string     `json:"model"`
	Embedder   string     `json:"embedder"`
	LatencyMs  int64      `json:"latencyMs"`
	AnsweredAt time.Time  `json:"answeredAt"`
}

func (r *RAG) Ask(ctx context.Context, question, mode string) (any, error) {
	start := time.Now()
	if mode == "" {
		mode = "live"
	}
	table := vecwriter.Table
	if mode == "batch" {
		table = snapshotTable
	} else if mode != "live" {
		return nil, fmt.Errorf(`mode must be "live" or "batch"`)
	}

	qv, err := r.emb.Embed(ctx, []string{question}, true)
	if err != nil {
		return nil, fmt.Errorf("embed question: %w", err)
	}

	rows, err := r.pool.Query(ctx, fmt.Sprintf(`
SELECT "table", pk, text, 1 - (embedding <=> $1::vector) AS score, __bartie_updated_at
FROM %s
ORDER BY embedding <=> $1::vector
LIMIT $2`, table), embed.Literal(qv[0]), r.topK)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" { // undefined_table
			if mode == "batch" {
				return nil, fmt.Errorf("no snapshot yet: the batch copy refreshes every %s", r.interval)
			}
			return nil, fmt.Errorf("vector destination not ready: the vecwriter has not created %s yet", table)
		}
		return nil, fmt.Errorf("vector search: %w", err)
	}
	defer rows.Close()

	var sources []Source
	var newest *time.Time
	for rows.Next() {
		var s Source
		if err := rows.Scan(&s.Table, &s.PK, &s.Text, &s.Score, &s.UpdatedAt); err != nil {
			return nil, err
		}
		if newest == nil || s.UpdatedAt.After(*newest) {
			t := s.UpdatedAt
			newest = &t
		}
		sources = append(sources, s)
	}
	if sources == nil {
		sources = []Source{}
	}

	texts := make([]string, len(sources))
	for i, s := range sources {
		texts[i] = s.Text
	}
	answer, err := r.llm.Answer(ctx, question, texts)
	if err != nil {
		return nil, err
	}

	out := Answer{
		Question: question, Mode: mode, Answer: answer, Sources: sources,
		Model: r.llm.Model(), Embedder: r.emb.Name(),
		LatencyMs: time.Since(start).Milliseconds(), AnsweredAt: time.Now().UTC(),
	}
	if mode == "batch" {
		if at, err := r.snapshotAt(ctx); err == nil {
			out.AsOf = &at
			stale := time.Since(at).Seconds()
			out.StaleBy = &stale
		}
	} else {
		out.AsOf = newest
	}
	return out, nil
}

// --- batch snapshot --------------------------------------------------------

func (r *RAG) snapshotLoop(ctx context.Context) {
	// First refresh soon after start so "batch" mode works within a minute
	// of a fresh deploy, then on the configured cadence.
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if err := r.refreshSnapshot(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("snapshot refresh failed", "err", err)
		}
		timer.Reset(r.interval)
	}
}

func (r *RAG) refreshSnapshot(ctx context.Context) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (LIKE %s INCLUDING DEFAULTS)`, snapshotTable, vecwriter.Table),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s_meta (id int PRIMARY KEY DEFAULT 1, refreshed_at timestamptz NOT NULL)`, snapshotTable),
		fmt.Sprintf(`TRUNCATE %s`, snapshotTable),
		fmt.Sprintf(`INSERT INTO %s SELECT * FROM %s`, snapshotTable, vecwriter.Table),
		fmt.Sprintf(`INSERT INTO %s_meta (id, refreshed_at) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET refreshed_at = now()`, snapshotTable),
	}
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			if strings.Contains(err.Error(), "42P01") || strings.Contains(err.Error(), "does not exist") {
				return fmt.Errorf("vector table not created yet")
			}
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	slog.Info("batch snapshot refreshed", "table", snapshotTable)
	return nil
}

func (r *RAG) snapshotAt(ctx context.Context) (time.Time, error) {
	var at time.Time
	err := r.pool.QueryRow(ctx, fmt.Sprintf(`SELECT refreshed_at FROM %s_meta WHERE id = 1`, snapshotTable)).Scan(&at)
	return at, err
}
