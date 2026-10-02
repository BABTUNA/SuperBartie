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

	"github.com/BABTUNA/superbartie/internal/embed"
	"github.com/BABTUNA/superbartie/internal/llm"
	"github.com/BABTUNA/superbartie/internal/vecwriter"
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

	// which search found this row, keyword or vector
	Match string `json:"match"`
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

	// two searches over the same table, merged
	// keyword search finds exact things like an id or a name
	// vector search finds things that mean the same without sharing words
	// either one alone misses rows the other finds
	keyword, err := r.keywordSearch(ctx, mode, table, question)
	if err != nil {
		return nil, err
	}
	vector, err := r.search(ctx, mode, fmt.Sprintf(`
SELECT "table", pk, text, (1 - (embedding <=> $1::vector))::float8 AS score, __bartie_updated_at
FROM %s
ORDER BY embedding <=> $1::vector
LIMIT $2`, table), "vector", embed.Literal(qv[0]), r.topK)
	if err != nil {
		return nil, err
	}

	// keyword hits go first because they are the precise ones
	// a row found by both is kept once
	sources := []Source{}
	seen := map[string]bool{}
	var newest *time.Time
	for _, s := range append(keyword, vector...) {
		key := s.Table + string(s.PK)
		if seen[key] {
			continue
		}
		seen[key] = true
		if newest == nil || s.UpdatedAt.After(*newest) {
			t := s.UpdatedAt
			newest = &t
		}
		sources = append(sources, s)
	}

	// some seed rows are several kilobytes of notes
	// the answer only needs the start of each, and short prompts come back faster
	texts := make([]string, len(sources))
	for i, s := range sources {
		texts[i] = clip(s.Text, maxContextChars)
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

const (
	// how much of each row the model sees
	maxContextChars = 700

	// how many words of the question the keyword search looks at
	maxKeywordTerms = 8
)

func clip(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}

// keywordSearch ranks rows by which words of the question they contain
// each word counts for more the fewer rows have it
// so "9001", which one row has, outweighs "seen", which thousands have
// plain word frequency ranks long rows first and buries the exact match
func (r *RAG) keywordSearch(ctx context.Context, mode, table, question string) ([]Source, error) {
	// let postgres split and stem the question so the words match how rows are indexed
	// stop words like "where" and "was" drop out here
	rows, err := r.pool.Query(ctx, `SELECT unnest(tsvector_to_array(to_tsvector('english', $1)))`, question)
	if err != nil {
		return nil, fmt.Errorf("keyword terms: %w", err)
	}
	var terms []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			rows.Close()
			return nil, err
		}
		terms = append(terms, t)
	}
	rows.Close()
	if len(terms) == 0 {
		return nil, nil
	}
	if len(terms) > maxKeywordTerms {
		terms = terms[:maxKeywordTerms]
	}

	// one pass over the table
	// df counts how many rows contain each word
	// a row's score is the sum of ln(1 + rows / df) over the words it contains
	var counts, score []string
	args := make([]any, 0, len(terms)+1)
	for i, t := range terms {
		args = append(args, t)
		match := fmt.Sprintf("tsv @@ plainto_tsquery('simple', $%d)", i+1)
		counts = append(counts, fmt.Sprintf("count(*) FILTER (WHERE %s)::float8 AS d%d", match, i+1))
		score = append(score, fmt.Sprintf("CASE WHEN %s AND df.d%d > 0 THEN ln(1 + n.n / df.d%d) ELSE 0 END", match, i+1, i+1))
	}
	args = append(args, r.topK)
	query := fmt.Sprintf(`
WITH docs AS MATERIALIZED (
  SELECT "table", pk, text, __bartie_updated_at, to_tsvector('english', text) AS tsv FROM %s
),
n AS (SELECT count(*)::float8 AS n FROM docs),
df AS (SELECT %s FROM docs),
scored AS (
  SELECT docs."table", docs.pk, docs.text, (%s)::float8 AS score, docs.__bartie_updated_at
  FROM docs, n, df
)
SELECT "table", pk, text, score, __bartie_updated_at
FROM scored
WHERE score > 0
ORDER BY score DESC, __bartie_updated_at DESC
LIMIT $%d`, table, strings.Join(counts, ", "), strings.Join(score, " + "), len(terms)+1)

	return r.search(ctx, mode, query, "keyword", args...)
}

// search runs one retrieval query and tags each row with how it was found
// a missing table means the vecwriter or the snapshot has not created it yet
func (r *RAG) search(ctx context.Context, mode, query, match string, args ...any) ([]Source, error) {
	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			if mode == "batch" {
				return nil, fmt.Errorf("no snapshot yet: the batch copy refreshes every %s", r.interval)
			}
			return nil, fmt.Errorf("vector destination not ready: the vecwriter has not created its table yet")
		}
		return nil, fmt.Errorf("%s search: %w", match, err)
	}
	defer rows.Close()

	var out []Source
	for rows.Next() {
		s := Source{Match: match}
		if err := rows.Scan(&s.Table, &s.PK, &s.Text, &s.Score, &s.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
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
