package verify

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/superbartie/internal/config"
)

// TableResult is one table's source-vs-destination comparison.
type TableResult struct {
	Table         string `json:"table"`
	SourceRows    int64  `json:"sourceRows"`
	DestRows      int64  `json:"destRows"`
	ChecksumMatch bool   `json:"checksumMatch"`
	Detail        string `json:"detail,omitempty"` // why it did not match, when it did not
}

// Result is one full pass over every replicated table.
type Result struct {
	Tables    []TableResult `json:"tables"`
	Match     bool          `json:"match"`
	CheckedAt time.Time     `json:"checkedAt"`
}

// Run compares every replicated table on the source against the destination,
// retrying until they match or the timeout expires. Retrying exists because a
// live pipeline has in-flight events: failure is never converging, not a
// single mismatched snapshot.
//
// It knows nothing about the reader, writer, or Kafka: it reads both ends and
// nothing else, which is what makes its MATCH trustworthy.
func Run(ctx context.Context, cfg config.Config, timeout time.Duration) error {
	res, err := Converge(ctx, cfg, timeout)
	if err != nil {
		return err
	}
	if res.Match {
		fmt.Printf("VERIFY: all %d tables MATCH\n", len(res.Tables))
		return nil
	}
	n := 0
	for _, t := range res.Tables {
		if !t.ChecksumMatch {
			n++
			fmt.Println("MISMATCH:", t.Table+":", t.Detail)
		}
	}
	return fmt.Errorf("verify failed: %d of %d tables did not converge within %s", n, len(res.Tables), timeout)
}

// Converge runs Compare until every table matches or the timeout passes, and
// returns the last result either way.
func Converge(ctx context.Context, cfg config.Config, timeout time.Duration) (Result, error) {
	source, dest, shapes, err := open(ctx, cfg)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	defer dest.Close()

	deadline := time.Now().Add(timeout)
	for {
		res, err := compareAll(ctx, source, dest, shapes)
		if err != nil {
			return Result{}, err
		}
		if res.Match || time.Now().After(deadline) {
			return res, nil
		}
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Compare is a single pass with no retry: what the api serves.
func Compare(ctx context.Context, cfg config.Config) (Result, error) {
	source, dest, shapes, err := open(ctx, cfg)
	if err != nil {
		return Result{}, err
	}
	defer source.Close()
	defer dest.Close()
	return compareAll(ctx, source, dest, shapes)
}

func open(ctx context.Context, cfg config.Config) (*pgxpool.Pool, *pgxpool.Pool, []tableShape, error) {
	source, err := pgxpool.New(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect source: %w", err)
	}
	dest, err := pgxpool.New(ctx, cfg.DestDSN)
	if err != nil {
		source.Close()
		return nil, nil, nil, fmt.Errorf("connect dest: %w", err)
	}

	tables, err := listTables(ctx, source, cfg.Publication)
	if err != nil {
		source.Close()
		dest.Close()
		return nil, nil, nil, err
	}
	if len(tables) == 0 {
		source.Close()
		dest.Close()
		return nil, nil, nil, fmt.Errorf("publication %q covers no tables", cfg.Publication)
	}

	shapes := make([]tableShape, 0, len(tables))
	for _, tbl := range tables {
		shape, err := loadShape(ctx, source, tbl)
		if err != nil {
			source.Close()
			dest.Close()
			return nil, nil, nil, err
		}
		shapes = append(shapes, shape)
	}
	return source, dest, shapes, nil
}

func compareAll(ctx context.Context, source, dest *pgxpool.Pool, shapes []tableShape) (Result, error) {
	res := Result{Match: true, CheckedAt: time.Now().UTC()}
	for _, shape := range shapes {
		srcCS, err := checksum(ctx, source, shape)
		if err != nil {
			return Result{}, err
		}
		tr := TableResult{Table: shape.Table, SourceRows: srcCS.Count}
		destCS, err := checksum(ctx, dest, shape)
		switch {
		case err != nil:
			// The dest table may not exist yet (no events seen): a mismatch,
			// not a fatal error, so the retry loop can wait it out.
			tr.Detail = fmt.Sprintf("dest not readable (%v)", err)
		case srcCS != destCS:
			tr.DestRows = destCS.Count
			tr.Detail = fmt.Sprintf("source{n=%d %s} dest{n=%d %s}",
				srcCS.Count, short(srcCS.Digest), destCS.Count, short(destCS.Digest))
		default:
			tr.DestRows = destCS.Count
			tr.ChecksumMatch = true
		}
		if !tr.ChecksumMatch {
			res.Match = false
		}
		res.Tables = append(res.Tables, tr)
	}
	return res, nil
}

func listTables(ctx context.Context, source *pgxpool.Pool, publication string) ([]string, error) {
	rows, err := source.Query(ctx,
		`SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1`, publication)
	if err != nil {
		return nil, fmt.Errorf("list publication tables: %w", err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return tables, rows.Err()
}

func short(digest string) string {
	if len(digest) > 8 {
		return digest[:8]
	}
	return digest
}
