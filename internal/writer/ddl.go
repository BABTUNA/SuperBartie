package writer

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/superbartie/internal/events"
)

// Metadata columns every destination table carries, mirroring how Artie
// stamps its destinations. They are invisible to verify (which digests only
// source columns) and power latency measurement: how far behind the source a
// row landed.
const (
	metaCommitTS  = "__bartie_commit_ts"  // source transaction commit time
	metaUpdatedAt = "__bartie_updated_at" // when this writer applied the row
)

// ddlManager creates destination tables on first sight of a table's events,
// using the source type names the reader ships in every event. It never
// queries the source database.
type ddlManager struct {
	created map[string]bool
}

func newDDLManager() *ddlManager {
	return &ddlManager{created: map[string]bool{}}
}

func (d *ddlManager) ensureTable(ctx context.Context, pool *pgxpool.Pool, evt events.ChangeEvent) error {
	if d.created[evt.Table] {
		return nil
	}

	cols := orderedColumns(evt)
	defs := make([]string, 0, len(cols)+2)
	for _, col := range cols {
		defs = append(defs, fmt.Sprintf("%s %s", quoteIdent(col), destType(evt.Types[col])))
	}
	defs = append(defs,
		quoteIdent(metaCommitTS)+" timestamptz",
		quoteIdent(metaUpdatedAt)+" timestamptz",
	)

	pkCols := make([]string, 0, len(evt.PK))
	for col := range evt.PK {
		pkCols = append(pkCols, quoteIdent(col))
	}
	sort.Strings(pkCols)
	defs = append(defs, fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteTable(evt.Table), strings.Join(defs, ", "))
	if _, err := pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("create table %s: %w", evt.Table, err)
	}
	d.created[evt.Table] = true
	return nil
}

// orderedColumns returns the event's columns in a stable order: PK columns
// first, the rest alphabetical. Maps are unordered; DDL and inserts must not
// be. Types is the authoritative column set (After is nil on deletes and
// missing unchanged TOAST columns).
func orderedColumns(evt events.ChangeEvent) []string {
	source := evt.Types
	if len(source) == 0 {
		source = map[string]string{}
		for col := range evt.After {
			source[col] = ""
		}
	}
	var pks, rest []string
	for col := range source {
		if _, isPK := evt.PK[col]; isPK {
			pks = append(pks, col)
		} else {
			rest = append(rest, col)
		}
	}
	sort.Strings(pks)
	sort.Strings(rest)
	return append(pks, rest...)
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// quoteTable quotes a schema-qualified name ("public.animals").
func quoteTable(t string) string {
	parts := strings.SplitN(t, ".", 2)
	if len(parts) == 2 {
		return quoteIdent(parts[0]) + "." + quoteIdent(parts[1])
	}
	return quoteIdent(t)
}
