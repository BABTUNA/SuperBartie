// Package backfill copies a source database's existing rows into the pipeline
// before live streaming begins, reading from the exported snapshot the
// replication slot was created with. Every row becomes an "r" ChangeEvent and
// travels the same Kafka path as a live change, so the writer needs no special
// case: an "r" event is just an upsert.
package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BABTUNA/superbartie/internal/events"
	"github.com/BABTUNA/superbartie/internal/pgval"
)

const chunkSize = 500

// Publisher matches the reader's sink.
type Publisher interface {
	Publish(ctx context.Context, evts []events.ChangeEvent) error
}

// Run copies every published table as of the given snapshot. It opens its own
// normal (non-replication) connection so the replication connection can stay
// idle, which is what keeps the exported snapshot valid.
func Run(ctx context.Context, sourceDSN, publication, snapshotName string, pub Publisher) error {
	conn, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("backfill connect: %w", err)
	}
	defer conn.Close(ctx)

	// One transaction pinned to the exported snapshot: every table is read as
	// of the same instant the slot will stream from.
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("backfill begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
		return fmt.Errorf("set transaction snapshot: %w", err)
	}

	tables, err := listTables(ctx, tx, publication)
	if err != nil {
		return err
	}

	startTS := time.Now().UTC()
	for _, table := range tables {
		n, err := copyTable(ctx, tx, table, startTS, pub)
		if err != nil {
			return fmt.Errorf("backfill %s: %w", table, err)
		}
		slog.Info("backfilled table", "table", table, "rows", n)
	}
	return nil
}

func copyTable(ctx context.Context, tx pgx.Tx, table string, startTS time.Time, pub Publisher) (int, error) {
	cols, err := loadColumns(ctx, tx, table)
	if err != nil {
		return 0, err
	}

	selects := make([]string, len(cols))
	for i, c := range cols {
		// Read every column as text and convert by OID, exactly as the stream
		// decoder does, so a row's PK is identical from both paths.
		selects[i] = fmt.Sprintf("%s::text", quoteIdent(c.name))
	}
	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selects, ", "), quoteTable(table))

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var batch []events.ChangeEvent
	total := 0
	for rows.Next() {
		raw, err := rows.Values()
		if err != nil {
			return total, err
		}
		evt, err := rowToEvent(table, cols, raw, startTS)
		if err != nil {
			return total, err
		}
		batch = append(batch, evt)
		if len(batch) >= chunkSize {
			if err := pub.Publish(ctx, batch); err != nil {
				return total, err
			}
			total += len(batch)
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		return total, err
	}
	if len(batch) > 0 {
		if err := pub.Publish(ctx, batch); err != nil {
			return total, err
		}
		total += len(batch)
	}
	return total, nil
}

func rowToEvent(table string, cols []column, raw []any, startTS time.Time) (events.ChangeEvent, error) {
	evt := events.ChangeEvent{
		Op:       events.OpRead,
		Table:    table,
		LSN:      0, // a snapshot row has no WAL position of its own
		CommitTS: startTS,
		PK:       map[string]any{},
		After:    map[string]any{},
		Types:    map[string]string{},
	}
	for i, col := range cols {
		evt.Types[col.name] = pgval.TypeNameForOID(col.typeOID)

		var val any
		if raw[i] != nil {
			s, ok := raw[i].(string)
			if !ok {
				return evt, fmt.Errorf("%s.%s: expected text, got %T", table, col.name, raw[i])
			}
			v, err := pgval.TextToValue(s, col.typeOID)
			if err != nil {
				return evt, fmt.Errorf("%s.%s: %w", table, col.name, err)
			}
			val = v
		}
		evt.After[col.name] = val
		if col.isPK {
			evt.PK[col.name] = val
		}
	}
	if len(evt.PK) == 0 {
		return evt, fmt.Errorf("%s: no primary key", table)
	}
	return evt, nil
}
