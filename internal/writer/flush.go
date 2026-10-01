package writer

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/superbartie/internal/batch"
	"github.com/BABTUNA/superbartie/internal/events"
)

// flushTable applies one table's batch inside a single destination
// transaction: dedupe to final states, load them into a temp staging table,
// merge staging into the target. Idempotent by construction: replaying the
// same batch re-asserts the same final states.
func flushTable(ctx context.Context, pool *pgxpool.Pool, ddl *ddlManager, table string, evts []events.ChangeEvent) error {
	rep := batch.Representative(evts)
	if err := ddl.ensureTable(ctx, pool, rep); err != nil {
		return err
	}

	deduped := batch.Dedupe(evts)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: begin flush tx: %w", table, err)
	}
	defer tx.Rollback(ctx)

	cols := orderedColumns(rep)
	if err := createStaging(ctx, tx, rep, cols); err != nil {
		return err
	}
	if err := loadStaging(ctx, tx, rep, cols, deduped); err != nil {
		return err
	}
	if err := mergeStaging(ctx, tx, rep, cols); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%s: commit flush tx: %w", table, err)
	}
	return nil
}
