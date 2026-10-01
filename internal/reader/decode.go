package reader

import (
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/BABTUNA/superbartie/internal/events"
	"github.com/BABTUNA/superbartie/internal/pgval"
)

// buildChangeEvent turns a pgoutput tuple into our envelope, for inserts
// (op "c") and updates (op "u"). LSN and CommitTS are stamped later, by the
// Commit message of the enclosing transaction; pgoutput only reveals them at
// commit time.
//
// The TOAST trap lives here: an update that did not touch a TOASTed column
// receives it as an "unchanged" marker, not data. Treating that as NULL would
// silently erase the value downstream, so it goes into evt.Unchanged and is
// left out of After; the writer's merge preserves the destination's value.
func buildChangeEvent(op events.Op, rel Relation, tuple *pglogrepl.TupleData) (events.ChangeEvent, error) {
	if len(tuple.Columns) != len(rel.Columns) {
		return events.ChangeEvent{}, fmt.Errorf("%s: tuple has %d columns, relation has %d", rel.Table, len(tuple.Columns), len(rel.Columns))
	}

	evt := events.ChangeEvent{
		Op:    op,
		Table: rel.Table,
		PK:    map[string]any{},
		After: map[string]any{},
		Types: map[string]string{},
	}

	for i, col := range rel.Columns {
		evt.Types[col.Name] = col.TypeName

		tc := tuple.Columns[i]
		if tc.DataType == pglogrepl.TupleDataTypeToast {
			if op == events.OpCreate {
				return events.ChangeEvent{}, fmt.Errorf("%s.%s: unchanged TOAST marker on an insert", rel.Table, col.Name)
			}
			evt.Unchanged = append(evt.Unchanged, col.Name)
			continue
		}

		val, err := decodeTupleColumn(tc, col)
		if err != nil {
			return events.ChangeEvent{}, fmt.Errorf("%s.%s: %w", rel.Table, col.Name, err)
		}
		evt.After[col.Name] = val
		if col.IsKey {
			evt.PK[col.Name] = val
		}
	}

	if len(evt.PK) == 0 {
		return events.ChangeEvent{}, fmt.Errorf("%s: no primary key columns in relation; refusing keyless rows", rel.Table)
	}
	return evt, nil
}

// buildDeleteEvent turns a pgoutput delete into op "d". With default replica
// identity the old tuple carries only the key columns; that is all a delete
// needs. After stays nil.
func buildDeleteEvent(rel Relation, oldTuple *pglogrepl.TupleData) (events.ChangeEvent, error) {
	if oldTuple == nil {
		return events.ChangeEvent{}, fmt.Errorf("%s: delete without old tuple (replica identity NOTHING?)", rel.Table)
	}
	if len(oldTuple.Columns) != len(rel.Columns) {
		return events.ChangeEvent{}, fmt.Errorf("%s: old tuple has %d columns, relation has %d", rel.Table, len(oldTuple.Columns), len(rel.Columns))
	}

	evt := events.ChangeEvent{
		Op:    events.OpDelete,
		Table: rel.Table,
		PK:    map[string]any{},
		Types: map[string]string{},
	}
	for i, col := range rel.Columns {
		evt.Types[col.Name] = col.TypeName
		if !col.IsKey {
			continue
		}
		val, err := decodeTupleColumn(oldTuple.Columns[i], col)
		if err != nil {
			return events.ChangeEvent{}, fmt.Errorf("%s.%s: %w", rel.Table, col.Name, err)
		}
		evt.PK[col.Name] = val
	}
	if len(evt.PK) == 0 {
		return events.ChangeEvent{}, fmt.Errorf("%s: delete with no key columns", rel.Table)
	}
	return evt, nil
}

// oldKeyIfChanged detects a PK-changing UPDATE. Postgres only ships the old
// tuple when replica identity columns changed; if its key differs from the new
// event's, the update must become delete(old) + insert(new) downstream or the
// old row would silently linger.
func oldKeyIfChanged(rel Relation, oldTuple *pglogrepl.TupleData, newEvt events.ChangeEvent) (map[string]any, bool, error) {
	if oldTuple == nil {
		return nil, false, nil
	}
	if len(oldTuple.Columns) != len(rel.Columns) {
		return nil, false, fmt.Errorf("%s: old tuple has %d columns, relation has %d", rel.Table, len(oldTuple.Columns), len(rel.Columns))
	}
	oldPK := map[string]any{}
	for i, col := range rel.Columns {
		if !col.IsKey {
			continue
		}
		val, err := decodeTupleColumn(oldTuple.Columns[i], col)
		if err != nil {
			return nil, false, fmt.Errorf("%s.%s: %w", rel.Table, col.Name, err)
		}
		oldPK[col.Name] = val
	}
	for k, v := range oldPK {
		if newEvt.PK[k] != v {
			return oldPK, true, nil
		}
	}
	return nil, false, nil
}

func decodeTupleColumn(tc *pglogrepl.TupleDataColumn, col Column) (any, error) {
	switch tc.DataType {
	case pglogrepl.TupleDataTypeNull:
		return nil, nil
	case pglogrepl.TupleDataTypeText:
		return pgval.TextToValue(string(tc.Data), col.TypeOID)
	default:
		return nil, fmt.Errorf("unsupported tuple data type %q", tc.DataType)
	}
}
