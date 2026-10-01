package reader

import (
	"testing"

	"github.com/jackc/pglogrepl"

	"github.com/BABTUNA/superbartie/internal/events"
)

func testRelation() Relation {
	return Relation{
		ID:    1,
		Table: "public.animals",
		Columns: []Column{
			{Name: "animal_id", TypeOID: 23, TypeName: "integer", IsKey: true},
			{Name: "name", TypeOID: 25, TypeName: "text"},
			{Name: "alive", TypeOID: 16, TypeName: "boolean"},
			{Name: "notes", TypeOID: 25, TypeName: "text"},
		},
	}
}

func textCol(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}

func TestBuildChangeEvent(t *testing.T) {
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textCol("42"),
		textCol("Testo"),
		textCol("t"),
		{DataType: pglogrepl.TupleDataTypeNull},
	}}

	evt, err := buildChangeEvent(events.OpCreate, testRelation(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	if got := evt.PK["animal_id"]; got != int64(42) {
		t.Errorf("pk animal_id = %v (%T), want int64 42", got, got)
	}
	if got := evt.After["alive"]; got != true {
		t.Errorf("alive = %v, want true", got)
	}
	if got := evt.After["notes"]; got != nil {
		t.Errorf("notes = %v, want nil", got)
	}
	if evt.Types["animal_id"] != "integer" {
		t.Errorf("types[animal_id] = %q", evt.Types["animal_id"])
	}
}

func TestBuildChangeEventColumnCountMismatch(t *testing.T) {
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{textCol("42")}}
	if _, err := buildChangeEvent(events.OpCreate, testRelation(), tuple); err == nil {
		t.Fatal("expected error for column count mismatch")
	}
}

func TestBuildChangeEventRefusesKeylessRelation(t *testing.T) {
	rel := Relation{ID: 2, Table: "public.nopk", Columns: []Column{{Name: "x", TypeOID: 25, TypeName: "text"}}}
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{textCol("v")}}
	if _, err := buildChangeEvent(events.OpCreate, rel, tuple); err == nil {
		t.Fatal("expected error for relation without key columns")
	}
}

// The TOAST trap: an untouched TOASTed column arrives as a marker, not data.
// It must land in Unchanged and stay out of After, never become NULL.
func TestUpdateWithUnchangedToast(t *testing.T) {
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textCol("42"),
		textCol("Renamed"),
		textCol("t"),
		{DataType: pglogrepl.TupleDataTypeToast},
	}}
	evt, err := buildChangeEvent(events.OpUpdate, testRelation(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	if len(evt.Unchanged) != 1 || evt.Unchanged[0] != "notes" {
		t.Errorf("unchanged = %v, want [notes]", evt.Unchanged)
	}
	if _, present := evt.After["notes"]; present {
		t.Errorf("notes must be absent from after, got %v", evt.After["notes"])
	}
	if evt.After["name"] != "Renamed" {
		t.Errorf("name = %v", evt.After["name"])
	}
}

func TestToastMarkerOnInsertIsError(t *testing.T) {
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textCol("42"), textCol("x"), textCol("t"), {DataType: pglogrepl.TupleDataTypeToast},
	}}
	if _, err := buildChangeEvent(events.OpCreate, testRelation(), tuple); err == nil {
		t.Fatal("expected error for TOAST marker on insert")
	}
}

func TestBuildDeleteEvent(t *testing.T) {
	// Default replica identity: old tuple carries key columns, rest null.
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textCol("42"),
		{DataType: pglogrepl.TupleDataTypeNull},
		{DataType: pglogrepl.TupleDataTypeNull},
		{DataType: pglogrepl.TupleDataTypeNull},
	}}
	evt, err := buildDeleteEvent(testRelation(), tuple)
	if err != nil {
		t.Fatal(err)
	}
	if evt.Op != events.OpDelete || evt.PK["animal_id"] != int64(42) || evt.After != nil {
		t.Fatalf("bad delete event: %+v", evt)
	}
}

func TestOldKeyIfChanged(t *testing.T) {
	newEvt := events.ChangeEvent{PK: map[string]any{"animal_id": int64(43)}}
	oldTuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textCol("42"),
		{DataType: pglogrepl.TupleDataTypeNull},
		{DataType: pglogrepl.TupleDataTypeNull},
		{DataType: pglogrepl.TupleDataTypeNull},
	}}
	oldPK, changed, err := oldKeyIfChanged(testRelation(), oldTuple, newEvt)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || oldPK["animal_id"] != int64(42) {
		t.Fatalf("changed=%v oldPK=%v, want changed with old key 42", changed, oldPK)
	}

	// Same key: not a PK change. No old tuple: not a PK change either.
	same := events.ChangeEvent{PK: map[string]any{"animal_id": int64(42)}}
	if _, changed, _ := oldKeyIfChanged(testRelation(), oldTuple, same); changed {
		t.Fatal("same key reported as changed")
	}
	if _, changed, _ := oldKeyIfChanged(testRelation(), nil, newEvt); changed {
		t.Fatal("nil old tuple reported as changed")
	}
}

func TestRelCacheMissFailsLoudly(t *testing.T) {
	if _, err := NewRelCache().Get(999); err == nil {
		t.Fatal("expected error for unknown relation ID")
	}
}
