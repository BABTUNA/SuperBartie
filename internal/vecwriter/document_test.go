package vecwriter

import (
	"context"
	"testing"

	"github.com/BABTUNA/superbartie/internal/events"
)

func TestSkipUnchangedOnlyWhenEmbeddedColumnMissing(t *testing.T) {
	cols := embeddedColumns["public.observations"]
	// TOAST-unchanged notes: the text we would embed did not arrive and did
	// not change. Skip.
	evt := events.ChangeEvent{Op: events.OpUpdate, Table: "public.observations", Unchanged: []string{"notes"}}
	if !skipUnchanged(evt, cols) {
		t.Fatal("update with unchanged notes should be skipped")
	}
	// An update where notes did arrive re-embeds even if something else was
	// unchanged.
	evt = events.ChangeEvent{Op: events.OpUpdate, Table: "public.observations", Unchanged: []string{"some_blob"}}
	if skipUnchanged(evt, cols) {
		t.Fatal("update with fresh notes should embed")
	}
	// Inserts and snapshot reads always embed.
	for _, op := range []events.Op{events.OpCreate, events.OpRead} {
		evt = events.ChangeEvent{Op: op, Table: "public.observations", Unchanged: []string{"notes"}}
		if skipUnchanged(evt, cols) {
			t.Fatalf("%s should never be skipped", op)
		}
	}
}

func TestRenderIgnoresUnembeddedTables(t *testing.T) {
	lk := &lookups{animals: map[string]string{}, holes: map[string]string{}}
	_, ok := render(context.Background(), lk, events.ChangeEvent{Op: events.OpCreate, Table: "public.watering_holes", After: map[string]any{"name": "x"}})
	if ok {
		t.Fatal("watering_holes should not produce a document")
	}
}

func TestRenderAnimalUsesCachedLookups(t *testing.T) {
	// Pre-seeded cache means no pool is needed.
	lk := &lookups{animals: map[string]string{}, holes: map[string]string{"7": "Reed Lagoon (watering hole 7)"}}
	doc, ok := render(context.Background(), lk, events.ChangeEvent{
		Op: events.OpCreate, Table: "public.animals",
		After: map[string]any{"animal_id": 42, "name": "Mosi", "species": "blue_wildebeest", "status": "adult", "home_watering_hole_id": 7},
	})
	if !ok {
		t.Fatal("animal should render")
	}
	want := "Mosi is a blue wildebeest, status adult, animal id 42. Home: Reed Lagoon (watering hole 7)."
	if doc.text != want {
		t.Fatalf("got %q", doc.text)
	}
}
