package batch

import (
	"testing"

	"github.com/BABTUNA/superbartie/internal/events"
)

func evt(op events.Op, id int, after map[string]any, unchanged ...string) events.ChangeEvent {
	return events.ChangeEvent{
		Op:        op,
		Table:     "public.t",
		PK:        map[string]any{"id": id},
		After:     after,
		Unchanged: unchanged,
		Types:     map[string]string{"id": "integer", "name": "text", "notes": "text"},
	}
}

func TestDedupeLastEventWins(t *testing.T) {
	out := Dedupe([]events.ChangeEvent{
		evt(events.OpCreate, 1, map[string]any{"id": 1, "name": "a", "notes": "x"}),
		evt(events.OpUpdate, 1, map[string]any{"id": 1, "name": "b", "notes": "x"}),
		evt(events.OpUpdate, 1, map[string]any{"id": 1, "name": "c", "notes": "x"}),
	})
	if len(out) != 1 {
		t.Fatalf("got %d events, want 1", len(out))
	}
	if out[0].After["name"] != "c" {
		t.Errorf("name = %v, want c (last event wins)", out[0].After["name"])
	}
}

func TestDedupeCreateThenDeleteCollapsesToDelete(t *testing.T) {
	out := Dedupe([]events.ChangeEvent{
		evt(events.OpCreate, 1, map[string]any{"id": 1, "name": "a"}),
		evt(events.OpDelete, 1, nil),
	})
	if len(out) != 1 || out[0].Op != events.OpDelete {
		t.Fatalf("got %+v, want single delete", out)
	}
}

// The TOAST fold: an earlier event in the batch carried the big value, the
// last event has it "unchanged". Keeping only the last event would lose the
// value, because the earlier event never reaches the destination.
func TestDedupeFoldsUnchangedToastFromEarlierEvent(t *testing.T) {
	out := Dedupe([]events.ChangeEvent{
		evt(events.OpUpdate, 1, map[string]any{"id": 1, "name": "a", "notes": "BIG TOASTED VALUE"}),
		evt(events.OpUpdate, 1, map[string]any{"id": 1, "name": "b"}, "notes"),
	})
	if len(out) != 1 {
		t.Fatalf("got %d events, want 1", len(out))
	}
	if out[0].After["notes"] != "BIG TOASTED VALUE" {
		t.Errorf("notes = %v, want the earlier event's value folded in", out[0].After["notes"])
	}
	if len(out[0].Unchanged) != 0 {
		t.Errorf("unchanged = %v, want empty after fold", out[0].Unchanged)
	}
	if out[0].After["name"] != "b" {
		t.Errorf("name = %v, want b", out[0].After["name"])
	}
}

// When nothing earlier in the batch knows the value, "unchanged" must survive
// to the merge, which preserves the destination's copy.
func TestDedupeKeepsUnchangedWhenValueUnknown(t *testing.T) {
	out := Dedupe([]events.ChangeEvent{
		evt(events.OpUpdate, 1, map[string]any{"id": 1, "name": "b"}, "notes"),
	})
	if len(out[0].Unchanged) != 1 || out[0].Unchanged[0] != "notes" {
		t.Errorf("unchanged = %v, want [notes]", out[0].Unchanged)
	}
	if _, present := out[0].After["notes"]; present {
		t.Errorf("notes should be absent from after, got %v", out[0].After["notes"])
	}
}

func TestDedupeDistinctRowsAllSurvive(t *testing.T) {
	out := Dedupe([]events.ChangeEvent{
		evt(events.OpCreate, 1, map[string]any{"id": 1, "name": "a"}),
		evt(events.OpCreate, 2, map[string]any{"id": 2, "name": "b"}),
		evt(events.OpDelete, 3, nil),
	})
	if len(out) != 3 {
		t.Fatalf("got %d events, want 3", len(out))
	}
}
