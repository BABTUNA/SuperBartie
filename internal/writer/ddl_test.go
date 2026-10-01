package writer

import (
	"reflect"
	"testing"

	"github.com/BABTUNA/superbartie/internal/events"
)

func TestOrderedColumnsIsStable(t *testing.T) {
	evt := events.ChangeEvent{
		PK:    map[string]any{"id": 1},
		After: map[string]any{"zeta": 1, "id": 1, "alpha": 2, "mid": 3},
	}
	want := []string{"id", "alpha", "mid", "zeta"}
	for range 20 { // map iteration is randomized; order must not be
		if got := orderedColumns(evt); !reflect.DeepEqual(got, want) {
			t.Fatalf("orderedColumns = %v, want %v", got, want)
		}
	}
}

func TestQuoting(t *testing.T) {
	if got := quoteTable("public.animals"); got != `"public"."animals"` {
		t.Errorf("quoteTable = %s", got)
	}
	if got := quoteIdent(`we"ird`); got != `"we""ird"` {
		t.Errorf("quoteIdent = %s", got)
	}
}
