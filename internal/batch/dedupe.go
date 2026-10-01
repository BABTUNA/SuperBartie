// Package batch holds what every Kafka consumer in Bartie shares: a buffer
// that groups events by table and remembers offsets to commit, and the
// per-primary-key collapse that turns a batch of changes into final states.
// The writer and the vector writer both build on it.
package batch

import (
	"encoding/json"
	"fmt"

	"github.com/BABTUNA/superbartie/internal/events"
)

// Dedupe collapses a batch to one event per PK: destinations care about final
// states, not history. Safe because a row's events share a Kafka partition, so
// "later in the slice" really is later.
//
// Collapsing must FOLD, not just keep the last event: if an earlier event in
// the batch carried a TOASTed value and the last event has it marked
// unchanged, the value would otherwise be lost (the earlier event never
// reaches the destination on its own).
func Dedupe(evts []events.ChangeEvent) []events.ChangeEvent {
	state := map[string]events.ChangeEvent{}
	var order []string
	for _, evt := range evts {
		key := PKKey(evt)
		prev, seen := state[key]
		if !seen {
			order = append(order, key)
		}
		state[key] = fold(prev, seen, evt)
	}
	out := make([]events.ChangeEvent, 0, len(order))
	for _, key := range order {
		out = append(out, state[key])
	}
	return out
}

func fold(prev events.ChangeEvent, seen bool, next events.ChangeEvent) events.ChangeEvent {
	if !seen || next.Op == events.OpDelete || prev.Op == events.OpDelete || prev.After == nil {
		return next
	}
	if len(next.Unchanged) == 0 {
		return next
	}
	// Fill next's unchanged columns from what the batch already knows.
	after := make(map[string]any, len(next.After)+len(next.Unchanged))
	for k, v := range next.After {
		after[k] = v
	}
	var stillUnknown []string
	for _, col := range next.Unchanged {
		if v, ok := prev.After[col]; ok {
			after[col] = v
		} else {
			stillUnknown = append(stillUnknown, col)
		}
	}
	next.After = after
	next.Unchanged = stillUnknown
	return next
}

// PKKey is a canonical string for an event's primary key: encoding/json sorts
// map keys, so equal PK maps always produce equal strings.
func PKKey(evt events.ChangeEvent) string {
	b, err := json.Marshal(evt.PK)
	if err != nil {
		// PK maps are built from decoded scalars; this cannot fail in practice.
		panic(fmt.Sprintf("marshal pk for %s: %v", evt.Table, err))
	}
	return string(b)
}

// Representative picks the event with the fullest Types map, used for DDL and
// staging shape. Delete events carry full Types too, so any event works; this
// just guards against mixed batches.
func Representative(evts []events.ChangeEvent) events.ChangeEvent {
	rep := evts[0]
	for _, e := range evts[1:] {
		if len(e.Types) > len(rep.Types) {
			rep = e
		}
	}
	return rep
}
