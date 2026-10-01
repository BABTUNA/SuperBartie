// package events defines the one message shape that flows from the reader
// to kafka to the writer
//
// every change looks like this on the wire
//
//	{
//	  "op": "u",
//	  "table": "public.animals",
//	  "lsn": 27001136,
//	  "commit_ts": "2026-09-10T09:00:00Z",
//	  "pk": {"animal_id": 1},
//	  "after": {"animal_id": 1, "name": "mosi", "status": "adult"},
//	  "types": {"animal_id": "integer", "name": "text", "status": "text"}
//	}
package events

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// op is what happened to the row
// one letter, same convention debezium uses
type Op string

const (
	// a row was inserted
	OpCreate Op = "c"

	// a row was updated
	OpUpdate Op = "u"

	// a row was deleted
	OpDelete Op = "d"

	// a row was read from the backfill snapshot
	// the writer applies it as an upsert, same as an insert or update
	OpRead Op = "r"
)

// change event is the envelope around one row change
// the reader builds it, kafka carries it, the writer applies it
// inserts, updates, deletes and backfill rows all use this same shape
// new fields can be added later but existing ones never change meaning
type ChangeEvent struct {
	// what happened to the row
	Op Op `json:"op"`

	// which table, always with its schema, like public.animals
	Table string `json:"table"`

	// wal position of the commit this change belongs to
	// backfill rows have no wal position so this is 0 for them
	LSN uint64 `json:"lsn"`

	// when the source transaction committed
	// for backfill rows this is when the backfill started
	CommitTS time.Time `json:"commit_ts"`

	// primary key columns and their values
	// this is what identifies the row
	PK map[string]any `json:"pk"`

	// the full row after the change
	// nil for deletes
	After map[string]any `json:"after"`

	// toast columns postgres did not resend because the update left them alone
	// they are missing from after
	// the writer must keep the value already in the destination and never write null
	Unchanged []string `json:"unchanged,omitempty"`

	// column name to source postgres type, like int8 or timestamptz
	// lets the writer create destination tables without ever touching the source
	Types map[string]string `json:"types,omitempty"`
}

// key is the kafka message key, table plus primary key
// every change to the same row gets the same key
// so it lands in the same partition and stays in order
func (e ChangeEvent) Key() ([]byte, error) {
	pk, err := json.Marshal(e.PK)
	if err != nil {
		return nil, fmt.Errorf("marshal pk: %w", err)
	}
	return []byte(e.Table + ":" + string(pk)), nil
}

// marshal turns the event into the json that goes on the wire
func (e ChangeEvent) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// decode reads an event back from json
// numbers are kept as json.Number instead of float64
// a float64 would silently round int8 values above 2^53
// an event with no op or no table is rejected
func Decode(b []byte) (ChangeEvent, error) {
	var e ChangeEvent
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&e); err != nil {
		return ChangeEvent{}, fmt.Errorf("unmarshal change event: %w", err)
	}
	if e.Op == "" || e.Table == "" {
		return ChangeEvent{}, fmt.Errorf("change event missing op or table: %s", b)
	}
	return e, nil
}
