package vecwriter

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/bartie/internal/events"
)

// A document is what gets embedded: one row rendered as a sentence, with a
// header that names the entity so retrieval on "lion 42" or "observation
// 5512" lands. Only tables listed here are embedded; the rest of the stream is
// ignored by this consumer.
type document struct {
	table string
	pk    string // canonical JSON of the PK map
	text  string
}

// embeddedColumns are the columns whose change should trigger a re-embed.
// An update that leaves all of them untouched (a TOAST-unchanged notes column,
// or a metadata-only change) is skipped: the vector would come out identical.
var embeddedColumns = map[string][]string{
	"public.animals":      {"name", "species", "status", "home_watering_hole_id"},
	"public.observations": {"animal_id", "watering_hole_id", "observed_at", "notes"},
}

// lookups caches destination reads for one flush: an observation's document
// wants the animal's name and the watering hole's name, both of which the
// plain writer keeps in the destination. Ordering across the two consumers is
// not guaranteed, so a miss falls back to the id.
type lookups struct {
	pool    *pgxpool.Pool
	animals map[string]string // animal_id -> "Mosi (blue wildebeest)"
	holes   map[string]string // watering_hole_id -> "Reed Lagoon"
}

func newLookups(pool *pgxpool.Pool) *lookups {
	return &lookups{pool: pool, animals: map[string]string{}, holes: map[string]string{}}
}

func (l *lookups) animal(ctx context.Context, id string) string {
	if v, ok := l.animals[id]; ok {
		return v
	}
	var name, species string
	err := l.pool.QueryRow(ctx, `SELECT name, species::text FROM public.animals WHERE animal_id = $1::int`, id).Scan(&name, &species)
	v := "animal " + id
	if err == nil {
		v = fmt.Sprintf("%s (%s, animal %s)", name, strings.ReplaceAll(species, "_", " "), id)
	}
	l.animals[id] = v
	return v
}

func (l *lookups) hole(ctx context.Context, id string) string {
	if v, ok := l.holes[id]; ok {
		return v
	}
	var name string
	err := l.pool.QueryRow(ctx, `SELECT name FROM public.watering_holes WHERE watering_hole_id = $1::int`, id).Scan(&name)
	v := "watering hole " + id
	if err == nil {
		v = fmt.Sprintf("%s (watering hole %s)", name, id)
	}
	l.holes[id] = v
	return v
}

// render returns the document for an upsert event, or ok=false when the event
// should not touch the vector table (table not embedded, or nothing embedded
// changed). Deletes are handled by the caller and never reach here.
func render(ctx context.Context, lk *lookups, evt events.ChangeEvent) (document, bool) {
	cols, embedded := embeddedColumns[evt.Table]
	if !embedded {
		return document{}, false
	}
	if skipUnchanged(evt, cols) {
		return document{}, false
	}
	get := func(col string) string { return str(evt.After[col]) }

	var text string
	switch evt.Table {
	case "public.animals":
		text = fmt.Sprintf("%s is a %s, status %s, animal id %s. Home: %s.",
			get("name"), strings.ReplaceAll(get("species"), "_", " "), get("status"), get("animal_id"),
			lk.hole(ctx, get("home_watering_hole_id")))
	case "public.observations":
		notes := strings.TrimSpace(get("notes"))
		if notes == "" {
			notes = "no notes"
		}
		text = fmt.Sprintf("Observation %s of %s at %s on %s: %s",
			get("observation_id"), lk.animal(ctx, get("animal_id")), lk.hole(ctx, get("watering_hole_id")),
			get("observed_at"), notes)
	}
	return document{table: evt.Table, text: text}, true
}

// skipUnchanged is true when every embedded column is either absent from
// After because it was TOAST-unchanged, or the event is an update that could
// not have changed any embedded column. The first case is the one that
// matters: we cannot embed text we were not sent, and we do not need to.
func skipUnchanged(evt events.ChangeEvent, cols []string) bool {
	if evt.Op != events.OpUpdate || len(evt.Unchanged) == 0 {
		return false
	}
	unchanged := map[string]bool{}
	for _, c := range evt.Unchanged {
		unchanged[c] = true
	}
	for _, c := range cols {
		if unchanged[c] {
			// A text column we did not receive: the row's embedded content
			// did not change, so the existing vector is still correct.
			return true
		}
	}
	return false
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}
