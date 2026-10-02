package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// the demo surface writes to a public database so it is fenced hard
// only the observations table can be written
// only ids in the demo range or rows the traffic writer made
// no free text, a visitor picks a place from a fixed list and the server writes the sentence
// a per ip rate limit sits on top of all of it

const (
	// ids reserved for the page's own demo rows
	// these can be created, updated and deleted
	demoPKMin = 9000
	demoPKMax = 9099

	// the traffic writer's ids start here
	// these can only be updated
	trafficPKMin = 3_000_000_000

	// where a demo row starts and what the reset button returns it to
	defaultPlace = "the south bank"

	demoObsTable = "public.observations"

	// matches the last clause of a ranger log, like ", the reed bed."
	placeSuffix = `,[^,]*\.\s*$`
)

// the only values a visitor can write
// same list scripts/demo-traffic.sh picks from
var demoPlaces = []string{
	"the north ridge",
	"the south bank",
	"the reed bed",
	"the shallows",
	"the acacia line",
	"the dry channel",
	"the salt lick",
	"the far shore",
}

type pokeRequest struct {
	// c create or reset a demo row, u update, d delete
	Op string `json:"op"`

	// optional, must be public.observations when present
	Table string `json:"table"`

	// which row, like {"observation_id": 9001}
	PK map[string]any `json:"pk"`

	// one of demoPlaces
	// defaults to defaultPlace for c, required for u
	Place string `json:"place"`

	// free text is not accepted
	// the field exists only so an old client gets a clear error
	Set map[string]any `json:"set"`
}

type pokeResponse struct {
	Op    string `json:"op"`
	Table string `json:"table"`
	PK    int64  `json:"pk"`
	Place string `json:"place,omitempty"`

	// source clock just before commit
	// the page compares it to when the row lands in the destination
	CommitTs time.Time `json:"commitTs"`
}

// handlePoke changes one observation on the source
// the reader and writer then carry it to the destination like any other change
func (s *Server) handlePoke(w http.ResponseWriter, r *http.Request) {
	var req pokeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad json")
		return
	}
	if req.Table != "" && req.Table != demoObsTable {
		writeError(w, http.StatusBadRequest, "only public.observations can be changed from the demo")
		return
	}
	if len(req.Set) > 0 {
		writeError(w, http.StatusBadRequest, "free text is not accepted, send place instead")
		return
	}
	id, err := pokeID(req.PK)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	scope := pokeScope(id)
	if scope == "" {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("observation %d is read only, pick a demo row (%d-%d) or a sighting the traffic writer made", id, demoPKMin, demoPKMax))
		return
	}
	if scope == "traffic" && req.Op != "u" {
		writeError(w, http.StatusBadRequest, "traffic sightings can only be updated")
		return
	}
	if req.Place == "" && req.Op == "c" {
		req.Place = defaultPlace
	}
	if req.Op != "d" && !validPlace(req.Place) {
		writeError(w, http.StatusBadRequest, "place must be one of: "+strings.Join(demoPlaces, ", "))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	tx, err := s.source.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	defer tx.Rollback(ctx)

	switch req.Op {
	case "c":
		err = pokeCreate(ctx, tx, id, req.Place)
	case "u":
		err = pokeMove(ctx, tx, id, req.Place)
	case "d":
		_, err = tx.Exec(ctx, `DELETE FROM public.observations WHERE observation_id = $1`, id)
	default:
		writeError(w, http.StatusBadRequest, `op must be "c", "u", or "d"`)
		return
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var commitTs time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&commitTs); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if err := tx.Commit(ctx); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	out := pokeResponse{Op: req.Op, Table: demoObsTable, PK: id, CommitTs: commitTs.UTC()}
	if req.Op != "d" {
		out.Place = req.Place
	}
	writeJSON(w, http.StatusOK, out)
}

// pokeID reads the observation id out of the pk object
func pokeID(pk map[string]any) (int64, error) {
	raw, ok := pk["observation_id"]
	if !ok || len(pk) != 1 {
		return 0, fmt.Errorf(`pk must be {"observation_id": <int>}`)
	}
	switch v := raw.(type) {
	case float64:
		if v != float64(int64(v)) {
			return 0, fmt.Errorf("observation_id must be an integer")
		}
		return int64(v), nil
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("observation_id must be an integer")
		}
		return n, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("observation_id must be an integer")
		}
		return n, nil
	default:
		return 0, fmt.Errorf("observation_id must be an integer")
	}
}

// pokeScope says what a visitor may do to this id
// demo means full control, traffic means update only, empty means read only
func pokeScope(id int64) string {
	switch {
	case id >= demoPKMin && id <= demoPKMax:
		return "demo"
	case id >= trafficPKMin:
		return "traffic"
	default:
		return ""
	}
}

func validPlace(p string) bool {
	for _, ok := range demoPlaces {
		if p == ok {
			return true
		}
	}
	return false
}

// the sentence every demo row is written with
// built in sql from the real animal and watering hole names so retrieval finds it
const rangerLog = `format('Ranger log: %s the %s seen resting alone at %s, %s.', a.name, replace(a.species::text, '_', ' '), w.name, $2::text)`

// pokeCreate writes a demo row in its default shape
// if the row already exists it is put back to that shape, which is what reset needs
func pokeCreate(ctx context.Context, tx pgx.Tx, id int64, place string) error {
	_, err := tx.Exec(ctx, `
INSERT INTO public.observations (observation_id, animal_id, watering_hole_id, observed_at, notes)
SELECT $1, a.animal_id, w.watering_hole_id, now(), `+rangerLog+`
FROM (SELECT animal_id, name, species FROM animals ORDER BY animal_id LIMIT 1) a,
     (SELECT watering_hole_id, name FROM watering_holes ORDER BY watering_hole_id LIMIT 1) w
ON CONFLICT (observation_id) DO UPDATE
SET notes = EXCLUDED.notes, animal_id = EXCLUDED.animal_id, watering_hole_id = EXCLUDED.watering_hole_id`, id, place)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	return nil
}

// pokeMove swaps the place at the end of a sighting's notes
// a row whose notes are not a ranger log gets rewritten as one
func pokeMove(ctx context.Context, tx pgx.Tx, id int64, place string) error {
	tag, err := tx.Exec(ctx, `
UPDATE public.observations o
SET notes = CASE
  WHEN o.notes ~ '`+placeSuffix+`' THEN regexp_replace(o.notes, '`+placeSuffix+`', ', ' || $2::text || '.')
  ELSE (SELECT `+rangerLog+`
        FROM animals a, watering_holes w
        WHERE a.animal_id = o.animal_id AND w.watering_hole_id = o.watering_hole_id)
END
WHERE o.observation_id = $1`, id, place)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no observation %d on the source, it may have been retracted", id)
	}
	return nil
}

// readTables is the read-only allowlist for the table views: every replicated
// table plus the vector destination, with the column that says "recently
// changed" and, where the full row would be too big to ship, the columns to
// select (the vector table's embedding is 1536 floats per row).
var readTables = map[string]struct {
	pk, recency, columns string
	destOnly             bool
}{
	"public.animals":        {pk: "animal_id", recency: "updated_at", columns: "*"},
	"public.observations":   {pk: "observation_id", recency: "observed_at", columns: "*"},
	"public.watering_holes": {pk: "watering_hole_id", recency: "created_at", columns: "*"},
	"public.bartie_vectors": {pk: "pk", recency: "__bartie_updated_at", destOnly: true,
		columns: `"table", pk, text, __bartie_commit_ts, __bartie_updated_at`},
}

// handleTable returns one table from both databases: a page of rows ordered
// by recency (default) or by primary key, plus total counts per side. The
// live page polls the recent view; the data browser pages through the whole
// thing. Destination rows carry the pipeline's metadata columns.
func (s *Server) handleTable(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	spec, ok := readTables[table]
	if !ok {
		writeError(w, http.StatusBadRequest, "table not in demo allowlist")
		return
	}
	limit, offset := 10, 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	order := fmt.Sprintf("%s DESC, %s DESC", quoteIdent(spec.recency), quoteIdent(spec.pk))
	if r.URL.Query().Get("order") == "pk" {
		order = quoteIdent(spec.pk) + " ASC"
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	q := fmt.Sprintf(`SELECT row_to_json(t) FROM (SELECT %s FROM %s ORDER BY %s LIMIT $1 OFFSET $2) t`,
		spec.columns, quoteTable(table), order)
	type pool interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
		QueryRow(context.Context, string, ...any) pgx.Row
	}
	read := func(p pool) ([]json.RawMessage, *int64) {
		out := []json.RawMessage{}
		var count *int64
		var n int64
		if err := p.QueryRow(ctx, `SELECT count(*) FROM `+quoteTable(table)).Scan(&n); err == nil {
			count = &n
		} else {
			return out, nil // table does not exist on this side
		}
		rows, err := p.Query(ctx, q, limit, offset)
		if err != nil {
			return out, count
		}
		defer rows.Close()
		for rows.Next() {
			var raw json.RawMessage
			if rows.Scan(&raw) == nil {
				out = append(out, raw)
			}
		}
		return out, count
	}
	srcRows, srcCount := []json.RawMessage{}, (*int64)(nil)
	if !spec.destOnly {
		srcRows, srcCount = read(s.source)
	}
	dstRows, dstCount := read(s.dest)
	writeJSON(w, http.StatusOK, map[string]any{
		"table":    table,
		"pk":       spec.pk,
		"recency":  spec.recency,
		"destOnly": spec.destOnly,
		"limit":    limit,
		"offset":   offset,
		"counts":   map[string]any{"source": srcCount, "dest": dstCount},
		"source":   srcRows,
		"dest":     dstRows,
	})
}

// handleRow shows the same row on both sides, so the page can render "source
// says X, destination says X, applied at T".
func (s *Server) handleRow(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	spec, ok := readTables[table]
	if !ok || spec.destOnly {
		writeError(w, http.StatusBadRequest, "table not in demo allowlist")
		return
	}
	pk, err := strconv.ParseInt(r.PathValue("pk"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "pk must be an integer")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	q := fmt.Sprintf(`SELECT row_to_json(t) FROM %s t WHERE %s = $1`, quoteTable(table), quoteIdent(spec.pk))
	out := map[string]any{"table": table, "pk": pk}
	var src, dst json.RawMessage
	if err := s.source.QueryRow(ctx, q, pk).Scan(&src); err == nil {
		out["source"] = src
	} else {
		out["source"] = nil
	}
	if err := s.dest.QueryRow(ctx, q, pk).Scan(&dst); err == nil {
		out["dest"] = dst
	} else {
		out["dest"] = nil
	}
	writeJSON(w, http.StatusOK, out)
}

// --- rate limit ------------------------------------------------------------

type demoLimiter struct {
	perMinute int
	mu        sync.Mutex
	hits      map[string][]time.Time
}

func newDemoLimiter(perMinute int) *demoLimiter {
	return &demoLimiter{perMinute: perMinute, hits: map[string][]time.Time{}}
}

func (l *demoLimiter) limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if l.perMinute > 0 && !l.allow(clientIP(r)) {
			w.Header().Set("Retry-After", "60")
			writeError(w, http.StatusTooManyRequests, "slow down: demo endpoints are rate limited")
			return
		}
		next(w, r)
	}
}

func (l *demoLimiter) allow(ip string) bool {
	now := time.Now()
	cutoff := now.Add(-time.Minute)
	l.mu.Lock()
	defer l.mu.Unlock()
	recent := l.hits[ip][:0]
	for _, t := range l.hits[ip] {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	if len(recent) >= l.perMinute {
		l.hits[ip] = recent
		return false
	}
	l.hits[ip] = append(recent, now)
	if len(l.hits) > 10000 { // crude memory bound; a real limiter would sweep
		for k, v := range l.hits {
			if len(v) == 0 || !v[len(v)-1].After(cutoff) {
				delete(l.hits, k)
			}
		}
	}
	return true
}

// clientIP trusts X-Forwarded-For only because Caddy sits in front in
// deployment and sets it; locally it is absent and RemoteAddr is used.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// --- traffic switch --------------------------------------------------------

// The demo traffic writer is a separate process (scripts/demo-traffic.sh)
// that shares a data directory with the api. A marker file in that directory
// pauses it; the script checks for the file before every write. No sockets,
// no signals, survives restarts of either side.
func (s *Server) trafficPauseFile() string {
	return filepath.Join(filepath.Dir(s.cfg.ErrorLogPath), "traffic.paused")
}

func (s *Server) handleTrafficGet(w http.ResponseWriter, _ *http.Request) {
	_, err := os.Stat(s.trafficPauseFile())
	writeJSON(w, http.StatusOK, map[string]any{"enabled": errors.Is(err, os.ErrNotExist)})
}

func (s *Server) handleTrafficSet(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Enabled == nil {
		writeError(w, http.StatusBadRequest, `body must be {"enabled": true|false}`)
		return
	}
	path := s.trafficPauseFile()
	var err error
	if *body.Enabled {
		err = os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		}
	} else {
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		err = os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "enabled": *body.Enabled})
}

// --- timed pause -----------------------------------------------------------

// how long a visitor's pause lasts
// long enough to watch the backlog build, short enough that nobody is left waiting
const demoPauseSeconds = 15

// handleDemoPause stops the writer for a few seconds so the lag numbers visibly move
// the writer resumes itself, so a visitor who clicks and leaves cannot break the demo
// open to anyone, unlike the untimed pause on /pipelines/{uuid}/status
func (s *Server) handleDemoPause(w http.ResponseWriter, r *http.Request) {
	body := fmt.Sprintf(`{"status":"paused","seconds":%d}`, demoPauseSeconds)
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, s.cfg.WriterMetricsURL+"/control", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		writeError(w, http.StatusBadGateway, "writer unreachable: "+err.Error())
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("writer returned %d", resp.StatusCode))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"success":     true,
		"seconds":     demoPauseSeconds,
		"pausedUntil": time.Now().UTC().Add(demoPauseSeconds * time.Second),
	})
}

// --- holding traffic still for verify --------------------------------------

// with a write every two seconds and a two second flush, a row is almost always in flight
// so a checksum of a busy pipeline rarely matches even when nothing is wrong
// verify asks the traffic writer to wait while it checks, then lets it go
//
// this is a second marker file, separate from the pause switch
// the page keeps showing traffic as on, because from a visitor's view it is
func (s *Server) trafficHoldFile() string {
	return filepath.Join(filepath.Dir(s.cfg.ErrorLogPath), "traffic.hold")
}

// holdTraffic stops the traffic writer until the returned func is called
// calls nest, the writer resumes when the last holder lets go
// the script ignores a hold older than a minute, so a crash here cannot stall it for good
func (s *Server) holdTraffic() (release func()) {
	path := s.trafficHoldFile()
	s.holdMu.Lock()
	s.holds++
	if s.holds == 1 {
		_ = os.MkdirAll(filepath.Dir(path), 0o755)
		_ = os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
	}
	s.holdMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.holdMu.Lock()
			s.holds--
			if s.holds == 0 {
				_ = os.Remove(path)
			}
			s.holdMu.Unlock()
		})
	}
}
