package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

// The demo surface writes to a public database, so it is fenced three ways:
// an allowlist of tables and columns, a reserved primary-key range that the
// nightly reset wipes, and a per-IP rate limit. Never raw SQL from a client.

const (
	demoPKMin = 9000
	demoPKMax = 9099
)

type demoTable struct {
	pk       string
	cols     map[string]bool   // writable columns
	required []string          // must be present on insert unless defaulted
	defaults map[string]string // SQL expressions used on insert when absent
}

var demoTables = map[string]demoTable{
	"public.animals": {
		pk:       "animal_id",
		cols:     set("name", "species", "status", "home_watering_hole_id"),
		required: []string{"name"},
		defaults: map[string]string{
			"species":               "'lion'",
			"status":                "'adult'",
			"home_watering_hole_id": "(SELECT min(watering_hole_id) FROM watering_holes)",
		},
	},
	"public.observations": {
		pk:       "observation_id",
		cols:     set("animal_id", "watering_hole_id", "observed_at", "notes"),
		required: []string{"notes"},
		defaults: map[string]string{
			"animal_id":        "(SELECT min(animal_id) FROM animals)",
			"watering_hole_id": "(SELECT min(watering_hole_id) FROM watering_holes)",
			"observed_at":      "now()",
		},
	},
}

func set(keys ...string) map[string]bool {
	m := map[string]bool{}
	for _, k := range keys {
		m[k] = true
	}
	return m
}

type pokeRequest struct {
	Op    string         `json:"op"`    // c | u | d
	Table string         `json:"table"` // public.animals | public.observations
	PK    map[string]any `json:"pk"`    // {"animal_id": 9001}
	Set   map[string]any `json:"set"`   // columns to write (c, u)
}

type pokeResponse struct {
	Op       string    `json:"op"`
	Table    string    `json:"table"`
	PK       int64     `json:"pk"`
	CommitTs time.Time `json:"commitTs"` // source clock just before COMMIT
}

func (s *Server) handlePoke(w http.ResponseWriter, r *http.Request) {
	var req pokeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad json")
		return
	}
	spec, ok := demoTables[req.Table]
	if !ok {
		writeError(w, http.StatusBadRequest, "table not in demo allowlist")
		return
	}
	pk, err := demoPK(req.PK, spec.pk)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	for col := range req.Set {
		if !spec.cols[col] {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("column %q not writable in demo", col))
			return
		}
		if str, isStr := req.Set[col].(string); isStr && len(str) > 2000 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("column %q too long", col))
			return
		}
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
		err = demoInsert(ctx, tx, req.Table, spec, pk, req.Set)
	case "u":
		if len(req.Set) == 0 {
			writeError(w, http.StatusBadRequest, "update needs at least one column in set")
			return
		}
		err = demoUpdate(ctx, tx, req.Table, spec, pk, req.Set)
	case "d":
		_, err = tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE %s = $1`, quoteTable(req.Table), quoteIdent(spec.pk)), pk)
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
	writeJSON(w, http.StatusOK, pokeResponse{Op: req.Op, Table: req.Table, PK: pk, CommitTs: commitTs.UTC()})
}

func demoPK(pk map[string]any, col string) (int64, error) {
	raw, ok := pk[col]
	if !ok || len(pk) != 1 {
		return 0, fmt.Errorf("pk must be {%q: <int>}", col)
	}
	var n int64
	switch v := raw.(type) {
	case float64:
		n = int64(v)
	case json.Number:
		var err error
		if n, err = v.Int64(); err != nil {
			return 0, fmt.Errorf("pk must be an integer")
		}
	case string:
		var err error
		if n, err = strconv.ParseInt(v, 10, 64); err != nil {
			return 0, fmt.Errorf("pk must be an integer")
		}
	default:
		return 0, fmt.Errorf("pk must be an integer")
	}
	if n < demoPKMin || n > demoPKMax {
		return 0, fmt.Errorf("demo pk must be between %d and %d", demoPKMin, demoPKMax)
	}
	return n, nil
}

func demoInsert(ctx context.Context, tx pgx.Tx, table string, spec demoTable, pk int64, vals map[string]any) error {
	for _, req := range spec.required {
		if _, ok := vals[req]; !ok {
			return fmt.Errorf("insert needs %q", req)
		}
	}
	cols := []string{quoteIdent(spec.pk)}
	exprs := []string{"$1"}
	args := []any{pk}
	names := make([]string, 0, len(spec.cols))
	for c := range spec.cols {
		names = append(names, c)
	}
	sort.Strings(names)
	for _, c := range names {
		cols = append(cols, quoteIdent(c))
		if v, ok := vals[c]; ok {
			args = append(args, v)
			exprs = append(exprs, fmt.Sprintf("$%d", len(args)))
		} else if def, ok := spec.defaults[c]; ok {
			exprs = append(exprs, def)
		} else {
			cols = cols[:len(cols)-1]
		}
	}
	stmt := fmt.Sprintf(`INSERT INTO %s (%s) VALUES (%s)`, quoteTable(table), strings.Join(cols, ", "), strings.Join(exprs, ", "))
	if _, err := tx.Exec(ctx, stmt, args...); err != nil {
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

func demoUpdate(ctx context.Context, tx pgx.Tx, table string, spec demoTable, pk int64, vals map[string]any) error {
	names := make([]string, 0, len(vals))
	for c := range vals {
		names = append(names, c)
	}
	sort.Strings(names)
	sets := make([]string, 0, len(names))
	args := []any{pk}
	for _, c := range names {
		args = append(args, vals[c])
		sets = append(sets, fmt.Sprintf("%s = $%d", quoteIdent(c), len(args)))
	}
	if table == "public.animals" {
		sets = append(sets, "updated_at = now()")
	}
	stmt := fmt.Sprintf(`UPDATE %s SET %s WHERE %s = $1`, quoteTable(table), strings.Join(sets, ", "), quoteIdent(spec.pk))
	tag, err := tx.Exec(ctx, stmt, args...)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("no row with %s = %d (insert it first)", spec.pk, pk)
	}
	return nil
}

// readTables is the read-only allowlist for the table panel: every replicated
// table, with the column that says "recently changed" on that table.
var readTables = map[string]struct{ pk, recency string }{
	"public.animals":        {pk: "animal_id", recency: "updated_at"},
	"public.observations":   {pk: "observation_id", recency: "observed_at"},
	"public.watering_holes": {pk: "watering_hole_id", recency: "created_at"},
}

// handleTable returns the most recently changed rows of one table from both
// databases, so a page can show traffic landing. Destination rows carry the
// pipeline's metadata columns, which is how the page knows a row is fresh.
func (s *Server) handleTable(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	spec, ok := readTables[table]
	if !ok {
		writeError(w, http.StatusBadRequest, "table not in demo allowlist")
		return
	}
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 50 {
			limit = n
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	q := fmt.Sprintf(`SELECT row_to_json(t) FROM %s t ORDER BY %s DESC, %s DESC LIMIT $1`,
		quoteTable(table), quoteIdent(spec.recency), quoteIdent(spec.pk))
	read := func(pool interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}) []json.RawMessage {
		out := []json.RawMessage{}
		rows, err := pool.Query(ctx, q, limit)
		if err != nil {
			return out // table may not exist on the destination yet
		}
		defer rows.Close()
		for rows.Next() {
			var raw json.RawMessage
			if rows.Scan(&raw) == nil {
				out = append(out, raw)
			}
		}
		return out
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"table":   table,
		"pk":      spec.pk,
		"recency": spec.recency,
		"source":  read(s.source),
		"dest":    read(s.dest),
	})
}

// handleRow shows the same row on both sides, so the page can render "source
// says X, destination says X, applied at T".
func (s *Server) handleRow(w http.ResponseWriter, r *http.Request) {
	table := r.PathValue("table")
	spec, ok := demoTables[table]
	if !ok {
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
