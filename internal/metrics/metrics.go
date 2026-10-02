// Package metrics is the pipeline's view of itself: how long flushes take,
// what broke, and which phase a process is in. Each binary keeps its own
// in-memory state and serves it as JSON on a localhost port; errors are also
// appended to a JSONL file so they outlive the process that hit them. The api
// binary reads both and never shares a connection pool with the pipeline.
package metrics

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const flushRing = 512

// Flush is one flushTable call: a table's batch applied in one transaction.
type Flush struct {
	At         time.Time `json:"at"`
	Table      string    `json:"table"`
	Events     int       `json:"events"`
	DurationMs float64   `json:"durationMs"`
}

// ErrorLog uses Artie's PayloadsPipelineErrorLog field names where they apply
// so a consumer written against their API parses ours.
type ErrorLog struct {
	Timestamp   time.Time `json:"timestamp"`
	Service     string    `json:"service"` // reader | writer | vecwriter | api
	Table       string    `json:"table,omitempty"`
	Message     string    `json:"message"`
	ErrorDetail string    `json:"errorDetail,omitempty"`
}

// Snapshot is what a process serves on GET /metrics.json.
type Snapshot struct {
	Service   string    `json:"service"`
	StartedAt time.Time `json:"startedAt"`
	Phase     string    `json:"phase"` // reader: backfill | streaming; writer: running | paused
	Paused    bool      `json:"paused"`

	// set when the pause is timed and will lift on its own
	PausedUntil *time.Time `json:"pausedUntil,omitempty"`

	Flushes []Flush `json:"flushes"`
	MergeMs MergeMs `json:"mergeMs"`
}

// MergeMs summarizes recent flush durations. Zero Samples means the writer has
// not flushed since it started, not that merges are instant.
type MergeMs struct {
	Samples int     `json:"samples"`
	Last    float64 `json:"last"`
	P50     float64 `json:"p50"`
	P95     float64 `json:"p95"`
	Max     float64 `json:"max"`
}

var (
	mu        sync.Mutex
	service   string
	startedAt = time.Now()
	phase     string
	flushes   []Flush
	errPath   string

	paused atomic.Bool

	// when a timed pause ends, zero when not paused or paused with no end
	pausedUntil time.Time

	// lifts a timed pause, replaced or stopped whenever the pause state changes
	resumeTimer *time.Timer
)

// Init names the process and picks the error log path. Call once from main.
func Init(svc, errorLogPath string) {
	mu.Lock()
	defer mu.Unlock()
	service = svc
	errPath = errorLogPath
	if errPath != "" {
		_ = os.MkdirAll(filepath.Dir(errPath), 0o755)
	}
}

func SetPhase(p string) {
	mu.Lock()
	phase = p
	mu.Unlock()
}

func Paused() bool { return paused.Load() }

// SetPaused pauses or resumes with no time limit
// it cancels any timed pause that was running
func SetPaused(v bool) {
	mu.Lock()
	if resumeTimer != nil {
		resumeTimer.Stop()
		resumeTimer = nil
	}
	pausedUntil = time.Time{}
	mu.Unlock()
	setPaused(v)
}

// PauseFor pauses now and resumes on its own after d
// the process resumes itself, so a caller that goes away cannot leave it stuck
func PauseFor(d time.Duration) time.Time {
	until := time.Now().Add(d)
	mu.Lock()
	if resumeTimer != nil {
		resumeTimer.Stop()
	}
	pausedUntil = until
	resumeTimer = time.AfterFunc(d, func() {
		mu.Lock()
		pausedUntil = time.Time{}
		resumeTimer = nil
		mu.Unlock()
		setPaused(false)
	})
	mu.Unlock()
	setPaused(true)
	return until
}

func setPaused(v bool) {
	paused.Store(v)
	if v {
		SetPhase("paused")
	} else {
		SetPhase("running")
	}
}

func RecordFlush(table string, events int, d time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	flushes = append(flushes, Flush{At: time.Now(), Table: table, Events: events, DurationMs: float64(d.Microseconds()) / 1000})
	if len(flushes) > flushRing {
		flushes = flushes[len(flushes)-flushRing:]
	}
}

// RecordError appends to the JSONL file (if configured) and logs. It never
// fails the caller: metrics must not be able to break the pipeline.
func RecordError(table, message string, err error) {
	entry := ErrorLog{Timestamp: time.Now().UTC(), Service: service, Table: table, Message: message}
	if err != nil {
		entry.ErrorDetail = err.Error()
	}
	slog.Error(message, "table", table, "err", err)

	mu.Lock()
	path := errPath
	mu.Unlock()
	if path == "" {
		return
	}
	f, openErr := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if openErr != nil {
		slog.Warn("error log unavailable", "path", path, "err", openErr)
		return
	}
	defer f.Close()
	b, _ := json.Marshal(entry)
	_, _ = f.Write(append(b, '\n'))
}

// ReadErrors returns the newest entries first, at most limit. A missing file
// is an empty log, not an error.
func ReadErrors(path string, limit int) ([]ErrorLog, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return []ErrorLog{}, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var all []ErrorLog
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var e ErrorLog
		if json.Unmarshal(sc.Bytes(), &e) == nil {
			all = append(all, e)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Timestamp.After(all[j].Timestamp) })
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	if all == nil {
		all = []ErrorLog{}
	}
	return all, nil
}

func TakeSnapshot() Snapshot {
	mu.Lock()
	defer mu.Unlock()
	out := Snapshot{
		Service:   service,
		StartedAt: startedAt,
		Phase:     phase,
		Paused:    paused.Load(),
		Flushes:   append([]Flush(nil), flushes...),
	}
	if !pausedUntil.IsZero() {
		t := pausedUntil
		out.PausedUntil = &t
	}
	out.MergeMs = summarize(out.Flushes)
	return out
}

func summarize(fl []Flush) MergeMs {
	if len(fl) == 0 {
		return MergeMs{}
	}
	ds := make([]float64, len(fl))
	for i, f := range fl {
		ds[i] = f.DurationMs
	}
	last := ds[len(ds)-1]
	sort.Float64s(ds)
	pct := func(p float64) float64 {
		idx := int(p*float64(len(ds)-1) + 0.5)
		return ds[idx]
	}
	return MergeMs{Samples: len(ds), Last: last, P50: pct(0.50), P95: pct(0.95), Max: ds[len(ds)-1]}
}

// Handler serves the snapshot and, for the writer, a pause/resume control.
//
//	GET  /metrics.json
//	POST /control  {"status": "paused" | "running"}
func Handler(controllable bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(TakeSnapshot())
	})
	if controllable {
		mux.HandleFunc("POST /control", func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				Status string `json:"status"`

				// optional, makes the pause lift on its own after this many seconds
				Seconds int `json:"seconds"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "bad json", http.StatusBadRequest)
				return
			}
			switch body.Status {
			case "paused":
				if body.Seconds > 0 {
					PauseFor(time.Duration(body.Seconds) * time.Second)
				} else {
					SetPaused(true)
				}
			case "running":
				SetPaused(false)
			default:
				http.Error(w, `status must be "paused" or "running"`, http.StatusBadRequest)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"success":true,"status":%q}`, body.Status)
		})
	}
	return mux
}

// Serve starts the metrics endpoint in the background. Failing to bind is
// logged, not fatal: the pipeline runs fine blind, it is just not observable.
func Serve(addr string, controllable bool) {
	if addr == "" {
		return
	}
	go func() {
		srv := &http.Server{Addr: addr, Handler: Handler(controllable), ReadHeaderTimeout: 5 * time.Second}
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Warn("metrics endpoint failed", "addr", addr, "err", err)
		}
	}()
}
