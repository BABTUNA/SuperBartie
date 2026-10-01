// Package api is Bartie's control plane: one HTTP server that makes the
// pipeline visible (usage, errors, verify) and pokeable (demo writes on the
// source). Paths mirror Artie's public API where an equivalent exists so the
// MCP layer can generate tools with their names.
//
// It reads the source and destination over ordinary connections and scrapes
// each pipeline process's metrics endpoint. It never shares a pool with the
// writer and never touches the replication connection.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BABTUNA/superbartie/internal/config"
	"github.com/BABTUNA/superbartie/internal/metrics"
)

// PipelineUUID is the one pipeline this deployment runs. Artie addresses
// pipelines by uuid; ours is a fixed, readable one.
const PipelineUUID = "bartie"

type Server struct {
	cfg    config.Config
	source *pgxpool.Pool
	dest   *pgxpool.Pool
	http   *http.Client
	kafka  *kafkaLag
	demo   *demoLimiter
	asker  Asker // nil until 6b wires one in
}

// Asker answers a natural-language question from the vector destination.
// Defined here so the api compiles before the vector writer exists.
type Asker interface {
	Ask(ctx context.Context, question string, mode string) (any, error)
}

func New(ctx context.Context, cfg config.Config, asker Asker) (*Server, error) {
	source, err := pgxpool.New(ctx, cfg.SourceDSN)
	if err != nil {
		return nil, fmt.Errorf("connect source: %w", err)
	}
	dest, err := pgxpool.New(ctx, cfg.DestDSN)
	if err != nil {
		source.Close()
		return nil, fmt.Errorf("connect dest: %w", err)
	}
	return &Server{
		cfg:    cfg,
		source: source,
		dest:   dest,
		http:   &http.Client{Timeout: 2 * time.Second},
		kafka:  newKafkaLag(cfg.KafkaBroker, cfg.Topic),
		demo:   newDemoLimiter(cfg.DemoRateLimit),
		asker:  asker,
	}, nil
}

func (s *Server) Close() {
	s.source.Close()
	s.dest.Close()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})

	// Artie-shaped.
	mux.HandleFunc("GET /pipelines", s.handlePipelineList)
	mux.HandleFunc("GET /pipelines/{uuid}", s.withPipeline(s.handlePipelineDetail))
	mux.HandleFunc("GET /pipelines/{uuid}/usage", s.withPipeline(s.handleUsage))
	mux.HandleFunc("GET /pipelines/{uuid}/error-logs", s.withPipeline(s.handleErrorLogs))
	mux.HandleFunc("POST /pipelines/{uuid}/status", s.withPipeline(s.requireToken(s.handleStatus)))

	// Bartie-only: their product cannot read the destination.
	mux.HandleFunc("POST /pipelines/{uuid}/verify", s.withPipeline(s.handleVerify))

	// Demo surface for the live page.
	mux.HandleFunc("POST /demo/poke", s.demo.limit(s.handlePoke))
	mux.HandleFunc("GET /demo/row/{table}/{pk}", s.handleRow)
	mux.HandleFunc("GET /demo/table/{table}", s.handleTable)
	mux.HandleFunc("GET /demo/traffic", s.handleTrafficGet)
	mux.HandleFunc("POST /demo/traffic", s.requireToken(s.handleTrafficSet))
	mux.HandleFunc("POST /ask", s.demo.limit(s.handleAsk))

	return s.cors(mux)
}

// withPipeline rejects any uuid but ours with Artie's shape of 404.
func (s *Server) withPipeline(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("uuid") != PipelineUUID {
			writeError(w, http.StatusNotFound, fmt.Sprintf("pipeline %q not found", r.PathValue("uuid")))
			return
		}
		next(w, r)
	}
}

// requireToken guards mutations when MINICDC_API_TOKEN is set. Unset means an
// open demo box; the writer's control port is still bound to localhost.
func (s *Server) requireToken(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.APIToken != "" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if got != s.cfg.APIToken {
				writeError(w, http.StatusUnauthorized, "bearer token required")
				return
			}
		}
		next(w, r)
	}
}

func (s *Server) cors(next http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, o := range strings.Split(s.cfg.CORSOrigins, ",") {
		if o = strings.TrimSpace(o); o != "" {
			allowed[o] = true
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && allowed[origin] {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- pipelines -------------------------------------------------------------

// PipelineSummary is the pipeline_list row. Field names follow Artie's list
// response; `status` values are theirs too (running | paused), with "unknown"
// when the writer is unreachable.
type PipelineSummary struct {
	UUID                 string `json:"uuid"`
	Name                 string `json:"name"`
	Status               string `json:"status"`
	SourceType           string `json:"sourceType"`
	DestinationType      string `json:"destinationType"`
	IsDeploying          bool   `json:"isDeploying"`
	HasBackfillingTables bool   `json:"hasBackfillingTables"`
	HasUndeployedChanges bool   `json:"hasUndeployedChanges"`
}

type PipelineDetail struct {
	PipelineSummary
	Tables    []TableInfo       `json:"tables"`
	Slot      SlotInfo          `json:"slot"`
	Consumer  ConsumerInfo      `json:"consumer"`
	Processes map[string]string `json:"processes"` // service -> phase | "unreachable"
}

type TableInfo struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
}

func (s *Server) handlePipelineList(w http.ResponseWriter, r *http.Request) {
	sum := s.summary(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"items": []PipelineSummary{sum}})
}

func (s *Server) handlePipelineDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	det := PipelineDetail{PipelineSummary: s.summary(ctx), Processes: map[string]string{}}

	tables, err := s.publicationTables(ctx)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	det.Tables = tables

	if slot, err := s.slotInfo(ctx); err == nil {
		det.Slot = slot
	} else {
		det.Slot = SlotInfo{Name: s.cfg.Slot, Error: err.Error()}
	}
	det.Consumer = s.kafka.consumer(ctx, writerGroup)

	for name, url := range map[string]string{"reader": s.cfg.ReaderMetricsURL, "writer": s.cfg.WriterMetricsURL, "vecwriter": s.cfg.VecMetricsURL} {
		if snap, err := s.scrape(ctx, url); err == nil {
			det.Processes[name] = snap.Phase
		} else {
			det.Processes[name] = "unreachable"
		}
	}
	writeJSON(w, http.StatusOK, det)
}

func (s *Server) summary(ctx context.Context) PipelineSummary {
	sum := PipelineSummary{
		UUID:            PipelineUUID,
		Name:            "terra → warehouse",
		Status:          "unknown",
		SourceType:      "PostgreSQL",
		DestinationType: "PostgreSQL",
	}
	if snap, err := s.scrape(ctx, s.cfg.WriterMetricsURL); err == nil {
		if snap.Paused {
			sum.Status = "paused"
		} else {
			sum.Status = "running"
		}
	}
	if snap, err := s.scrape(ctx, s.cfg.ReaderMetricsURL); err == nil {
		sum.HasBackfillingTables = snap.Phase == "backfill"
	}
	return sum
}

func (s *Server) publicationTables(ctx context.Context) ([]TableInfo, error) {
	rows, err := s.source.Query(ctx,
		`SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY 1, 2`, s.cfg.Publication)
	if err != nil {
		return nil, fmt.Errorf("list publication tables: %w", err)
	}
	defer rows.Close()
	out := []TableInfo{}
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Schema, &t.Name); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// --- status ----------------------------------------------------------------

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || (body.Status != "paused" && body.Status != "running") {
		writeError(w, http.StatusBadRequest, `body must be {"status": "paused" | "running"}`)
		return
	}
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodPost, s.cfg.WriterMetricsURL+"/control", strings.NewReader(string(b)))
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
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "status": body.Status})
}

// --- error logs ------------------------------------------------------------

func (s *Server) handleErrorLogs(w http.ResponseWriter, r *http.Request) {
	items, err := metrics.ReadErrors(s.cfg.ErrorLogPath, 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

// --- ask (wired in 6b) -----------------------------------------------------

func (s *Server) handleAsk(w http.ResponseWriter, r *http.Request) {
	if s.asker == nil {
		writeError(w, http.StatusNotImplemented, "vector destination not configured")
		return
	}
	var body struct {
		Q    string `json:"q"`
		Mode string `json:"mode"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Q) == "" {
		writeError(w, http.StatusBadRequest, `body must be {"q": "..."}`)
		return
	}
	if len(body.Q) > 500 {
		writeError(w, http.StatusBadRequest, "question too long")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	out, err := s.asker.Ask(ctx, body.Q, body.Mode)
	if err != nil {
		slog.Warn("ask failed", "err", err)
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// --- helpers ---------------------------------------------------------------

type metricsSnapshot = metrics.Snapshot

func (s *Server) scrape(ctx context.Context, base string) (metricsSnapshot, error) {
	var snap metricsSnapshot
	if base == "" {
		return snap, errors.New("no metrics url")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/metrics.json", nil)
	if err != nil {
		return snap, err
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return snap, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return snap, fmt.Errorf("metrics returned %d", resp.StatusCode)
	}
	return snap, json.NewDecoder(resp.Body).Decode(&snap)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"error": msg})
}
