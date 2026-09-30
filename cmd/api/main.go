package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BABTUNA/bartie/internal/api"
	"github.com/BABTUNA/bartie/internal/config"
	"github.com/BABTUNA/bartie/internal/embed"
	"github.com/BABTUNA/bartie/internal/llm"
	"github.com/BABTUNA/bartie/internal/metrics"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	metrics.Init("api", cfg.ErrorLogPath)

	// /ask is wired only when a vector destination exists. Same embedder
	// config as the vecwriter, or the query vectors would not be comparable.
	var asker api.Asker
	emb, err := embed.New(cfg)
	if err != nil {
		metrics.RecordError("", "embedder init failed", err)
		os.Exit(1)
	}
	if emb != nil {
		rag, err := api.NewRAG(ctx, cfg.DestDSN, emb, llm.New(cfg), cfg.SnapshotInterval)
		if err != nil {
			metrics.RecordError("", "rag init failed", err)
			os.Exit(1)
		}
		asker = rag
		slog.Info("ask enabled", "embedder", emb.Name(), "llm", llm.New(cfg).Model(), "snapshotEvery", cfg.SnapshotInterval)
	}

	srv, err := api.New(ctx, cfg, asker)
	if err != nil {
		metrics.RecordError("", "api init failed", err)
		os.Exit(1)
	}
	defer srv.Close()

	httpSrv := &http.Server{
		Addr:              cfg.APIAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      90 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
	}()

	slog.Info("api up", "addr", cfg.APIAddr, "pipeline", api.PipelineUUID)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		metrics.RecordError("", "api failed", err)
		os.Exit(1)
	}
	slog.Info("api shut down")
}
