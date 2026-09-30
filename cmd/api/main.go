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
	"github.com/BABTUNA/bartie/internal/metrics"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	metrics.Init("api", cfg.ErrorLogPath)

	srv, err := api.New(ctx, cfg, nil)
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
