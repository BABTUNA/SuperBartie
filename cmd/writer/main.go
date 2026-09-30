package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/BABTUNA/bartie/internal/config"
	"github.com/BABTUNA/bartie/internal/metrics"
	"github.com/BABTUNA/bartie/internal/writer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	metrics.Init("writer", cfg.ErrorLogPath)
	metrics.Serve(cfg.WriterMetricsAddr, true) // controllable: pause/resume

	w, err := writer.New(ctx, cfg)
	if err != nil {
		metrics.RecordError("", "writer init failed", err)
		os.Exit(1)
	}
	defer w.Close()

	slog.Info("writer up", "dest", cfg.DestDSN, "topic", cfg.Topic, "metrics", cfg.WriterMetricsAddr)
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		metrics.RecordError("", "writer failed", err)
		os.Exit(1)
	}
	slog.Info("writer shut down")
}
