package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/BABTUNA/bartie/internal/config"
	"github.com/BABTUNA/bartie/internal/embed"
	"github.com/BABTUNA/bartie/internal/metrics"
	"github.com/BABTUNA/bartie/internal/vecwriter"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	metrics.Init("vecwriter", cfg.ErrorLogPath)

	emb, err := embed.New(cfg)
	if err != nil {
		metrics.RecordError("", "embedder init failed", err)
		os.Exit(1)
	}
	if emb == nil {
		slog.Info("vector destination disabled (MINICDC_EMBED_PROVIDER=none); exiting")
		return
	}
	metrics.Serve(cfg.VecMetricsAddr, false)

	v, err := vecwriter.New(ctx, cfg, emb)
	if err != nil {
		metrics.RecordError("", "vecwriter init failed", err)
		os.Exit(1)
	}
	defer v.Close()

	slog.Info("vecwriter up", "dest", cfg.DestDSN, "topic", cfg.Topic, "embedder", emb.Name(), "dim", emb.Dim())
	if err := v.Run(ctx); err != nil && ctx.Err() == nil {
		metrics.RecordError("", "vecwriter failed", err)
		os.Exit(1)
	}
	slog.Info("vecwriter shut down")
}
