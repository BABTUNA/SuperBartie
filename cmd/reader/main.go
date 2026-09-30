package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/BABTUNA/bartie/internal/config"
	"github.com/BABTUNA/bartie/internal/metrics"
	"github.com/BABTUNA/bartie/internal/reader"
	"github.com/BABTUNA/bartie/internal/sink"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.Load()
	metrics.Init("reader", cfg.ErrorLogPath)
	metrics.Serve(cfg.ReaderMetricsAddr, false)
	metrics.SetPhase("starting")

	pub := sink.NewKafkaPublisher(cfg.KafkaBroker, cfg.Topic)
	defer pub.Close()

	r, err := reader.New(ctx, cfg, pub)
	if err != nil {
		metrics.RecordError("", "reader init failed", err)
		os.Exit(1)
	}
	defer r.Close(context.Background())

	slog.Info("reader up", "source", cfg.SourceDSN, "topic", cfg.Topic, "metrics", cfg.ReaderMetricsAddr)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		metrics.RecordError("", "reader failed", err)
		os.Exit(1)
	}
	slog.Info("reader shut down")
}
