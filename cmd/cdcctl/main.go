package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/BABTUNA/superbartie/internal/bench"
	"github.com/BABTUNA/superbartie/internal/config"
	"github.com/BABTUNA/superbartie/internal/verify"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	switch os.Args[1] {
	case "verify":
		fs := flag.NewFlagSet("verify", flag.ExitOnError)
		timeout := fs.Duration("timeout", 30*time.Second, "how long to wait for source and dest to converge")
		fs.Parse(os.Args[2:])

		if err := verify.Run(context.Background(), config.Load(), *timeout); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	case "latency":
		fs := flag.NewFlagSet("latency", flag.ExitOnError)
		table := fs.String("table", "public.observations", "destination table to measure")
		csv := fs.String("csv", "data/latency.csv", "path to write the per-minute CSV")
		since := fs.Duration("since", 0, "only measure rows applied within this window (e.g. 3m); 0 measures all")
		fs.Parse(os.Args[2:])

		summary, err := bench.Report(context.Background(), config.Load().DestDSN, *table, *csv, *since)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		fmt.Println(summary)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", os.Args[1])
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  cdcctl verify  [--timeout 30s]")
	fmt.Fprintln(os.Stderr, "  cdcctl latency [--table public.observations] [--csv data/latency.csv] [--since 3m]")
	os.Exit(2)
}
