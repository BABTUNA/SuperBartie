package metrics

import (
	"path/filepath"
	"testing"
	"time"
)

func TestSummarizePercentiles(t *testing.T) {
	var fl []Flush
	for i := 1; i <= 100; i++ {
		fl = append(fl, Flush{DurationMs: float64(i)})
	}
	m := summarize(fl)
	if m.Samples != 100 || m.Last != 100 || m.Max != 100 {
		t.Fatalf("samples/last/max wrong: %+v", m)
	}
	if m.P50 < 50 || m.P50 > 51 {
		t.Fatalf("p50 = %v, want ~50", m.P50)
	}
	if m.P95 < 95 || m.P95 > 96 {
		t.Fatalf("p95 = %v, want ~95", m.P95)
	}
	if summarize(nil).Samples != 0 {
		t.Fatal("empty summary should have zero samples")
	}
}

func TestErrorLogRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "errors.jsonl")
	Init("writer", path)

	RecordError("public.animals", "flush failed", errTest("boom"))
	time.Sleep(time.Millisecond)
	RecordError("", "writer failed", errTest("later"))

	got, err := ReadErrors(path, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries, got %d", len(got))
	}
	if got[0].Message != "writer failed" {
		t.Fatalf("newest first: got %q", got[0].Message)
	}
	if got[1].Table != "public.animals" || got[1].ErrorDetail != "boom" || got[1].Service != "writer" {
		t.Fatalf("fields lost: %+v", got[1])
	}

	limited, _ := ReadErrors(path, 1)
	if len(limited) != 1 {
		t.Fatalf("limit ignored: %d", len(limited))
	}
	missing, err := ReadErrors(filepath.Join(t.TempDir(), "nope.jsonl"), 10)
	if err != nil || len(missing) != 0 {
		t.Fatalf("missing file should be empty log, got %v %v", missing, err)
	}
}

func TestPauseFlipsPhase(t *testing.T) {
	SetPaused(true)
	if !Paused() || TakeSnapshot().Phase != "paused" {
		t.Fatal("pause not reflected")
	}
	SetPaused(false)
	if Paused() || TakeSnapshot().Phase != "running" {
		t.Fatal("resume not reflected")
	}
}

type errTest string

func (e errTest) Error() string { return string(e) }

func TestPauseForLiftsOnItsOwn(t *testing.T) {
	until := PauseFor(60 * time.Millisecond)
	snap := TakeSnapshot()
	if !Paused() || snap.PausedUntil == nil || !snap.PausedUntil.Equal(until) {
		t.Fatalf("timed pause not reflected: paused=%v until=%v", Paused(), snap.PausedUntil)
	}
	time.Sleep(150 * time.Millisecond)
	if Paused() || TakeSnapshot().PausedUntil != nil || TakeSnapshot().Phase != "running" {
		t.Fatal("timed pause did not lift")
	}
}

func TestExplicitResumeCancelsTimedPause(t *testing.T) {
	PauseFor(time.Hour)
	SetPaused(false)
	if Paused() || TakeSnapshot().PausedUntil != nil {
		t.Fatal("resume did not clear the timed pause")
	}
	// a plain pause after a timed one must not be lifted by the old timer
	PauseFor(40 * time.Millisecond)
	SetPaused(true)
	time.Sleep(100 * time.Millisecond)
	if !Paused() {
		t.Fatal("old timer lifted an untimed pause")
	}
	SetPaused(false)
}
