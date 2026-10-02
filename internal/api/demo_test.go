package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/BABTUNA/superbartie/internal/config"
)

func TestPokeIDAndScope(t *testing.T) {
	good := []map[string]any{
		{"observation_id": float64(9001)},
		{"observation_id": json.Number("9099")},
		{"observation_id": "3000000123"},
	}
	for _, pk := range good {
		if _, err := pokeID(pk); err != nil {
			t.Fatalf("rejected %v: %v", pk, err)
		}
	}
	bad := []map[string]any{
		{"animal_id": float64(9001)},
		{"observation_id": float64(9001), "x": float64(1)},
		{"observation_id": "abc"},
		{"observation_id": 9001.5},
		{},
	}
	for _, pk := range bad {
		if _, err := pokeID(pk); err == nil {
			t.Fatalf("accepted %v", pk)
		}
	}

	scopes := map[int64]string{
		1:             "",
		8999:          "",
		9000:          "demo",
		9099:          "demo",
		9100:          "",
		2_999_999_999: "",
		3_000_000_000: "traffic",
		4_499_270_205: "traffic",
	}
	for id, want := range scopes {
		if got := pokeScope(id); got != want {
			t.Fatalf("scope(%d) = %q, want %q", id, got, want)
		}
	}
}

func TestOnlyListedPlacesAreWritable(t *testing.T) {
	if !validPlace(defaultPlace) {
		t.Fatal("the default place must be in the list")
	}
	for _, p := range []string{"", "the moon", "the north ridge; DROP TABLE observations", "The North Ridge"} {
		if validPlace(p) {
			t.Fatalf("accepted %q", p)
		}
	}
}

func TestRateLimiter(t *testing.T) {
	l := newDemoLimiter(3)
	hits := 0
	h := l.limit(func(w http.ResponseWriter, _ *http.Request) { hits++; w.WriteHeader(200) })
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("POST", "/demo/poke", nil)
		req.RemoteAddr = "1.2.3.4:5555"
		rr := httptest.NewRecorder()
		h(rr, req)
		if i < 3 && rr.Code != 200 {
			t.Fatalf("request %d should pass, got %d", i, rr.Code)
		}
		if i >= 3 && rr.Code != http.StatusTooManyRequests {
			t.Fatalf("request %d should be limited, got %d", i, rr.Code)
		}
	}
	if hits != 3 {
		t.Fatalf("handler ran %d times, want 3", hits)
	}
	// Another IP is independent.
	req := httptest.NewRequest("POST", "/demo/poke", nil)
	req.RemoteAddr = "9.9.9.9:1"
	rr := httptest.NewRecorder()
	h(rr, req)
	if rr.Code != 200 {
		t.Fatalf("other ip limited: %d", rr.Code)
	}
	// Forwarded header wins when present (Caddy in front).
	req = httptest.NewRequest("POST", "/demo/poke", nil)
	req.RemoteAddr = "9.9.9.9:1"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	rr = httptest.NewRecorder()
	h(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("forwarded ip should be limited: %d", rr.Code)
	}
}

func TestParseWindow(t *testing.T) {
	req := httptest.NewRequest("GET", "/pipelines/bartie/usage", nil)
	w, err := parseWindow(req)
	if err != nil {
		t.Fatal(err)
	}
	if d := w.To.Sub(w.From); d < 59*time.Minute || d > 61*time.Minute {
		t.Fatalf("default window should be an hour, got %s", d)
	}
	req = httptest.NewRequest("GET", "/pipelines/bartie/usage?from=2026-10-01T00:00:00Z&to=2026-10-01T01:00:00Z", nil)
	w, err = parseWindow(req)
	if err != nil || w.From.Hour() != 0 || w.To.Hour() != 1 {
		t.Fatalf("explicit window: %v %v", w, err)
	}
	req = httptest.NewRequest("GET", "/pipelines/bartie/usage?from=2026-10-01T02:00:00Z&to=2026-10-01T01:00:00Z", nil)
	if _, err = parseWindow(req); err == nil {
		t.Fatal("inverted window accepted")
	}
	req = httptest.NewRequest("GET", "/pipelines/bartie/usage?from=yesterday", nil)
	if _, err = parseWindow(req); err == nil {
		t.Fatal("garbage from accepted")
	}
}

func TestPipelineUUIDGuard(t *testing.T) {
	s := &Server{}
	h := s.withPipeline(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux := http.NewServeMux()
	mux.HandleFunc("GET /pipelines/{uuid}", h)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/pipelines/other", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown uuid: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/pipelines/bartie", nil))
	if rr.Code != 200 {
		t.Fatalf("known uuid: %d", rr.Code)
	}
}

func TestHoldTrafficNestsAndReleases(t *testing.T) {
	dir := t.TempDir()
	s := &Server{cfg: config.Config{ErrorLogPath: filepath.Join(dir, "errors.jsonl")}}
	exists := func() bool { _, err := os.Stat(s.trafficHoldFile()); return err == nil }

	a := s.holdTraffic()
	if !exists() {
		t.Fatal("first hold should create the marker")
	}
	b := s.holdTraffic()
	a()
	if !exists() {
		t.Fatal("marker must stay while another verify still holds it")
	}
	a() // releasing twice must not let go of someone else's hold
	if !exists() {
		t.Fatal("double release dropped the second hold")
	}
	b()
	if exists() {
		t.Fatal("last release should remove the marker")
	}
	// the pause switch is a different file and is not touched by a hold
	if s.trafficHoldFile() == s.trafficPauseFile() {
		t.Fatal("hold and pause must be separate markers")
	}
}
