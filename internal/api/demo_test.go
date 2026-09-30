package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDemoPKRange(t *testing.T) {
	if _, err := demoPK(map[string]any{"animal_id": float64(9001)}, "animal_id"); err != nil {
		t.Fatal(err)
	}
	if _, err := demoPK(map[string]any{"animal_id": json.Number("9099")}, "animal_id"); err != nil {
		t.Fatal(err)
	}
	bad := []map[string]any{
		{"animal_id": float64(1)},                     // real data, out of range
		{"animal_id": float64(9100)},                  // just past the range
		{"observation_id": float64(9001)},             // wrong pk column
		{"animal_id": float64(9001), "x": float64(1)}, // extra keys
		{"animal_id": "abc"},                          // not an int
	}
	for _, pk := range bad {
		if _, err := demoPK(pk, "animal_id"); err == nil {
			t.Fatalf("accepted %v", pk)
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
