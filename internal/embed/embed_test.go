package embed

import (
	"context"
	"math"
	"strings"
	"testing"
)

func cosine(a, b []float32) float64 {
	var dot float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
	}
	return dot
}

func TestHashEmbedderRanksOverlap(t *testing.T) {
	h := &hashEmbedder{dim: 256}
	vecs, err := h.Embed(context.Background(), []string{
		"where was lion 42 last seen",
		"Observation of animal 42 (lion) at Kambi watering hole: seen resting near the south bank",
		"Tracking collar C93393 reported normally; last known movement within expected range.",
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	q, hit, miss := vecs[0], vecs[1], vecs[2]
	if cosine(q, hit) <= cosine(q, miss) {
		t.Fatalf("query should be closer to the lion observation: hit=%.3f miss=%.3f", cosine(q, hit), cosine(q, miss))
	}
	if n := cosine(hit, hit); math.Abs(n-1) > 1e-4 {
		t.Fatalf("vectors should be unit length, got %.5f", n)
	}
	again, _ := h.Embed(context.Background(), []string{"where was lion 42 last seen"}, true)
	if cosine(q, again[0]) < 0.9999 {
		t.Fatal("hash embedding must be deterministic")
	}
}

func TestHashEmbedderEmptyText(t *testing.T) {
	h := &hashEmbedder{dim: 16}
	v, _ := h.Embed(context.Background(), []string{""}, false)
	for _, x := range v[0] {
		if math.IsNaN(float64(x)) {
			t.Fatal("empty text produced NaN")
		}
	}
}

func TestLiteral(t *testing.T) {
	got := Literal([]float32{0.5, -1, 0.25})
	if got != "[0.5,-1,0.25]" {
		t.Fatalf("got %q", got)
	}
	if !strings.HasPrefix(Literal(nil), "[") {
		t.Fatal("nil vector should still render brackets")
	}
}
