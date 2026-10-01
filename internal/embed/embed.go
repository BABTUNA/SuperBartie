// Package embed turns text into vectors. One interface, three providers: a
// hosted model (OpenAI or Voyage over HTTP) and a deterministic hashing
// embedder that needs no key, so the stack runs end to end with zero
// credentials. Hashing gives keyword overlap, not semantics; it is enough for
// the demo dataset and for tests, and a key upgrades it.
package embed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/BABTUNA/superbartie/internal/config"
)

type Embedder interface {
	// Embed returns one vector per text, in order. query marks retrieval-time
	// input (some providers embed queries and documents differently).
	Embed(ctx context.Context, texts []string, query bool) ([][]float32, error)
	Dim() int
	Name() string
}

// New picks the provider from config. "none" returns nil, nil: the caller
// treats a nil embedder as "vector destination disabled".
func New(cfg config.Config) (Embedder, error) {
	switch cfg.EmbedProvider {
	case "", "none":
		return nil, nil
	case "hash":
		return &hashEmbedder{dim: cfg.EmbedDim}, nil
	case "openai":
		if cfg.EmbedAPIKey == "" {
			return nil, fmt.Errorf("openai embeddings need MINICDC_EMBED_API_KEY")
		}
		return &httpEmbedder{
			name: "openai/" + cfg.EmbedModel, url: "https://api.openai.com/v1/embeddings",
			model: cfg.EmbedModel, key: cfg.EmbedAPIKey, dim: cfg.EmbedDim, sendDim: true,
			client: &http.Client{Timeout: 30 * time.Second},
		}, nil
	case "voyage":
		if cfg.EmbedAPIKey == "" {
			return nil, fmt.Errorf("voyage embeddings need MINICDC_EMBED_API_KEY")
		}
		return &httpEmbedder{
			name: "voyage/" + cfg.EmbedModel, url: "https://api.voyageai.com/v1/embeddings",
			model: cfg.EmbedModel, key: cfg.EmbedAPIKey, dim: cfg.EmbedDim, inputType: true,
			client: &http.Client{Timeout: 30 * time.Second},
		}, nil
	default:
		return nil, fmt.Errorf("unknown embed provider %q (none | hash | openai | voyage)", cfg.EmbedProvider)
	}
}

// Literal renders a vector the way pgvector parses it: "[0.1,0.2,...]".
func Literal(v []float32) string {
	var b strings.Builder
	b.Grow(len(v) * 10)
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// --- hosted providers -----------------------------------------------------

// httpEmbedder speaks the request/response shape OpenAI and Voyage share:
// {"model", "input": [...]} -> {"data": [{"index", "embedding"}]}.
type httpEmbedder struct {
	name, url, model, key string
	dim                   int
	sendDim               bool // OpenAI accepts "dimensions" to truncate
	inputType             bool // Voyage wants "input_type": document | query
	client                *http.Client
}

func (h *httpEmbedder) Name() string { return h.name }
func (h *httpEmbedder) Dim() int     { return h.dim }

func (h *httpEmbedder) Embed(ctx context.Context, texts []string, query bool) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}
	body := map[string]any{"model": h.model, "input": texts}
	if h.sendDim {
		body["dimensions"] = h.dim
	}
	if h.inputType {
		if query {
			body["input_type"] = "query"
		} else {
			body["input_type"] = "document"
		}
	}
	payload, _ := json.Marshal(body)

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.url, bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+h.key)
		resp, err := h.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if resp.StatusCode == 429 || resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("%s: status %d: %s", h.name, resp.StatusCode, truncate(raw))
			continue
		}
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("%s: status %d: %s", h.name, resp.StatusCode, truncate(raw))
		}
		var out struct {
			Data []struct {
				Index     int       `json:"index"`
				Embedding []float32 `json:"embedding"`
			} `json:"data"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("%s: decode: %w", h.name, err)
		}
		if len(out.Data) != len(texts) {
			return nil, fmt.Errorf("%s: asked for %d embeddings, got %d", h.name, len(texts), len(out.Data))
		}
		vecs := make([][]float32, len(texts))
		for _, d := range out.Data {
			if d.Index < 0 || d.Index >= len(vecs) {
				return nil, fmt.Errorf("%s: embedding index %d out of range", h.name, d.Index)
			}
			if len(d.Embedding) != h.dim {
				return nil, fmt.Errorf("%s: model returned %d dims, table expects %d (set MINICDC_EMBED_DIM)", h.name, len(d.Embedding), h.dim)
			}
			vecs[d.Index] = d.Embedding
		}
		return vecs, nil
	}
	return nil, fmt.Errorf("embedding failed after retries: %w", lastErr)
}

func truncate(b []byte) string {
	if len(b) > 300 {
		return string(b[:300]) + "..."
	}
	return string(b)
}

// --- hashing embedder ------------------------------------------------------

// hashEmbedder is a feature-hashing bag of words: each token (and each
// adjacent-token pair) lands in a bucket with a sign, the vector is L2
// normalized, so cosine similarity is normalized term overlap. Deterministic
// and free. It cannot know that "cub" is a young lion; it can find "lion 42".
type hashEmbedder struct{ dim int }

func (h *hashEmbedder) Name() string { return "hash" }
func (h *hashEmbedder) Dim() int     { return h.dim }

func (h *hashEmbedder) Embed(_ context.Context, texts []string, _ bool) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, t := range texts {
		out[i] = h.one(t)
	}
	return out, nil
}

func (h *hashEmbedder) one(text string) []float32 {
	v := make([]float32, h.dim)
	toks := tokenize(text)
	add := func(s string, w float32) {
		f := fnv.New64a()
		f.Write([]byte(s))
		sum := f.Sum64()
		idx := int(sum % uint64(h.dim))
		if sum&(1<<63) != 0 {
			w = -w
		}
		v[idx] += w
	}
	for i, t := range toks {
		add(t, 1)
		if i+1 < len(toks) {
			add(t+" "+toks[i+1], 0.5)
		}
	}
	var norm float64
	for _, x := range v {
		norm += float64(x) * float64(x)
	}
	if norm == 0 {
		v[0] = 1 // empty text: a fixed unit vector rather than NaN
		return v
	}
	inv := float32(1 / math.Sqrt(norm))
	for i := range v {
		v[i] *= inv
	}
	return v
}

func tokenize(s string) []string {
	var toks []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			cur.WriteRune(r)
		} else {
			flush()
		}
	}
	flush()
	return toks
}
