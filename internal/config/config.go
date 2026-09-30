package config

import "os"

// Config is shared by every binary; each uses the fields it needs. Everything
// comes from env vars with defaults matching deploy/docker-compose.yml run on
// the host (the compose "live" profile overrides hosts to service names).
type Config struct {
	SourceDSN   string // reader: replication connection to the source
	DestDSN     string // writer: destination database
	KafkaBroker string
	Topic       string
	Slot        string
	Publication string

	// Observability. Each pipeline process serves metrics on its own port;
	// the api scrapes them. Errors go to a JSONL file shared by every
	// process (a compose volume in deployment).
	WriterMetricsAddr string
	ReaderMetricsAddr string
	VecMetricsAddr    string
	WriterMetricsURL  string
	ReaderMetricsURL  string
	VecMetricsURL     string
	ErrorLogPath      string

	// Control API (cmd/api).
	APIAddr       string
	APIToken      string // when set, required as a bearer token on mutations
	CORSOrigins   string // comma-separated; empty allows none
	DemoRateLimit int    // requests per minute per IP on /demo and /ask

	// Vector destination (cmd/vecwriter, api /ask).
	EmbedProvider string // openai | voyage | none
	EmbedModel    string
	EmbedDim      int
	EmbedAPIKey   string
	LLMModel      string
	LLMAPIKey     string
}

func Load() Config {
	return Config{
		SourceDSN:   getenv("MINICDC_SOURCE_DSN", "postgres://postgres:bartie@localhost:5410/terra"),
		DestDSN:     getenv("MINICDC_DEST_DSN", "postgres://postgres:bartie@localhost:5411/warehouse"),
		KafkaBroker: getenv("MINICDC_KAFKA_BROKER", "localhost:19092"),
		Topic:       getenv("MINICDC_TOPIC", "cdc.events"),
		Slot:        getenv("MINICDC_SLOT", "bartie"),
		Publication: getenv("MINICDC_PUBLICATION", "dbz_publication"),

		WriterMetricsAddr: getenv("MINICDC_WRITER_METRICS_ADDR", "127.0.0.1:9101"),
		ReaderMetricsAddr: getenv("MINICDC_READER_METRICS_ADDR", "127.0.0.1:9102"),
		VecMetricsAddr:    getenv("MINICDC_VEC_METRICS_ADDR", "127.0.0.1:9103"),
		WriterMetricsURL:  getenv("MINICDC_WRITER_METRICS_URL", "http://127.0.0.1:9101"),
		ReaderMetricsURL:  getenv("MINICDC_READER_METRICS_URL", "http://127.0.0.1:9102"),
		VecMetricsURL:     getenv("MINICDC_VEC_METRICS_URL", "http://127.0.0.1:9103"),
		ErrorLogPath:      getenv("MINICDC_ERROR_LOG", "data/errors.jsonl"),

		APIAddr:       getenv("MINICDC_API_ADDR", "127.0.0.1:8080"),
		APIToken:      getenv("MINICDC_API_TOKEN", ""),
		CORSOrigins:   getenv("MINICDC_CORS_ORIGINS", "http://localhost:3000"),
		DemoRateLimit: getenvInt("MINICDC_DEMO_RATE_LIMIT", 30),

		EmbedProvider: getenv("MINICDC_EMBED_PROVIDER", "none"),
		EmbedModel:    getenv("MINICDC_EMBED_MODEL", "text-embedding-3-small"),
		EmbedDim:      getenvInt("MINICDC_EMBED_DIM", 1536),
		EmbedAPIKey:   getenv("MINICDC_EMBED_API_KEY", ""),
		LLMModel:      getenv("MINICDC_LLM_MODEL", "claude-haiku-4-5-20251001"),
		LLMAPIKey:     getenv("MINICDC_LLM_API_KEY", ""),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}
