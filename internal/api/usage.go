package api

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/BABTUNA/bartie/internal/metrics"
	"github.com/BABTUNA/bartie/internal/verify"
)

const writerGroup = "bartie-writer"

// TableStats is byte-compatible with Artie's RouterPipelineTableStats:
// `count` is rows processed in the window, `latency` is seconds, nullable
// when nothing moved.
type TableStats struct {
	TableName string   `json:"tableName"`
	Count     int64    `json:"count"`
	Latency   *float64 `json:"latency"`
}

// Usage is Artie's response plus the three numbers that say WHERE the latency
// is. Their skill can only report `latency`; ours can name the bottleneck.
type Usage struct {
	TableStats []TableStats `json:"tableStats"`
	Window     Window       `json:"window"`

	// Reader side: how far the source's WAL is ahead of what the reader has
	// acked. Growing here with a flat backlog means the reader or the source.
	ReaderLagBytes *int64 `json:"readerLagBytes"`
	SlotActive     *bool  `json:"slotActive"`

	// Broker side: messages published but not yet committed by the writer.
	// Growing here means the writer is behind the reader.
	BacklogMessages *int64 `json:"backlogMessages"`

	// Destination side: how long the writer's staging+MERGE transactions take.
	// Rising here means the destination is the bottleneck.
	MergeMs *metrics.MergeMs `json:"mergeMs"`

	// Freshness, independent of the window: the newest row applied.
	LastAppliedAt   *time.Time `json:"lastAppliedAt"`
	LastCommitTs    *time.Time `json:"lastCommitTs"`
	WriterReachable bool       `json:"writerReachable"`
}

type Window struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

func (s *Server) handleUsage(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	win, err := parseWindow(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	u := Usage{Window: win, TableStats: []TableStats{}}

	stats, err := s.tableStats(ctx, win)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	u.TableStats = stats

	if slot, err := s.slotInfo(ctx); err == nil {
		u.ReaderLagBytes = &slot.LagBytes
		u.SlotActive = &slot.Active
	}
	if c := s.kafka.consumer(ctx, writerGroup); c.Error == "" {
		u.BacklogMessages = &c.Backlog
	}
	if snap, err := s.scrape(ctx, s.cfg.WriterMetricsURL); err == nil {
		u.WriterReachable = true
		m := snap.MergeMs
		u.MergeMs = &m
	}
	u.LastAppliedAt, u.LastCommitTs = s.freshness(ctx)

	writeJSON(w, http.StatusOK, u)
}

// parseWindow follows Artie's contract (RFC3339 from/to) but defaults to the
// last hour when absent, matching what their monitoring skill tells the agent
// to assume.
func parseWindow(r *http.Request) (Window, error) {
	now := time.Now().UTC()
	win := Window{From: now.Add(-time.Hour), To: now}
	if v := r.URL.Query().Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return win, fmt.Errorf("from: %v", err)
		}
		win.From = t
	}
	if v := r.URL.Query().Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return win, fmt.Errorf("to: %v", err)
		}
		win.To = t
	}
	if !win.To.After(win.From) {
		return win, fmt.Errorf("to must be after from")
	}
	return win, nil
}

// tableStats measures latency the way Artie's benchmarks repo does: source
// commit time vs destination apply time, both stamped on every row.
func (s *Server) tableStats(ctx context.Context, win Window) ([]TableStats, error) {
	tables, err := s.destTables(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]TableStats, 0, len(tables))
	for _, t := range tables {
		q := fmt.Sprintf(`
SELECT count(*),
       avg(extract(epoch FROM __bartie_updated_at - __bartie_commit_ts))
FROM %s
WHERE __bartie_updated_at >= $1 AND __bartie_updated_at < $2
  AND __bartie_commit_ts IS NOT NULL`, quoteTable(t))
		var ts TableStats
		ts.TableName = t
		var avg *float64
		if err := s.dest.QueryRow(ctx, q, win.From, win.To).Scan(&ts.Count, &avg); err != nil {
			return nil, fmt.Errorf("%s: usage: %w", t, err)
		}
		if avg != nil {
			rounded := float64(int(*avg*1000)) / 1000
			ts.Latency = &rounded
		}
		out = append(out, ts)
	}
	return out, nil
}

// destTables lists replicated tables on the destination: anything the writer
// stamped with its metadata column, minus the vector destination's own
// tables, which carry the same columns but are derived data with their own
// (embedding-bound) latency.
func (s *Server) destTables(ctx context.Context) ([]string, error) {
	rows, err := s.dest.Query(ctx, `
SELECT table_schema || '.' || table_name
FROM information_schema.columns
WHERE column_name = '__bartie_commit_ts'
  AND table_name NOT LIKE 'bartie\_vectors%'
ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("list dest tables: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Server) freshness(ctx context.Context) (*time.Time, *time.Time) {
	tables, err := s.destTables(ctx)
	if err != nil || len(tables) == 0 {
		return nil, nil
	}
	var newestApplied, newestCommit *time.Time
	for _, t := range tables {
		var a, c *time.Time
		q := fmt.Sprintf(`SELECT max(__bartie_updated_at), max(__bartie_commit_ts) FROM %s`, quoteTable(t))
		if err := s.dest.QueryRow(ctx, q).Scan(&a, &c); err != nil {
			continue
		}
		if a != nil && (newestApplied == nil || a.After(*newestApplied)) {
			newestApplied = a
		}
		if c != nil && (newestCommit == nil || c.After(*newestCommit)) {
			newestCommit = c
		}
	}
	return newestApplied, newestCommit
}

// --- slot ------------------------------------------------------------------

type SlotInfo struct {
	Name              string `json:"name"`
	Active            bool   `json:"active"`
	ConfirmedFlushLSN string `json:"confirmedFlushLsn,omitempty"`
	CurrentWALLSN     string `json:"currentWalLsn,omitempty"`
	LagBytes          int64  `json:"lagBytes"`
	Error             string `json:"error,omitempty"`
}

func (s *Server) slotInfo(ctx context.Context) (SlotInfo, error) {
	var info SlotInfo
	info.Name = s.cfg.Slot
	err := s.source.QueryRow(ctx, `
SELECT active,
       coalesce(confirmed_flush_lsn::text, ''),
       pg_current_wal_lsn()::text,
       coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
FROM pg_replication_slots WHERE slot_name = $1`, s.cfg.Slot).
		Scan(&info.Active, &info.ConfirmedFlushLSN, &info.CurrentWALLSN, &info.LagBytes)
	if err != nil {
		return info, fmt.Errorf("slot %q: %w", s.cfg.Slot, err)
	}
	return info, nil
}

// --- kafka consumer lag ----------------------------------------------------

type ConsumerInfo struct {
	GroupID    string         `json:"groupId"`
	Backlog    int64          `json:"backlog"`
	Partitions []PartitionLag `json:"partitions"`
	Error      string         `json:"error,omitempty"`
}

type PartitionLag struct {
	Partition int   `json:"partition"`
	Committed int64 `json:"committed"`
	End       int64 `json:"end"`
	Lag       int64 `json:"lag"`
}

// kafkaLag answers "how many messages has the writer not committed yet". It
// caches for a second: a page polling twice a second must not turn into a
// coordinator round trip each time.
type kafkaLag struct {
	client *kafka.Client
	topic  string

	mu    sync.Mutex
	cache map[string]cachedLag
}

type cachedLag struct {
	at   time.Time
	info ConsumerInfo
}

func newKafkaLag(broker, topic string) *kafkaLag {
	return &kafkaLag{
		client: &kafka.Client{Addr: kafka.TCP(broker), Timeout: 2 * time.Second},
		topic:  topic,
		cache:  map[string]cachedLag{},
	}
}

func (k *kafkaLag) consumer(ctx context.Context, group string) ConsumerInfo {
	k.mu.Lock()
	if c, ok := k.cache[group]; ok && time.Since(c.at) < time.Second {
		k.mu.Unlock()
		return c.info
	}
	k.mu.Unlock()

	info := k.fetch(ctx, group)

	k.mu.Lock()
	k.cache[group] = cachedLag{at: time.Now(), info: info}
	k.mu.Unlock()
	return info
}

func (k *kafkaLag) fetch(ctx context.Context, group string) ConsumerInfo {
	info := ConsumerInfo{GroupID: group, Partitions: []PartitionLag{}}

	meta, err := k.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{k.topic}})
	if err != nil {
		info.Error = "metadata: " + err.Error()
		return info
	}
	var partitions []int
	for _, t := range meta.Topics {
		if t.Name != k.topic {
			continue
		}
		if t.Error != nil {
			info.Error = "topic: " + t.Error.Error()
			return info
		}
		for _, p := range t.Partitions {
			partitions = append(partitions, p.ID)
		}
	}
	if len(partitions) == 0 {
		info.Error = "topic has no partitions yet"
		return info
	}

	offReq := make([]kafka.OffsetRequest, len(partitions))
	for i, p := range partitions {
		offReq[i] = kafka.LastOffsetOf(p)
	}
	ends, err := k.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{k.topic: offReq}})
	if err != nil {
		info.Error = "list offsets: " + err.Error()
		return info
	}
	endBy := map[int]int64{}
	for _, po := range ends.Topics[k.topic] {
		endBy[po.Partition] = po.LastOffset
	}

	committed, err := k.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: group, Topics: map[string][]int{k.topic: partitions}})
	if err != nil {
		info.Error = "offset fetch: " + err.Error()
		return info
	}
	if committed.Error != nil {
		info.Error = "offset fetch: " + committed.Error.Error()
		return info
	}
	commitBy := map[int]int64{}
	for _, cp := range committed.Topics[k.topic] {
		commitBy[cp.Partition] = cp.CommittedOffset
	}

	for _, p := range partitions {
		end := endBy[p]
		c, ok := commitBy[p]
		if !ok || c < 0 {
			c = 0 // group has never committed here: everything is backlog
		}
		lag := end - c
		if lag < 0 {
			lag = 0
		}
		info.Partitions = append(info.Partitions, PartitionLag{Partition: p, Committed: c, End: end, Lag: lag})
		info.Backlog += lag
	}
	return info
}

// --- verify ----------------------------------------------------------------

// handleVerify is a single pass by default. A live pipeline always has events
// in flight, so a mismatch a moment after a write is expected; `?timeout=10s`
// retries until match or deadline, which is what "did it converge" means.
func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	var (
		res verify.Result
		err error
	)
	if v := r.URL.Query().Get("timeout"); v != "" {
		wait, perr := time.ParseDuration(v)
		if perr != nil || wait <= 0 || wait > 45*time.Second {
			writeError(w, http.StatusBadRequest, "timeout must be a duration up to 45s, e.g. 10s")
			return
		}
		res, err = verify.Converge(ctx, s.cfg, wait)
	} else {
		res, err = verify.Compare(ctx, s.cfg)
	}
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func quoteTable(t string) string {
	for i := 0; i < len(t); i++ {
		if t[i] == '.' {
			return `"` + t[:i] + `"."` + t[i+1:] + `"`
		}
	}
	return `"` + t + `"`
}
