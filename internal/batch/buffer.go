package batch

import (
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/BABTUNA/bartie/internal/events"
)

// Buffer accumulates events between flushes, grouped by table, and remembers
// the newest Kafka message per partition so the offset commit after a flush
// covers exactly what was applied.
type Buffer struct {
	perTable map[string][]events.ChangeEvent
	count    int
	firstAdd time.Time
	newest   map[int]kafka.Message
}

func NewBuffer() *Buffer {
	return &Buffer{
		perTable: map[string][]events.ChangeEvent{},
		newest:   map[int]kafka.Message{},
	}
}

func (b *Buffer) Add(evt events.ChangeEvent, msg kafka.Message) {
	if b.count == 0 {
		b.firstAdd = time.Now()
	}
	b.perTable[evt.Table] = append(b.perTable[evt.Table], evt)
	b.count++
	if cur, ok := b.newest[msg.Partition]; !ok || msg.Offset > cur.Offset {
		b.newest[msg.Partition] = msg
	}
}

func (b *Buffer) Count() int { return b.count }

func (b *Buffer) Empty() bool { return b.count == 0 }

func (b *Buffer) Age() time.Duration {
	if b.count == 0 {
		return 0
	}
	return time.Since(b.firstAdd)
}

// TakeAll empties the buffer, returning the batched events and the messages
// whose offsets should be committed once (and only once) the flush succeeds.
func (b *Buffer) TakeAll() (map[string][]events.ChangeEvent, []kafka.Message) {
	tables := b.perTable
	msgs := make([]kafka.Message, 0, len(b.newest))
	for _, m := range b.newest {
		msgs = append(msgs, m)
	}
	b.perTable = map[string][]events.ChangeEvent{}
	b.newest = map[int]kafka.Message{}
	b.count = 0
	return tables, msgs
}
