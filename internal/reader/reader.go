package reader

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/BABTUNA/bartie/internal/backfill"
	"github.com/BABTUNA/bartie/internal/config"
	"github.com/BABTUNA/bartie/internal/events"
	"github.com/BABTUNA/bartie/internal/metrics"
)

const standbyInterval = 5 * time.Second

// Publisher is what the reader needs from the Kafka side. Publish must not
// return until every event is durably accepted by the broker.
type Publisher interface {
	Publish(ctx context.Context, evts []events.ChangeEvent) error
}

type Reader struct {
	cfg  config.Config
	conn *pgconn.PgConn
	pub  Publisher
	rels *RelCache

	// ackedLSN is the only position we ever report to Postgres. It advances
	// exclusively after Kafka has confirmed a publish; reporting anything more
	// optimistic is how CDC pipelines lose data.
	ackedLSN pglogrepl.LSN

	// slotState carries whether this is a fresh slot (backfill needed) and the
	// snapshot to read from. Captured at New, consumed by Run before streaming.
	slotState SlotState
}

func New(ctx context.Context, cfg config.Config, pub Publisher) (*Reader, error) {
	conn, err := pgconn.Connect(ctx, cfg.SourceDSN+"?replication=database")
	if err != nil {
		return nil, fmt.Errorf("connect to source (replication mode): %w", err)
	}
	state, err := ensureSlot(ctx, conn, cfg.Slot)
	if err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return &Reader{cfg: cfg, conn: conn, pub: pub, rels: NewRelCache(), slotState: state}, nil
}

func (r *Reader) Close(ctx context.Context) error {
	return r.conn.Close(ctx)
}

func (r *Reader) Run(ctx context.Context) error {
	// A fresh slot exported a snapshot: copy existing rows before streaming.
	// This must happen while the replication connection is still idle, or the
	// exported snapshot is invalidated. Backfill uses its own connection.
	if r.slotState.Fresh {
		metrics.SetPhase("backfill")
		slog.Info("fresh slot: starting backfill", "snapshot", r.slotState.SnapshotName)
		if err := backfill.Run(ctx, r.cfg.SourceDSN, r.cfg.Publication, r.slotState.SnapshotName, r.pub); err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
		slog.Info("backfill complete, starting live replication")
	}
	metrics.SetPhase("streaming")

	// startLSN 0 means "resume from the slot's confirmed position": exactly
	// what we want on both first run and restart.
	err := pglogrepl.StartReplication(ctx, r.conn, r.cfg.Slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '2'",
			fmt.Sprintf("publication_names '%s'", r.cfg.Publication),
		},
	})
	if err != nil {
		return fmt.Errorf("start replication on slot %q: %w", r.cfg.Slot, err)
	}
	slog.Info("replication started", "slot", r.cfg.Slot, "publication", r.cfg.Publication)

	// txnBuffer holds the current transaction's events until its Commit
	// arrives with the LSN + timestamp to stamp them with.
	var txnBuffer []events.ChangeEvent
	nextStandby := time.Now().Add(standbyInterval)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(nextStandby) {
			if err := r.sendStandbyUpdate(ctx); err != nil {
				return err
			}
			nextStandby = time.Now().Add(standbyInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStandby)
		rawMsg, err := r.conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // deadline hit: loop around and send a standby update
			}
			return fmt.Errorf("receive replication message: %w", err)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			if errMsg, isErr := rawMsg.(*pgproto3.ErrorResponse); isErr {
				return fmt.Errorf("postgres error on replication stream: %s", errMsg.Message)
			}
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if ka.ReplyRequested {
				if err := r.sendStandbyUpdate(ctx); err != nil {
					return err
				}
				nextStandby = time.Now().Add(standbyInterval)
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlog data: %w", err)
			}
			txnBuffer, err = r.handleWALMessage(ctx, xld, txnBuffer)
			if err != nil {
				return err
			}
		}
	}
}

func (r *Reader) handleWALMessage(ctx context.Context, xld pglogrepl.XLogData, txnBuffer []events.ChangeEvent) ([]events.ChangeEvent, error) {
	logical, err := pglogrepl.ParseV2(xld.WALData, false)
	if err != nil {
		return txnBuffer, fmt.Errorf("parse pgoutput message: %w", err)
	}

	switch m := logical.(type) {
	case *pglogrepl.RelationMessageV2:
		r.rels.Store(m)

	case *pglogrepl.BeginMessage:
		txnBuffer = txnBuffer[:0]

	case *pglogrepl.InsertMessageV2:
		rel, err := r.rels.Get(m.RelationID)
		if err != nil {
			return txnBuffer, err
		}
		evt, err := buildChangeEvent(events.OpCreate, rel, m.Tuple)
		if err != nil {
			return txnBuffer, err
		}
		txnBuffer = append(txnBuffer, evt)

	case *pglogrepl.UpdateMessageV2:
		rel, err := r.rels.Get(m.RelationID)
		if err != nil {
			return txnBuffer, err
		}
		evt, err := buildChangeEvent(events.OpUpdate, rel, m.NewTuple)
		if err != nil {
			return txnBuffer, err
		}
		// PK-changing update: the old row must die or it lingers downstream.
		oldPK, changed, err := oldKeyIfChanged(rel, m.OldTuple, evt)
		if err != nil {
			return txnBuffer, err
		}
		if changed {
			txnBuffer = append(txnBuffer, events.ChangeEvent{
				Op: events.OpDelete, Table: rel.Table, PK: oldPK, Types: evt.Types,
			})
			evt.Op = events.OpCreate
		}
		txnBuffer = append(txnBuffer, evt)

	case *pglogrepl.DeleteMessageV2:
		rel, err := r.rels.Get(m.RelationID)
		if err != nil {
			return txnBuffer, err
		}
		evt, err := buildDeleteEvent(rel, m.OldTuple)
		if err != nil {
			return txnBuffer, err
		}
		txnBuffer = append(txnBuffer, evt)

	case *pglogrepl.CommitMessage:
		if len(txnBuffer) > 0 {
			for i := range txnBuffer {
				txnBuffer[i].LSN = uint64(m.CommitLSN)
				txnBuffer[i].CommitTS = m.CommitTime
			}
			if err := r.pub.Publish(ctx, txnBuffer); err != nil {
				return txnBuffer, fmt.Errorf("publish transaction (%d events): %w", len(txnBuffer), err)
			}
		}
		// Kafka has the events (or the txn was empty for our tables): only
		// now is it safe to let Postgres discard WAL up to the txn's end.
		r.ackedLSN = m.TransactionEndLSN
		txnBuffer = txnBuffer[:0]

	default:
		// Truncate/Origin/Type messages are safe to ignore for now.
		slog.Debug("skipping pgoutput message", "type", fmt.Sprintf("%T", m))
	}
	return txnBuffer, nil
}

func (r *Reader) sendStandbyUpdate(ctx context.Context) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, r.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: r.ackedLSN,
		WALFlushPosition: r.ackedLSN,
		WALApplyPosition: r.ackedLSN,
	})
	if err != nil {
		return fmt.Errorf("send standby status update: %w", err)
	}
	slog.Debug("standby update sent", "acked_lsn", r.ackedLSN)
	return nil
}
