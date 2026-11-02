// Package outcomerelay delivers a pending-only journal. At least once, not exactly once.
package outcomerelay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/outcomes"
	"time"
)

type PermanentError struct{ Reason string }

func (e *PermanentError) Error() string { return e.Reason }
func IsPermanent(e error) bool {
	var p *PermanentError
	var c *outcomes.ConfigError
	return errors.As(e, &p) || errors.As(e, &c)
}

type Publisher interface {
	Publish(context.Context, outcomes.Event, []byte) (*nats.PubAck, error)
}
type Relay struct {
	conn         *pgx.Conn
	stream       int16
	afterPublish func() error
	listening    bool
	wakePending  bool // coalesced hint; only accessed by the connection-owning goroutine
}
type Stats struct {
	PendingCount int64      `json:"pending_count"`
	PendingBytes int64      `json:"pending_bytes"`
	Oldest       *time.Time `json:"oldest_recorded_at"`
	StreamID     int16      `json:"stream_id"`
}

func Open(ctx context.Context, dsn string, stream int16, manifestHash string) (r *Relay, err error) {
	if stream <= 0 || manifestHash == "" {
		return nil, &PermanentError{"invalid relay native identity"}
	}
	return OpenWithWakeups(ctx, dsn, stream, manifestHash, true)
}

// OpenWithWakeups retains polling only as an explicit benchmark/compatibility
// control. LISTEN is established on the same leased physical session, before
// the caller's first pending-row scan. There is no concurrent use of pgx.Conn.
func OpenWithWakeups(ctx context.Context, dsn string, stream int16, manifestHash string, enabled bool) (r *Relay, err error) {
	if stream <= 0 || manifestHash == "" {
		return nil, &PermanentError{"invalid relay native identity"}
	}
	r = &Relay{stream: stream, listening: enabled}
	config, e := pgx.ParseConfig(dsn)
	if e != nil {
		return nil, e
	}
	// pgx's default handler buffers every hint. Coalesce into a single bit
	// instead so a busy relay cannot accumulate an unbounded hint slice.
	target := r
	config.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) {
		if n.Channel == outcomes.WakeChannel(stream) {
			target.wakePending = true
		}
	}
	c, e := pgx.ConnectConfig(ctx, config)
	if e != nil {
		return nil, e
	}
	r.conn = c
	ok := false
	defer func() {
		if !ok {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			c.Close(cleanup)
		}
	}()
	h := sha256.Sum256([]byte(fmt.Sprintf("mtbt-outcome-relay-v1:%d", stream)))
	var locked bool
	if e = c.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", int64(binary.LittleEndian.Uint64(h[:8]))).Scan(&locked); e != nil {
		return nil, e
	}
	if !locked {
		return nil, fmt.Errorf("another relay owns native stream")
	}
	var hash, mode string
	var start *int32
	e = c.QueryRow(ctx, `SELECT manifest_hash,owner_mode,outcomes_start_sequence FROM mtbt.feed_state WHERE stream_id=$1`, stream).Scan(&hash, &mode, &start)
	if e != nil {
		return nil, e
	}
	if hash != manifestHash || mode != "matching" || start == nil {
		return nil, &PermanentError{"incompatible or outcome-disabled matching stream"}
	}
	if enabled {
		if _, e = c.Exec(ctx, "LISTEN "+pgx.Identifier{outcomes.WakeChannel(stream)}.Sanitize()); e != nil {
			return nil, e
		}
	}
	ok = true
	return r, nil
}

// Wait blocks for a commit hint or a bounded reconciliation deadline. A missed,
// duplicate or spurious hint never determines business progress: Step must scan
// the durable outbox again. Call only after Step returned no work.
func (r *Relay) Wait(ctx context.Context, reconcile time.Duration) error {
	if r == nil || r.conn == nil || r.conn.IsClosed() {
		return fmt.Errorf("relay lease connection lost")
	}
	if !r.listening || reconcile <= 0 {
		return &PermanentError{"notification wait requires LISTEN and a positive reconciliation interval"}
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	if r.wakePending {
		r.wakePending = false
		return nil
	}
	wait, cancel := context.WithTimeout(ctx, reconcile)
	defer cancel()
	for {
		e := r.conn.PgConn().WaitForNotification(wait)
		if e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(e, context.DeadlineExceeded) && !r.conn.IsClosed() {
				return nil // safety reconciliation, not an idle 100ms poll
			}
			return e
		}
		if r.wakePending {
			r.wakePending = false
			return nil
		}
	}
}
func (r *Relay) Step(ctx context.Context, p Publisher) (bool, error) {
	if r == nil || r.conn == nil || r.conn.IsClosed() {
		return false, fmt.Errorf("relay lease connection lost")
	}
	// Clear older hints before scanning. A concurrent commit is either visible
	// in this query or its hint remains buffered for Wait after an empty scan.
	r.wakePending = false
	var seq int32
	var id, raw []byte
	e := r.conn.QueryRow(ctx, `SELECT source_sequence,order_id,payload FROM mtbt.order_outbox WHERE stream_id=$1 ORDER BY source_sequence,order_id LIMIT 1`, r.stream).Scan(&seq, &id, &raw)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	event, e := outcomes.ValidateKey(raw, r.stream, seq, id)
	if e != nil {
		return false, &PermanentError{e.Error()}
	}
	ack, e := p.Publish(ctx, event, raw)
	if e != nil {
		return false, e
	}
	name, _ := outcomes.Names(r.stream)
	if ack == nil || ack.Stream != name || ack.Sequence == 0 {
		return false, &PermanentError{"unexpected outcome publish acknowledgement"}
	}
	if r.afterPublish != nil {
		if e = r.afterPublish(); e != nil {
			return false, e
		}
	}
	// Same physical leased session; no pool replacement or network wait inside tx.
	tx, e := r.conn.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(ctx)
	var stored []byte
	e = tx.QueryRow(ctx, `SELECT payload FROM mtbt.order_outbox WHERE stream_id=$1 AND source_sequence=$2 AND order_id=$3 FOR UPDATE`, r.stream, seq, id).Scan(&stored)
	if e != nil {
		return false, e
	}
	if !bytes.Equal(stored, raw) {
		return false, &PermanentError{"pending payload changed during publication"}
	}
	tag, e := tx.Exec(ctx, `DELETE FROM mtbt.order_outbox WHERE stream_id=$1 AND source_sequence=$2 AND order_id=$3 AND payload=$4`, r.stream, seq, id, raw)
	if e != nil {
		return false, e
	}
	if tag.RowsAffected() != 1 {
		return false, &PermanentError{"pending row delete did not affect exactly one row"}
	}
	if e = tx.Commit(ctx); e != nil {
		return false, e
	}
	return true, nil
}
func (r *Relay) Status(ctx context.Context) (Stats, error) {
	s := Stats{StreamID: r.stream}
	e := r.conn.QueryRow(ctx, `SELECT count(*),COALESCE(sum(octet_length(payload)),0)::bigint,min(recorded_at) FROM mtbt.order_outbox WHERE stream_id=$1`, r.stream).Scan(&s.PendingCount, &s.PendingBytes, &s.Oldest)
	return s, e
}
func (r *Relay) Close(ctx context.Context) error {
	if r == nil || r.conn == nil {
		return nil
	}
	return r.conn.Close(ctx)
}
