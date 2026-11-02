package durable

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/tickerwire"
	"time"
)

// Dispatcher is one bounded publisher per configured ticker, with its own lease
// session. Only one command is in flight per ticker; different tickers overlap.
type Dispatcher struct {
	conn         *pgx.Conn
	m            runconfig.Manifest
	token        int32
	wake         bool
	afterPublish func() error
}
type CommandPublisher interface {
	Publish(context.Context, tickerwire.Command) error
}

func OpenDispatcher(ctx context.Context, dsn string, m runconfig.Manifest, token int32) (d *Dispatcher, err error) {
	if !m.TokenAllowed(token) {
		return nil, fmt.Errorf("unknown dispatch ticker")
	}
	d = &Dispatcher{m: m, token: token}
	cfg, e := pgx.ParseConfig(dsn)
	if e != nil {
		return nil, e
	}
	target := d
	cfg.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) {
		if n.Channel == RoutingChannel(m.StreamID) {
			target.wake = true
		}
	}
	d.conn, e = pgx.ConnectConfig(ctx, cfg)
	if e != nil {
		return nil, e
	}
	ok := false
	owned := d
	defer func() {
		if !ok {
			owned.Close()
		}
	}()
	var locked bool
	if e = d.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1,$2)", int32(m.StreamID)+200000, token).Scan(&locked); e != nil {
		return nil, e
	}
	if !locked {
		return nil, fmt.Errorf("ticker dispatcher already owned")
	}
	h, e := m.Digest()
	if e != nil {
		return nil, e
	}
	var stored string
	if e = d.conn.QueryRow(ctx, `SELECT f.manifest_hash FROM mtbt.feed_state f JOIN mtbt.ticker_topology t USING(stream_id) JOIN mtbt.ticker_progress p USING(stream_id) WHERE f.stream_id=$1 AND p.token=$2 AND f.owner_mode='matching'`, m.StreamID, token).Scan(&stored); e != nil {
		return nil, e
	}
	if h != stored {
		return nil, fmt.Errorf("dispatcher topology mismatch")
	}
	if _, e = d.conn.Exec(ctx, "LISTEN "+pgx.Identifier{RoutingChannel(m.StreamID)}.Sanitize()); e != nil {
		return nil, e
	}
	ok = true
	return d, nil
}
func (d *Dispatcher) Close() {
	if d != nil && d.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = d.conn.Close(ctx)
		cancel()
	}
}
func (d *Dispatcher) Step(ctx context.Context, p CommandPublisher) (bool, error) {
	d.wake = false
	// Periodic idempotent cleanup handles either commit ordering: a witness is
	// retained until both broker publication and matching have completed.
	if _, e := d.conn.Exec(ctx, `DELETE FROM mtbt.match_work w USING mtbt.ticker_progress p,mtbt.ticker_applied a WHERE w.stream_id=$1 AND w.token=$2 AND p.stream_id=w.stream_id AND p.token=w.token AND a.stream_id=w.stream_id AND a.token=w.token AND w.sequence<=p.published AND w.sequence<=a.applied`, d.m.StreamID, d.token); e != nil {
		return false, e
	}
	var raw []byte
	var next int64
	var state string
	e := d.conn.QueryRow(ctx, `SELECT w.payload,w.sequence,f.status FROM mtbt.match_work w JOIN mtbt.ticker_progress p USING(stream_id,token) JOIN mtbt.feed_state f USING(stream_id) WHERE w.stream_id=$1 AND w.token=$2 AND w.sequence>p.published ORDER BY w.sequence LIMIT 1`, d.m.StreamID, d.token).Scan(&raw, &next, &state)
	if e == pgx.ErrNoRows {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if state == "INVALID" {
		return false, fmt.Errorf("source invalid; no routing")
	}
	c, e := tickerwire.Decode(raw)
	if e != nil {
		return false, e
	}
	if e = tickerwire.Validate(d.m, c); e != nil {
		return false, e
	}
	if c.Token != d.token || int64(c.Sequence) != next {
		return false, fmt.Errorf("routing witness identity mismatch")
	}
	if e = p.Publish(ctx, c); e != nil {
		return false, e
	}
	if d.afterPublish != nil {
		if e = d.afterPublish(); e != nil {
			return false, e
		}
	}
	tx, e := d.conn.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer tx.Rollback(context.Background())
	tag, e := tx.Exec(ctx, `UPDATE mtbt.ticker_progress SET published=$3 WHERE stream_id=$1 AND token=$2 AND published=$3-1 AND admitted>=$3`, d.m.StreamID, d.token, next)
	if e != nil {
		return false, e
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("routing progress conflict")
	}
	// The admitted command witness survives until matcher commit as well as PubAck.
	_, e = tx.Exec(ctx, `DELETE FROM mtbt.match_work WHERE stream_id=$1 AND token=$2 AND sequence=$3 AND sequence<=(SELECT applied FROM mtbt.ticker_applied WHERE stream_id=$1 AND token=$2)`, d.m.StreamID, d.token, next)
	if e != nil {
		return false, e
	}
	return true, tx.Commit(ctx)
}
func (d *Dispatcher) Wait(ctx context.Context) error {
	if d.wake {
		d.wake = false
		return nil
	}
	wait, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for {
		e := d.conn.PgConn().WaitForNotification(wait)
		if e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if wait.Err() != nil && !d.conn.IsClosed() {
				return nil
			}
			return e
		}
		if d.wake {
			d.wake = false
			return nil
		}
	}
}
