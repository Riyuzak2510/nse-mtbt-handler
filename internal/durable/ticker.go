package durable

import (
	"context"
	"fmt"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/matching"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/store"
	"nse-mtbt-handler/internal/tickerwire"
	"time"
)

// OpenTicker never creates/converts topology. Intake must initialize it first.
func OpenTicker(ctx context.Context, dsn string, m runconfig.Manifest, token int32, maxActive int) (s *Service, err error) {
	if e := m.Validate(); e != nil {
		return nil, e
	}
	if len(m.Instruments) > 16 {
		return nil, fmt.Errorf("at most16 configured tickers")
	}
	if !m.TokenAllowed(token) {
		return nil, fmt.Errorf("unknown ticker")
	}
	engine, e := matching.New(maxActive)
	if e != nil {
		return nil, e
	}
	db, e := store.OpenWithPool(ctx, dsn, 3)
	if e != nil {
		return nil, e
	}
	s = &Service{db: db, manifest: m, engine: engine, token: token, outcomes: DefaultOutcomeOptions()}
	ok := false
	owned := s
	defer func() {
		if !ok {
			owned.Close()
		}
	}()
	s.conn, e = db.Pool().Acquire(ctx)
	if e != nil {
		return nil, e
	}
	if e = lease(ctx, s.conn, "mtbt-ticker:", m.StreamID, token); e != nil {
		return nil, e
	}
	hash, e := m.Digest()
	if e != nil {
		return nil, e
	}
	var stored, protocol, state string
	if e = s.conn.QueryRow(ctx, `SELECT f.manifest_hash,t.protocol,f.status FROM mtbt.feed_state f JOIN mtbt.ticker_topology t USING(stream_id) WHERE stream_id=$1 AND f.owner_mode='matching' AND f.outcomes_start_sequence=1`, m.StreamID).Scan(&stored, &protocol, &state); e != nil {
		return nil, e
	}
	if stored != hash || protocol != "ticker-v1" || state == "INVALID" {
		return nil, fmt.Errorf("incompatible/invalid ticker topology")
	}
	var admitted, published, applied int64
	var digest string
	if e = s.conn.QueryRow(ctx, `SELECT p.admitted,p.published,a.applied,a.applied_digest,a.source_sequence,a.source_broker,a.last_received FROM mtbt.ticker_progress p JOIN mtbt.ticker_applied a USING(stream_id,token) WHERE p.stream_id=$1 AND p.token=$2`, m.StreamID, token).Scan(&admitted, &published, &applied, &digest, &s.source, &s.position, &s.last); e != nil {
		return nil, e
	}
	rows, e := s.conn.Query(ctx, `SELECT order_id,side,price,remaining_quantity FROM mtbt.orders WHERE stream_id=$1 AND token=$2 AND status IN ('OPEN','PARTIALLY_FILLED') ORDER BY order_id`, m.StreamID, token)
	if e != nil {
		return nil, e
	}
	for rows.Next() {
		var raw []byte
		var side string
		o := book.Order{Token: token}
		if e = rows.Scan(&raw, &side, &o.Price, &o.Quantity); e != nil {
			break
		}
		o.ID, e = feed.IDFromBytes(raw)
		if e != nil {
			break
		}
		if len(side) != 1 {
			e = fmt.Errorf("invalid persisted side")
			break
		}
		o.Side = feed.Side(side[0])
		if !onTick(m, token, o.Price) {
			e = fmt.Errorf("invalid persisted tick")
			break
		}
		if e = engine.Restore(o); e != nil {
			break
		}
	}
	re := rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	if re != nil {
		return nil, re
	}
	// Sum of per-instrument budgets cannot exceed the stream limit, without a
	// shared hot counter row serializing all instrument transactions.
	s.outcomes.MaxPendingRows /= int64(len(m.Instruments))
	s.outcomes.MaxPendingBytes /= int64(len(m.Instruments))
	ok = true
	return s, nil
}
func (s *Service) ProcessCommand(ctx context.Context, c tickerwire.Command) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == 0 || s.failed != "" || s.closed {
		return fmt.Errorf("ticker matcher unavailable: %s", s.failed)
	}
	defer func() {
		if err != nil {
			s.failed = err.Error()
		}
	}()
	if e := tickerwire.Validate(s.manifest, c); e != nil {
		return e
	}
	if c.Token != s.token {
		return fmt.Errorf("wrong ticker owner")
	}
	tx, e := s.conn.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	if e = writer(ctx, tx, "matcher", s.token); e != nil {
		return e
	}
	// Shared validity fence permits independent ticker transactions; native
	// invalidation takes the exclusive form so it commits after prior work.
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared($1,$2)", int32(s.manifest.StreamID)+300000, int32(0)); e != nil {
		return e
	}
	var state string
	if e = tx.QueryRow(ctx, "SELECT status FROM mtbt.feed_state WHERE stream_id=$1", s.manifest.StreamID).Scan(&state); e != nil {
		return e
	}
	if state == "INVALID" {
		return fmt.Errorf("native source invalid")
	}
	var applied, admitted int64
	var hash string
	if e = tx.QueryRow(ctx, "SELECT applied,applied_digest FROM mtbt.ticker_applied WHERE stream_id=$1 AND token=$2 FOR UPDATE", s.manifest.StreamID, s.token).Scan(&applied, &hash); e != nil {
		return e
	}
	if e = tx.QueryRow(ctx, "SELECT admitted FROM mtbt.ticker_progress WHERE stream_id=$1 AND token=$2", s.manifest.StreamID, s.token).Scan(&admitted); e != nil {
		return e
	}
	if c.Sequence <= uint64(applied) {
		if c.Sequence == uint64(applied) && c.Digest == hash {
			return nil
		}
		return fmt.Errorf("historical/conflicting routed redelivery")
	}
	if c.Sequence != uint64(applied)+1 || c.Sequence > uint64(admitted) || c.PreviousDigest != hash {
		return fmt.Errorf("routed sequence/digest gap or uncommitted command")
	}
	p, e := feed.Decode(c.Raw)
	if e != nil {
		return e
	}
	// Keep the immutable admitted witness until both publication and matching
	// commit. This verifies every command, not only the last admitted digest.
	var expected string
	if e = tx.QueryRow(ctx, "SELECT digest FROM mtbt.match_work WHERE stream_id=$1 AND token=$2 AND sequence=$3", s.manifest.StreamID, s.token, int64(c.Sequence)).Scan(&expected); e != nil {
		return e
	}
	if expected != c.Digest {
		return fmt.Errorf("command differs from admitted witness")
	}
	if p.Type == feed.New || p.Type == feed.Modify || p.Type == feed.Cancel {
		if e = s.apply(ctx, tx, p, c.SourceBroker); e != nil {
			return e
		}
	}
	source := s.source
	if p.Type == feed.Heartbeat {
		source = p.LastSequence
	} else {
		source = p.Sequence
	}
	if _, e = tx.Exec(ctx, `UPDATE mtbt.ticker_applied SET applied=$3,applied_digest=$4,source_sequence=$5,source_broker=$6,last_received=GREATEST(last_received,$7) WHERE stream_id=$1 AND token=$2`, s.manifest.StreamID, s.token, int64(c.Sequence), c.Digest, source, int64(c.SourceBroker), c.Received); e != nil {
		return e
	}
	if s.beforeCommit != nil {
		if e = s.beforeCommit(tx); e != nil {
			return e
		}
	}
	if e = tx.Commit(ctx); e != nil {
		return fmt.Errorf("uncertain matching commit: rebuild before reuse: %w", e)
	}
	s.source = source
	s.position = c.SourceBroker
	t := c.Received
	s.last = &t
	return nil
}

// TickerStatus is a coherent SQL snapshot, not an admission=completion claim.
func (s *Service) TickerStatus(ctx context.Context) (map[string]any, error) {
	var admitted, published, applied int64
	var source int32
	var position int64
	var state, reason string
	var last, global *time.Time
	e := s.db.Pool().QueryRow(ctx, `SELECT p.admitted,p.published,a.applied,a.source_sequence,a.source_broker,f.status,f.reason,a.last_received,f.last_received FROM mtbt.ticker_progress p JOIN mtbt.ticker_applied a USING(stream_id,token) JOIN mtbt.feed_state f USING(stream_id) WHERE p.stream_id=$1 AND p.token=$2`, s.manifest.StreamID, s.token).Scan(&admitted, &published, &applied, &source, &position, &state, &reason, &last, &global)
	s.mu.Lock()
	failed := s.failed
	s.mu.Unlock()
	if failed != "" {
		state = "INVALID"
		reason = failed
	} else if state != "INVALID" && (applied < admitted || last == nil || global == nil || time.Since(*last) > 5*time.Second || time.Since(*global) > 5*time.Second) {
		state = "STALE"
	}
	return map[string]any{"stream_id": s.manifest.StreamID, "token": s.token, "admitted": admitted, "published": published, "applied": applied, "source_sequence": source, "source_broker": position, "status": state, "reason": reason}, e
}
