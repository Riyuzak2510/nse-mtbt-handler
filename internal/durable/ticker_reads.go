package durable

import (
	"context"
	"fmt"
	"nse-mtbt-handler/internal/feed"
	"time"
)

func (s *Intake) Status(ctx context.Context) (map[string]any, error) {
	var source int32
	var broker, work, pending int64
	var state, reason string
	var last *time.Time
	e := s.db.Pool().QueryRow(ctx, `SELECT f.source_sequence,f.broker_sequence,f.status,f.reason,f.last_received,(SELECT count(*) FROM mtbt.match_work WHERE stream_id=f.stream_id),(SELECT count(*) FROM mtbt.orders WHERE stream_id=f.stream_id AND status='PENDING') FROM mtbt.feed_state f WHERE stream_id=$1`, s.m.StreamID).Scan(&source, &broker, &state, &reason, &last, &work, &pending)
	s.mu.Lock()
	failed := s.failed
	s.mu.Unlock()
	if failed != nil {
		state = "INVALID"
		reason = failed.Error()
	} else if state != "INVALID" && (last == nil || time.Since(*last) > 5*time.Second) {
		state = "STALE"
	}
	return map[string]any{"stream_id": s.m.StreamID, "source_sequence": source, "broker_sequence": broker, "status": state, "reason": reason, "pending_work": work, "pending_orders": pending}, e
}
func (s *Intake) OrderView(ctx context.Context, id feed.OrderID) (map[string]any, error) {
	var token int32
	var side, state string
	var price, remaining int32
	var accepted, filled, cancelled, admitted, applied int64
	e := s.db.Pool().QueryRow(ctx, `SELECT o.token,o.side,o.price,o.accepted_quantity,o.remaining_quantity,o.filled_quantity,o.cancelled_quantity,o.status,p.admitted,a.applied FROM mtbt.orders o JOIN mtbt.ticker_progress p USING(stream_id,token) JOIN mtbt.ticker_applied a USING(stream_id,token) WHERE o.stream_id=$1 AND o.order_id=$2`, s.m.StreamID, id.Bytes()).Scan(&token, &side, &price, &accepted, &remaining, &filled, &cancelled, &state, &admitted, &applied)
	return map[string]any{"stream_id": s.m.StreamID, "token": token, "order_id": fmt.Sprintf("%x", id.Bytes()), "side": side, "price": price, "accepted_quantity": accepted, "remaining_quantity": remaining, "filled_quantity": filled, "cancelled_quantity": cancelled, "status": state, "admitted": admitted, "applied": applied}, e
}
func (s *Service) TickerSnapshot(ctx context.Context, depth int) (Snapshot, error) {
	snap, e := s.Snapshot(ctx, s.token, depth, time.Now(), 5*time.Second)
	if e != nil {
		return snap, e
	}
	// Read SQL after copying RAM; if a concurrent match advanced RAM/SQL,
	// suppress the older snapshot instead of claiming a mixed watermark.
	state, e := s.TickerStatus(ctx)
	if e != nil {
		return Snapshot{}, e
	}
	if state["source_sequence"] != snap.SourceSequence || uint64(state["source_broker"].(int64)) != snap.BrokerSequence {
		state["status"] = "STALE"
		state["reason"] = "matching watermark changed during snapshot"
	}
	// Recheck native validity and pending work in SQL; never call a RAM-only
	// snapshot ready because another ticker advanced intake's watermark.
	if state["status"] != "VALID" {
		snap.Status = fmt.Sprint(state["status"])
		snap.Reason = fmt.Sprint(state["reason"])
		snap.Depth.Bids = nil
		snap.Depth.Asks = nil
	}
	return snap, nil
}
