package durable

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"sort"
	"strconv"
	"time"
)

type OutcomeOptions struct {
	Enabled                         bool
	MaxPendingRows, MaxPendingBytes int64
}

func DefaultOutcomeOptions() OutcomeOptions { return OutcomeOptions{true, 100000, 128 << 20} }
func (o OutcomeOptions) validate() error {
	if !o.Enabled {
		return nil
	}
	if o.MaxPendingRows < 1 || o.MaxPendingRows > 1000000 || o.MaxPendingBytes < 8192 || o.MaxPendingBytes > 1<<30 {
		return fmt.Errorf("invalid pending outcome limits")
	}
	return nil
}
func (s *Service) initOutcomes(ctx context.Context, options OutcomeOptions) error {
	if e := options.validate(); e != nil {
		return e
	}
	tx, e := s.conn.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var start *int32
	var rows, limit *int64
	var seq int32
	var broker int64
	if e = tx.QueryRow(ctx, `SELECT outcomes_start_sequence,outcomes_max_rows,outcomes_max_bytes,source_sequence,broker_sequence FROM mtbt.feed_state WHERE stream_id=$1 FOR UPDATE`, s.manifest.StreamID).Scan(&start, &rows, &limit, &seq, &broker); e != nil {
		return e
	}
	if start != nil {
		if !options.Enabled || rows == nil || limit == nil || *rows != options.MaxPendingRows || *limit != options.MaxPendingBytes {
			return fmt.Errorf("outcomes enabled with different/disabled settings; refusing reinterpretation")
		}
		s.outcomes = options
		return tx.Commit(ctx)
	}
	if !options.Enabled {
		return tx.Commit(ctx)
	}
	var count int64
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM mtbt.orders WHERE stream_id=$1", s.manifest.StreamID).Scan(&count); e != nil {
		return e
	}
	if seq != 0 || broker != 0 || count != 0 {
		return fmt.Errorf("enabling outcomes requires pristine stream; in-place historical cutover is not automatic")
	}
	if _, e = tx.Exec(ctx, "SELECT set_config('mtbt.outcome_writer','v1',true)"); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE mtbt.feed_state SET outcomes_start_sequence=1,outcomes_max_rows=$2,outcomes_max_bytes=$3 WHERE stream_id=$1`, s.manifest.StreamID, options.MaxPendingRows, options.MaxPendingBytes); e != nil {
		return e
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	s.outcomes = options
	return nil
}
func (s *Service) writeOutcomes(ctx context.Context, tx pgx.Tx, p feed.Packet, position uint64, changed map[feed.OrderID]*Order) error {
	if !s.outcomes.Enabled || len(changed) == 0 {
		return nil
	}
	var count, total int64
	query := `SELECT count(*),COALESCE(sum(octet_length(b.payload)),0)::bigint FROM mtbt.order_outbox b WHERE b.stream_id=$1`
	args := []any{s.manifest.StreamID}
	if s.token != 0 {
		query = `SELECT count(*),COALESCE(sum(octet_length(b.payload)),0)::bigint FROM mtbt.order_outbox b JOIN mtbt.orders o USING(stream_id,order_id) WHERE b.stream_id=$1 AND o.token=$2`
		args = append(args, s.token)
	}
	if e := tx.QueryRow(ctx, query, args...).Scan(&count, &total); e != nil {
		return e
	}
	if int64(len(changed)) > s.outcomes.MaxPendingRows-count {
		return fmt.Errorf("pending outcome row budget exhausted; restart from DB after relay drains")
	}
	var recorded time.Time
	if e := tx.QueryRow(ctx, "SELECT transaction_timestamp()").Scan(&recorded); e != nil {
		return e
	}
	ids := make([]feed.OrderID, 0, len(changed))
	for id := range changed {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i].Bytes(), ids[j].Bytes()) < 0 })
	for _, id := range ids {
		o := changed[id]
		cause := "MATCH"
		if id == p.OrderID {
			switch p.Type {
			case feed.New:
				cause = "NEW"
			case feed.Modify:
				cause = "MODIFY"
			case feed.Cancel:
				cause = "CANCEL"
			}
		}
		snap := outcomes.OrderSnapshot{OrderIDHex: hex.EncodeToString(id.Bytes()), Token: o.Token, Side: string(o.Side), PricePaise: o.Price, Status: o.Status, AcceptedQty: strconv.FormatInt(o.Accepted, 10), FilledQty: strconv.FormatInt(o.Filled, 10), CancelledQty: strconv.FormatInt(o.Cancelled, 10), RemainingQty: o.Remaining}
		event, e := outcomes.New(s.manifest.StreamID, p.Sequence, position, p.Type, cause, snap, recorded)
		if e != nil {
			return e
		}
		raw, e := outcomes.Encode(event)
		if e != nil {
			return e
		}
		if int64(len(raw)) > s.outcomes.MaxPendingBytes-total {
			return fmt.Errorf("pending outcome byte budget exhausted; restart from DB after relay drains")
		}
		total += int64(len(raw))
		if _, e = tx.Exec(ctx, `INSERT INTO mtbt.order_outbox(stream_id,source_sequence,order_id,payload,recorded_at) VALUES($1,$2,$3,$4,$5)`, s.manifest.StreamID, p.Sequence, id.Bytes(), raw, recorded); e != nil {
			return e
		}
	}
	// PostgreSQL delivers the hint only at COMMIT. One hint covers every changed
	// order in this transaction; the pending rows remain authoritative.
	_, e := tx.Exec(ctx, "SELECT pg_notify($1, '')", outcomes.WakeChannel(s.manifest.StreamID))
	return e
}

// OrderView adds a coherent native input watermark without changing the order model.
type OrderView struct {
	Order
	AsOfSourceSequence    int32  `json:"as_of_source_sequence"`
	StreamID              int16  `json:"stream_id"`
	FeedStatus            string `json:"feed_status"`
	Reason                string `json:"reason,omitempty"`
	OutcomesStartSequence *int32 `json:"outcomes_start_sequence"`
}

func (s *Service) OrderView(ctx context.Context, id feed.OrderID) (OrderView, bool, error) {
	var v OrderView
	v.ID = id
	v.StreamID = s.manifest.StreamID
	var side string
	// A single SQL statement uses one snapshot; no tentative in-memory watermark.
	e := s.db.Pool().QueryRow(ctx, `SELECT o.token,o.side,o.price,o.accepted_quantity,o.remaining_quantity,o.filled_quantity,o.cancelled_quantity,o.status,f.source_sequence,f.status,f.reason,f.outcomes_start_sequence FROM mtbt.orders o JOIN mtbt.feed_state f USING(stream_id) WHERE o.stream_id=$1 AND o.order_id=$2`, s.manifest.StreamID, id.Bytes()).Scan(&v.Token, &side, &v.Price, &v.Accepted, &v.Remaining, &v.Filled, &v.Cancelled, &v.Status, &v.AsOfSourceSequence, &v.FeedStatus, &v.Reason, &v.OutcomesStartSequence)
	if e == pgx.ErrNoRows {
		return v, false, nil
	}
	v.Side = feed.Side(0)
	if len(side) == 1 {
		v.Side = feed.Side(side[0])
	}
	return v, e == nil, e
}
