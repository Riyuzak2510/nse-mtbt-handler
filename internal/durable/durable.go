// Package durable owns a synthetic matching book whose authoritative state is PostgreSQL.
// It never writes a reconstructed exchange book or submits real exchange executions.
package durable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/matching"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/store"
)

type Order struct {
	ID        feed.OrderID `json:"id_bits,string"`
	Token     int32        `json:"token"`
	Side      feed.Side    `json:"side"`
	Price     int32        `json:"price"`
	Accepted  int64        `json:"accepted_quantity"`
	Remaining int32        `json:"remaining_quantity"`
	Filled    int64        `json:"filled_quantity"`
	Cancelled int64        `json:"cancelled_quantity"`
	Status    string       `json:"status"`
}

type Snapshot struct {
	StreamID       int16      `json:"stream_id"`
	Status         string     `json:"status"`
	Reason         string     `json:"reason,omitempty"`
	Synthetic      bool       `json:"synthetic"`
	Ephemeral      bool       `json:"ephemeral"`
	SourceSequence int32      `json:"source_sequence"`
	BrokerSequence uint64     `json:"broker_sequence"`
	ActiveOrders   int        `json:"active_orders"`
	LastReceived   *time.Time `json:"last_received,omitempty"`
	matching.Depth
}

type Service struct {
	outcomes  OutcomeOptions
	mu        sync.Mutex
	db        *store.Store
	conn      *pgxpool.Conn // lease AND transactions share a physical connection: no stale lease owner writes
	manifest  runconfig.Manifest
	engine    *matching.Engine
	source    int32
	position  uint64
	last      *time.Time
	failed    string
	connected bool
	catchUp   uint64
	closed    bool
	// Test-only injection before PostgreSQL commit; never exposed as a runtime option.
	beforeCommit func(pgx.Tx) error
	token        int32 // nonzero only for explicit ticker-owned mode
}

func Open(ctx context.Context, dsn string, m runconfig.Manifest, maxActive int) (*Service, error) {
	return OpenWithOutcomes(ctx, dsn, m, maxActive, OutcomeOptions{})
}

func OpenWithOutcomes(ctx context.Context, dsn string, m runconfig.Manifest, maxActive int, options OutcomeOptions) (*Service, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	engine, err := matching.New(maxActive)
	if err != nil {
		return nil, err
	}
	db, err := store.OpenWithPool(ctx, dsn, 3)
	if err != nil {
		return nil, err
	}
	s := &Service{db: db, manifest: m, engine: engine}
	success := false
	defer func() {
		if !success {
			_ = s.Close()
		}
	}()
	if err = db.Migrate(ctx); err != nil {
		return nil, err
	}
	var split bool
	if err = db.Pool().QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mtbt.ticker_topology WHERE stream_id=$1)", m.StreamID).Scan(&split); err != nil {
		return nil, err
	}
	if split {
		return nil, fmt.Errorf("ticker topology requires dedicated ticker matchers, not combined mode")
	}
	s.conn, err = db.Pool().Acquire(ctx)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256([]byte(fmt.Sprint("mtbt-stream:", m.StreamID)))
	var leased bool
	if err = s.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", int64(binary.LittleEndian.Uint64(h[:8]))).Scan(&leased); err != nil {
		return nil, err
	}
	if !leased {
		return nil, fmt.Errorf("another consumer owns this stream")
	}
	if err = db.InitMode(ctx, m, "matching"); err != nil {
		return nil, err
	}
	if err = s.initOutcomes(ctx, options); err != nil {
		return nil, err
	}
	var topology int
	if err = s.conn.QueryRow(ctx, "SELECT count(*) FROM mtbt.subscriber_topology WHERE stream_id=$1", m.StreamID).Scan(&topology); err != nil {
		return nil, err
	}
	if topology != 0 {
		return nil, fmt.Errorf("durable matching requires an unsharded matching-owned stream")
	}
	var status, reason string
	var position int64
	if err = s.conn.QueryRow(ctx, `SELECT source_sequence,broker_sequence,status,reason,last_received FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&s.source, &position, &status, &reason, &s.last); err != nil {
		return nil, err
	}
	if status == "INVALID" {
		return nil, fmt.Errorf("persisted stream invalid: %s", reason)
	}
	s.position = uint64(position)
	var count int64
	if err = s.conn.QueryRow(ctx, "SELECT count(*) FROM mtbt.orders WHERE stream_id=$1 AND remaining_quantity>0", m.StreamID).Scan(&count); err != nil {
		return nil, err
	}
	if count > int64(maxActive) {
		return nil, fmt.Errorf("%d persisted active orders exceed max-active %d", count, maxActive)
	}
	// No priority field: stable ID tie-breaking on rebuild is intentional, not arrival-order recovery.
	rows, err := s.conn.Query(ctx, `SELECT order_id,token,side,price,remaining_quantity FROM mtbt.orders WHERE stream_id=$1 AND remaining_quantity>0 ORDER BY order_id`, m.StreamID)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var raw []byte
		var side string
		var o book.Order
		if err = rows.Scan(&raw, &o.Token, &side, &o.Price, &o.Quantity); err != nil {
			break
		}
		o.ID, err = feed.IDFromBytes(raw)
		if err != nil {
			break
		}
		if side != "B" && side != "S" {
			err = fmt.Errorf("invalid stored side")
			break
		}
		o.Side = feed.Side(side[0])
		if !m.TokenAllowed(o.Token) || !onTick(m, o.Token, o.Price) {
			err = fmt.Errorf("persisted order outside configured catalog")
			break
		}
		if err = engine.Restore(o); err != nil {
			break
		}
	}
	rowErr := rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if rowErr != nil {
		return nil, rowErr
	}
	success = true
	return s, nil
}

func onTick(m runconfig.Manifest, token, price int32) bool {
	for _, i := range m.Instruments {
		if i.Token == token {
			return price%i.TickSize == 0
		}
	}
	return false
}

func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		// Destroy the dedicated session, releasing its advisory lease even on failures.
		_ = s.conn.Conn().Close(ctx)
		cancel()
		s.conn.Release()
	}
	if s.db != nil {
		s.db.Close()
		if e := s.db.DiagnosticsError(); e != nil {
			return e
		}
	}
	return nil
}
func (s *Service) Connected(value bool) { s.mu.Lock(); defer s.mu.Unlock(); s.connected = value }
func (s *Service) CatchUpThrough(position uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.catchUp = position
}
func (s *Service) Position() uint64 { s.mu.Lock(); defer s.mu.Unlock(); return s.position }

func status(o *Order) {
	switch {
	case o.Remaining > 0 && o.Filled > 0:
		o.Status = "PARTIALLY_FILLED"
	case o.Remaining > 0:
		o.Status = "OPEN"
	case o.Filled > 0:
		o.Status = "FILLED"
	default:
		o.Status = "CANCELLED"
	}
}
func loadOrder(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, stream int16, id feed.OrderID, lock bool) (Order, bool, error) {
	o := Order{ID: id}
	var side string
	sql := `SELECT token,side,price,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status FROM mtbt.orders WHERE stream_id=$1 AND order_id=$2`
	if lock {
		sql += " FOR UPDATE"
	}
	err := q.QueryRow(ctx, sql, stream, id.Bytes()).Scan(&o.Token, &side, &o.Price, &o.Accepted, &o.Remaining, &o.Filled, &o.Cancelled, &o.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, false, nil
	}
	if err != nil {
		return o, false, err
	}
	if side != "B" && side != "S" {
		return o, false, fmt.Errorf("invalid stored side")
	}
	o.Side = feed.Side(side[0])
	return o, true, nil
}
func (s *Service) Order(ctx context.Context, id feed.OrderID) (Order, bool, error) {
	if id == 0 {
		return Order{}, false, fmt.Errorf("zero order ID")
	}
	if _, err := feed.IDFromFloat64(id.Float64()); err != nil {
		return Order{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Order{}, false, fmt.Errorf("closed")
	}
	return loadOrder(ctx, s.db.Pool(), s.manifest.StreamID, id, false)
}

func (s *Service) DiagnosticsError() error { return s.db.DiagnosticsError() }

func (s *Service) Process(ctx context.Context, raw []byte, position uint64, received time.Time) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if s.closed || s.failed != "" {
		return fmt.Errorf("matching unavailable; restart from PostgreSQL: %s", s.failed)
	}
	defer func() {
		if err != nil {
			s.failed = err.Error()
		}
	}()
	if position == 0 || position > math.MaxInt64 {
		return fmt.Errorf("invalid broker position")
	}
	if received.IsZero() || received.After(time.Now().Add(time.Minute)) {
		return fmt.Errorf("invalid receive timestamp")
	}
	tx, err := s.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if s.outcomes.Enabled {
		if _, err = tx.Exec(ctx, "SELECT set_config('mtbt.outcome_writer','v1',true)"); err != nil {
			return err
		}
	}
	var source int32
	var broker int64
	var sourceHash, brokerHash []byte
	var state, mode string
	err = tx.QueryRow(ctx, `SELECT source_sequence,broker_sequence,source_hash,broker_hash,status,owner_mode FROM mtbt.feed_state WHERE stream_id=$1 FOR UPDATE`, s.manifest.StreamID).Scan(&source, &broker, &sourceHash, &brokerHash, &state, &mode)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(raw)
	reject := func(reason string) error {
		_, e := tx.Exec(ctx, `UPDATE mtbt.feed_state SET status='INVALID',reason=$2 WHERE stream_id=$1`, s.manifest.StreamID, reason)
		if e != nil {
			return e
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
		return fmt.Errorf("invalid input: %s", reason)
	}
	if state == "INVALID" || mode != "matching" {
		return fmt.Errorf("stream ownership/validity changed")
	}
	if uint64(broker) != s.position || source != s.source {
		return fmt.Errorf("database checkpoint changed outside owning session")
	}
	if position <= uint64(broker) {
		if position == uint64(broker) && !bytes.Equal(hash[:], brokerHash) {
			return reject("conflicting broker redelivery")
		}
		return nil // immutable JetStream prefix already committed; no matching replay
	}
	if position != uint64(broker)+1 {
		return reject("broker sequence gap")
	}
	p, e := feed.Decode(raw)
	if e != nil {
		return reject(e.Error())
	}
	if p.Stream != s.manifest.StreamID {
		return reject("native stream mismatch")
	}
	nextSource := source
	nextHash := sourceHash
	apply := false
	if p.Type == feed.Heartbeat {
		if p.LastSequence != source {
			return reject("heartbeat continuity mismatch")
		}
	} else {
		if !s.manifest.TokenAllowed(p.Token) {
			return reject("unknown instrument")
		}
		if p.Type != feed.Cancel && !onTick(s.manifest, p.Token, p.Price) {
			return reject("off-tick price")
		}
		switch {
		case p.Sequence == source && source > 0:
			if !bytes.Equal(hash[:], sourceHash) {
				return reject("conflicting source duplicate")
			}
		case int64(p.Sequence) != int64(source)+1:
			return reject("source sequence gap/reset")
		default:
			apply = true
			nextSource = p.Sequence
			nextHash = hash[:]
		}
	}
	if apply && (p.Type == feed.New || p.Type == feed.Modify || p.Type == feed.Cancel) {
		if err = s.apply(ctx, tx, p, position); err != nil {
			return err
		}
	}
	// T is exchange observation only: local matching already adjusts its own quantities.
	// Z and T still advance atomic ingestion progress, preventing repeated side effects.
	state = "UNINITIALIZED"
	if nextSource > 0 {
		state = "VALID"
	}
	_, err = tx.Exec(ctx, `UPDATE mtbt.feed_state SET source_sequence=$2,source_hash=$3,broker_sequence=$4,broker_hash=$5,status=$6,last_received=GREATEST(last_received,$7) WHERE stream_id=$1`, s.manifest.StreamID, nextSource, nextHash, int64(position), hash[:], state, received.UTC())
	if err != nil {
		return err
	}
	if s.beforeCommit != nil {
		if err = s.beforeCommit(tx); err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit outcome may be uncertain; restart from database: %w", err)
	}
	s.source = nextSource
	s.position = position
	if s.last == nil || received.After(*s.last) {
		t := received.UTC()
		s.last = &t
	}
	return nil
}

func (s *Service) apply(ctx context.Context, tx pgx.Tx, p feed.Packet, position uint64) error {
	incoming, found, err := loadOrder(ctx, tx, s.manifest.StreamID, p.OrderID, true)
	if err != nil {
		return err
	}
	if found && (incoming.Token != p.Token || incoming.Side != p.Side) {
		return fmt.Errorf("order identity token/side changed")
	}
	pendingNew := s.token != 0 && p.Type == feed.New
	if pendingNew {
		if !found || incoming.Status != "PENDING" || incoming.Price != p.Price || incoming.Remaining != p.Quantity || incoming.Accepted != int64(p.Quantity) {
			return fmt.Errorf("committed pending admission missing or different")
		}
	} else if p.Type == feed.New && found {
		return fmt.Errorf("order identity reused, including terminal order")
	}
	if s.token != 0 && !pendingNew && (!found || incoming.Status == "PENDING") {
		return fmt.Errorf("command precedes matching admission")
	}
	if p.Type == feed.Cancel && (!found || incoming.Remaining == 0) {
		return nil // terminal history is not converted from FILLED into CANCELLED
	}
	if pendingNew {
		// Admission already counted this quantity. Never cancel/add it again.
	} else if p.Type != feed.Cancel {
		if !found {
			incoming = Order{ID: p.OrderID, Token: p.Token, Side: p.Side}
		}
		if incoming.Accepted > math.MaxInt64-int64(p.Quantity) {
			return fmt.Errorf("accepted quantity overflow")
		}
		incoming.Cancelled += int64(incoming.Remaining)
		incoming.Accepted += int64(p.Quantity)
		incoming.Remaining = p.Quantity
		incoming.Price = p.Price
	} else {
		incoming.Cancelled += int64(incoming.Remaining)
		incoming.Remaining = 0
	}
	result, err := s.engine.Apply(p)
	if err != nil {
		return err
	}
	changed := map[feed.OrderID]*Order{incoming.ID: &incoming}
	for _, fill := range result.Fills {
		for _, id := range []feed.OrderID{fill.BuyID, fill.SellID} {
			o := changed[id]
			if o == nil {
				stored, exists, e := loadOrder(ctx, tx, s.manifest.StreamID, id, true)
				if e != nil {
					return e
				}
				if !exists {
					return fmt.Errorf("matched resting order missing from database")
				}
				changed[id] = &stored
				o = &stored
			}
			if o.Remaining < fill.Quantity {
				return fmt.Errorf("database/RAM matching quantity divergence")
			}
			o.Remaining -= fill.Quantity
			o.Filled += int64(fill.Quantity)
		}
	}
	for id, o := range changed {
		// Cancellation and replacement semantics: cancelled remainder is not counted as a fill.
		status(o)
		if p.Type == feed.Cancel && id == incoming.ID {
			o.Status = "CANCELLED"
		}
		active, ok := s.engine.Get(id)
		if (o.Remaining > 0) != ok || (ok && (active.Quantity != o.Remaining || active.Token != o.Token || active.Side != o.Side || active.Price != o.Price)) {
			return fmt.Errorf("post-match database/RAM divergence")
		}
		if s.token != 0 {
			var tag pgconn.CommandTag
			tag, err = tx.Exec(ctx, `UPDATE mtbt.orders SET price=$5,accepted_quantity=$6,remaining_quantity=$7,filled_quantity=$8,cancelled_quantity=$9,status=$10 WHERE stream_id=$1 AND order_id=$2 AND token=$3 AND side=$4`, s.manifest.StreamID, o.ID.Bytes(), o.Token, string(byte(o.Side)), o.Price, o.Accepted, o.Remaining, o.Filled, o.Cancelled, o.Status)
			if err == nil && tag.RowsAffected() != 1 {
				err = fmt.Errorf("matching update lost admitted order")
			}
		} else {
			_, err = tx.Exec(ctx, `INSERT INTO mtbt.orders(stream_id,order_id,token,side,price,accepted_quantity,remaining_quantity,filled_quantity,cancelled_quantity,status)
   VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(stream_id,order_id) DO UPDATE SET price=EXCLUDED.price,accepted_quantity=EXCLUDED.accepted_quantity,remaining_quantity=EXCLUDED.remaining_quantity,filled_quantity=EXCLUDED.filled_quantity,cancelled_quantity=EXCLUDED.cancelled_quantity,status=EXCLUDED.status`, s.manifest.StreamID, o.ID.Bytes(), o.Token, string(byte(o.Side)), o.Price, o.Accepted, o.Remaining, o.Filled, o.Cancelled, o.Status)
		}
		if err != nil {
			return err
		}
	}
	return s.writeOutcomes(ctx, tx, p, position, changed)
}

func (s *Service) Snapshot(ctx context.Context, token int32, depth int, now time.Time, stale time.Duration) (Snapshot, error) {
	if !s.manifest.TokenAllowed(token) || depth < 1 || depth > 100 || stale <= 0 {
		return Snapshot{}, fmt.Errorf("invalid token/depth/freshness")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	levels, err := s.engine.Snapshot(token, depth)
	if err != nil {
		return Snapshot{}, err
	}
	out := Snapshot{StreamID: s.manifest.StreamID, Synthetic: true, Ephemeral: false, SourceSequence: s.source, BrokerSequence: s.position, LastReceived: s.last, Status: "READY", ActiveOrders: s.engine.ActiveOrders(), Depth: levels}
	switch {
	case s.failed != "" || s.closed:
		out.Status = "INVALID"
		out.Reason = s.failed
		out.Depth = matching.Depth{Bids: []matching.Level{}, Asks: []matching.Level{}}
	case !s.connected:
		out.Status = "DISCONNECTED"
	case s.source == 0 || s.position < s.catchUp:
		out.Status = "REBUILDING"
	case s.last == nil || now.Sub(*s.last) > stale || s.last.After(now.Add(time.Minute)):
		out.Status = "STALE"
	}
	return out, nil
}
