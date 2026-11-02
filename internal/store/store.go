// Package store owns the database-authoritative book and its atomic checkpoint.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/querytrace"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/sqldiagnostics"
)

//go:embed schema.sql
var schema string

//go:embed matching.sql
var matchingSchema string

//go:embed outcomes.sql
var outcomesSchema string

//go:embed ticker.sql
var tickerSchema string

// Pool is shared with the optional durable matching transaction coordinator.
// Callers must hold the native-stream lease and must not close this pool separately.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

//go:embed parallel.sql
var parallelSchema string

type Store struct {
	pool        *pgxpool.Pool
	diagnostics *sqldiagnostics.Recorder
}

type fatalError struct{ reason string }

func (e *fatalError) Error() string { return e.reason }
func IsFatal(err error) bool        { var e *fatalError; return errors.As(err, &e) }

func Open(ctx context.Context, dsn string) (*Store, error) {
	return OpenWithPool(ctx, dsn, 6)
}

func OpenWithPool(ctx context.Context, dsn string, max int32) (*Store, error) {
	// Check for optional MTBT_SQL_METRICS environment variable.
	sqlMetricsPath := os.Getenv("MTBT_SQL_METRICS")
	return openWithPoolInternal(ctx, dsn, max, sqlMetricsPath)
}

// openWithPoolInternal is the internal helper that accepts optional sqlMetricsPath.
func openWithPoolInternal(ctx context.Context, dsn string, max int32, sqlMetricsPath string) (*Store, error) {
	if max < 3 || max > 32 {
		return nil, fmt.Errorf("pool size must be between 3 and 32, got %d", max)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = int32(max)

	// Initialize SQL diagnostics recorder if enabled via env.
	var recorder *sqldiagnostics.Recorder
	if sqlMetricsPath != "" {
		recorder, err = sqldiagnostics.NewRecorder(sqlMetricsPath)
		if err != nil {
			return nil, fmt.Errorf("init sql diagnostics: %w", err)
		}
		// Attach QueryTracer to the pool config.
		config.ConnConfig.Tracer = sqldiagnostics.NewQueryTracer(recorder)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		if recorder != nil {
			recorder.SetLastError(err)
			_ = recorder.Close()
		}
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		if recorder != nil {
			recorder.SetLastError(err)
			_ = recorder.Close()
		}
		return nil, err
	}
	return &Store{pool: pool, diagnostics: recorder}, nil
}

func (s *Store) Close() {
	// Close pool first, then diagnostics recorder.
	s.pool.Close()
	if s.diagnostics != nil {
		if e := s.diagnostics.Close(); e != nil {
			log.Printf("SQL diagnostics close failed: %v", e)
		}
	}
}

// DiagnosticsError returns any error encountered during SQL diagnostics recording.
// Called by the parent event loop to check for diagnostics-related issues.
// Returns nil if diagnostics are not enabled or no error occurred.
func (s *Store) DiagnosticsError() error {
	if s == nil || s.diagnostics == nil {
		return nil
	}
	return s.diagnostics.LastError()
}

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(19462471)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema+"\n"+parallelSchema+"\n"+matchingSchema+"\n"+outcomesSchema+"\n"+tickerSchema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Init(ctx context.Context, m runconfig.Manifest) error {
	return s.InitMode(ctx, m, "reconstruction")
}

// InitMode binds a native stream to one authoritative interpretation, without conversion.
func (s *Store) InitMode(ctx context.Context, m runconfig.Manifest, mode string) error {
	if mode != "reconstruction" && mode != "matching" {
		return fmt.Errorf("unknown owner mode")
	}
	if err := m.Validate(); err != nil {
		return err
	}
	h, err := m.Digest()
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO mtbt.feed_state(stream_id,manifest_hash,owner_mode) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, m.StreamID, h, mode)
	if err != nil {
		return err
	}
	var existing, owner string
	if err = s.pool.QueryRow(ctx, `SELECT manifest_hash,owner_mode FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&existing, &owner); err != nil {
		return err
	}
	if existing != h {
		return &fatalError{"stream_id is already bound to a different manifest"}
	}
	if owner != mode {
		return &fatalError{fmt.Sprintf("native stream belongs to %s, not %s; automatic conversion is forbidden", owner, mode)}
	}
	return nil
}

func (s *Store) AcquireLease(ctx context.Context, m runconfig.Manifest) (func(), error) {
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256([]byte(fmt.Sprint("mtbt-stream:", m.StreamID)))
	key := int64(binary.LittleEndian.Uint64(h[:8]))
	var ok bool
	if err = c.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&ok); err != nil || !ok {
		c.Release()
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("another consumer owns this stream")
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if _, err := c.Exec(cleanup, `SELECT pg_advisory_unlock($1)`, key); err != nil {
				_ = c.Conn().Close(cleanup)
			}
			c.Release()
		})
	}, nil
}

func (s *Store) Invalidate(ctx context.Context, m runconfig.Manifest, reason string) error {
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	tag, err := s.pool.Exec(ctx, `UPDATE mtbt.feed_state SET status='INVALID',reason=$2 WHERE stream_id=$1`, m.StreamID, reason)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("stream not initialized")
	}
	return nil
}

// Process assumes one per-stream JetStream stream/subject and serial consumption. Broker progress
// allows old committed redeliveries after a lost ack; new backward SOURCE positions fail closed.
func (s *Store) Process(ctx context.Context, m runconfig.Manifest, raw []byte, brokerSequence uint64, received time.Time) error {
	if brokerSequence == 0 || brokerSequence > math.MaxInt64 {
		return fmt.Errorf("invalid broker sequence")
	}
	hash := sha256.Sum256(raw)
	mh, err := m.Digest()
	if err != nil {
		return err
	}
	beginSpan := querytrace.Start(ctx, "begin")
	tx, err := s.pool.Begin(ctx)
	beginSpan.End(err == nil)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	var status, storedManifest string
	var source int32
	var brokerSeq int64
	var sourceHash, brokerHash []byte
	stateReadSpan := querytrace.Start(ctx, "checkpoint_select_for_update")
	err = tx.QueryRow(ctx, `SELECT status,manifest_hash,source_sequence,source_hash,broker_sequence,broker_hash FROM mtbt.feed_state WHERE stream_id=$1 FOR UPDATE`, m.StreamID).Scan(&status, &storedManifest, &source, &sourceHash, &brokerSeq, &brokerHash)
	stateReadSpan.End(err == nil)
	if err != nil {
		return err
	}
	reject := func(reason string) error {
		_ = tx.Rollback(ctx)
		if e := s.Invalidate(ctx, m, reason); e != nil {
			return fmt.Errorf("persist invalidation: %w", e)
		}
		return &fatalError{reason}
	}
	if storedManifest != mh {
		return reject("manifest digest mismatch")
	}
	if status == "INVALID" {
		return &fatalError{"stream is invalid; initialize a fresh stream"}
	}
	if int64(brokerSequence) <= brokerSeq {
		if int64(brokerSequence) == brokerSeq && !bytes.Equal(hash[:], brokerHash) {
			return reject("conflicting broker message identity")
		}
		commitSpan := querytrace.Start(ctx, "commit")
		err = tx.Commit(ctx)
		commitSpan.End(err == nil)
		return err
	}
	// New broker messages must follow a contiguous stream, so expired unprocessed backlog is detected.
	if int64(brokerSequence) != brokerSeq+1 {
		return reject(fmt.Sprintf("broker sequence gap: expected %d got %d", brokerSeq+1, brokerSequence))
	}
	p, err := feed.Decode(raw)
	if err != nil {
		return reject("decode: " + err.Error())
	}
	if p.Stream != m.StreamID {
		return reject("unexpected source stream")
	}
	nextSource, nextHash := source, sourceHash
	if p.Type == feed.Heartbeat {
		if p.LastSequence != source {
			return reject(fmt.Sprintf("heartbeat continuity mismatch: committed %d advertised %d", source, p.LastSequence))
		}
	} else {
		if !m.TokenAllowed(p.Token) {
			return reject("unknown instrument token")
		}
		if p.Sequence == source {
			if !bytes.Equal(hash[:], sourceHash) {
				return reject("conflicting source duplicate")
			}
		} else {
			if int64(p.Sequence) != int64(source)+1 {
				return reject(fmt.Sprintf("source sequence gap/reset: expected %d got %d", int64(source)+1, p.Sequence))
			}
			for _, inst := range m.Instruments {
				if inst.Token == p.Token && p.Type != feed.Cancel && p.Price%inst.TickSize != 0 {
					return reject("price is not on configured instrument tick")
				}
			}
			view := txBook{tx: tx, stream: m.StreamID}
			if err = book.Apply(ctx, view, p); err != nil {
				if book.IsInvariant(err) {
					return reject(err.Error())
				}
				return err
			}
			nextSource = p.Sequence
			nextHash = hash[:]
			status = "VALID"
		}
	}
	if received.IsZero() {
		return reject("missing receive timestamp")
	}
	stateWriteSpan := querytrace.Start(ctx, "checkpoint_update")
	_, err = tx.Exec(ctx, `UPDATE mtbt.feed_state SET source_sequence=$2,source_hash=$3,broker_sequence=$4,broker_hash=$5,status=$6,last_received=GREATEST(last_received,$7) WHERE stream_id=$1`, m.StreamID, nextSource, nextHash, int64(brokerSequence), hash[:], status, received.UTC())
	stateWriteSpan.End(err == nil)
	if err != nil {
		return err
	}
	commitSpan := querytrace.Start(ctx, "commit")
	err = tx.Commit(ctx)
	commitSpan.End(err == nil)
	return err
}

type txBook struct {
	tx     pgx.Tx
	stream int16
}

func (t txBook) Get(ctx context.Context, id feed.OrderID) (book.Order, bool, error) {
	var o book.Order
	var side string
	span := querytrace.Start(ctx, "order_select")
	err := t.tx.QueryRow(ctx, `SELECT token,side,price,quantity FROM mtbt.active_orders WHERE stream_id=$1 AND order_id=$2`, t.stream, id.Bytes()).Scan(&o.Token, &side, &o.Price, &o.Quantity)
	span.End(err == nil || errors.Is(err, pgx.ErrNoRows))
	if errors.Is(err, pgx.ErrNoRows) {
		return book.Order{}, false, nil
	}
	if err != nil {
		return book.Order{}, false, err
	}
	if side != "B" && side != "S" {
		return book.Order{}, false, fmt.Errorf("invalid stored side")
	}
	o.ID, o.Side = id, feed.Side(side[0])
	return o, true, nil
}

func (t txBook) Put(ctx context.Context, o book.Order) error {
	span := querytrace.Start(ctx, "order_upsert")
	_, err := t.tx.Exec(ctx, `INSERT INTO mtbt.active_orders(stream_id,order_id,token,side,price,quantity) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(stream_id,order_id) DO UPDATE SET token=$3,side=$4,price=$5,quantity=$6`, t.stream, o.ID.Bytes(), o.Token, string(byte(o.Side)), o.Price, o.Quantity)
	span.End(err == nil)
	return err
}

func (t txBook) Delete(ctx context.Context, id feed.OrderID) error {
	span := querytrace.Start(ctx, "order_delete")
	_, err := t.tx.Exec(ctx, `DELETE FROM mtbt.active_orders WHERE stream_id=$1 AND order_id=$2`, t.stream, id.Bytes())
	span.End(err == nil)
	return err
}

type Level struct {
	Price    int32 `json:"price"`
	Quantity int64 `json:"quantity"`
	Orders   int64 `json:"orders"`
}

type Snapshot struct {
	StreamID           int16      `json:"stream_id"`
	Sequence           int32      `json:"sequence"`
	Status             string     `json:"status"`
	Reason             string     `json:"reason"`
	LastReceived       *time.Time `json:"last_received"`
	DispatchedSequence int32      `json:"dispatched_sequence,omitempty"`
	PendingEvents      int64      `json:"pending_events,omitempty"`
	Shards             int        `json:"shards,omitempty"`
	Bids               []Level    `json:"bids"`
	Asks               []Level    `json:"asks"`
}

func (s *Store) Snapshot(ctx context.Context, m runconfig.Manifest, token int32, depth int) (Snapshot, error) {
	out := Snapshot{StreamID: m.StreamID, Bids: []Level{}, Asks: []Level{}}
	if !m.TokenAllowed(token) || depth < 1 || depth > 100 {
		return out, fmt.Errorf("unknown token or depth outside 1..100")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	if err = tx.QueryRow(ctx, `SELECT source_sequence,status,reason,last_received FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&out.Sequence, &out.Status, &out.Reason, &out.LastReceived); err != nil {
		return out, err
	}
	// Parallel source_sequence means dispatched, not globally applied. Only label
	// the book current when every shard in this repeatable-read snapshot caught up.
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(t.shards),0),COALESCE(sum(s.dispatched-s.applied),0) FROM mtbt.subscriber_topology t JOIN mtbt.subscriber_shards s USING(stream_id) WHERE t.stream_id=$1`, m.StreamID).Scan(&out.Shards, &out.PendingEvents); err != nil {
		return out, err
	}
	if out.Shards > 0 {
		out.DispatchedSequence = out.Sequence
		if out.PendingEvents > 0 {
			out.Sequence = 0 // No single applied global prefix is claimed during catch-up.
			if out.Status != "INVALID" {
				out.Status = "CATCHING_UP"
				out.Reason = "parallel workers have pending events; no globally current prefix claimed"
			}
		}
	}
	for _, side := range []feed.Side{feed.Buy, feed.Sell} {
		direction := "ASC"
		if side == feed.Buy {
			direction = "DESC"
		}
		rows, e := tx.Query(ctx, `SELECT price,sum(quantity)::bigint,count(*) FROM mtbt.active_orders WHERE stream_id=$1 AND token=$2 AND side=$3 GROUP BY price ORDER BY price `+direction+` LIMIT $4`, m.StreamID, token, string(byte(side)), depth)
		if e != nil {
			return out, e
		}
		for rows.Next() {
			var l Level
			if e = rows.Scan(&l.Price, &l.Quantity, &l.Orders); e != nil {
				rows.Close()
				return out, e
			}
			if side == feed.Buy {
				out.Bids = append(out.Bids, l)
			} else {
				out.Asks = append(out.Asks, l)
			}
		}
		rows.Close()
		if e = rows.Err(); e != nil {
			return out, e
		}
	}
	return out, tx.Commit(ctx)
}
