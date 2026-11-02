package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/sharding"
)

// CheckSerial prevents a serial process from reinterpreting routed source progress.
// Call under the stream lease before consuming anything.
func (s *Store) CheckSerial(ctx context.Context, m runconfig.Manifest) error {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM mtbt.subscriber_topology WHERE stream_id=$1)`, m.StreamID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("stream uses parallel subscribers; restart with its original shard count")
	}
	return nil
}

// InitParallel pins an immutable topology under the existing single coordinator lease.
// Fresh parallel runs are required; serial checkpoints cannot be upgraded in place.
func (s *Store) InitParallel(ctx context.Context, m runconfig.Manifest, count int) error {
	if err := sharding.ValidateCount(count); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var broker int64
	var status string
	if err = tx.QueryRow(ctx, `SELECT broker_sequence,status FROM mtbt.feed_state WHERE stream_id=$1 FOR UPDATE`, m.StreamID).Scan(&broker, &status); err != nil {
		return err
	}
	var old int
	var routing string
	err = tx.QueryRow(ctx, `SELECT shards,routing FROM mtbt.subscriber_topology WHERE stream_id=$1`, m.StreamID).Scan(&old, &routing)
	if err == nil {
		if old != count || routing != sharding.Version {
			return fmt.Errorf("existing subscriber topology differs; use original count or a fresh stream")
		}
		return tx.Commit(ctx)
	}
	if err != pgx.ErrNoRows {
		return err
	}
	if broker != 0 || status != "UNINITIALIZED" {
		return fmt.Errorf("parallel mode requires a fresh unprocessed stream")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO mtbt.subscriber_topology(stream_id,shards,routing) VALUES($1,$2,$3)`, m.StreamID, count, sharding.Version); err != nil {
		return err
	}
	for i := 0; i < count; i++ {
		if _, err = tx.Exec(ctx, `INSERT INTO mtbt.subscriber_shards(stream_id,shard) VALUES($1,$2)`, m.StreamID, i); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) parallelReject(ctx context.Context, tx pgx.Tx, m runconfig.Manifest, reason string) error {
	_ = tx.Rollback(ctx)
	if err := s.Invalidate(ctx, m, reason); err != nil {
		return fmt.Errorf("persist invalidation: %w", err)
	}
	return &fatalError{reason}
}

// Dispatch validates the raw stream in order and atomically checkpoints an outbox
// entry. It never applies book changes. Drain the outbox before fetching the next
// raw message, and acknowledge raw input only after its outbox publication succeeds.
func (s *Store) Dispatch(ctx context.Context, m runconfig.Manifest, count int, raw []byte, broker uint64, received time.Time) error {
	if err := sharding.ValidateCount(count); err != nil {
		return err
	}
	if broker == 0 || broker > math.MaxInt64 {
		return fmt.Errorf("invalid source broker sequence")
	}
	mh, err := m.Digest()
	if err != nil {
		return err
	}
	hash := sha256.Sum256(raw)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	reject := func(reason string) error { return s.parallelReject(ctx, tx, m, reason) }
	var status, stored, routing string
	var source int32
	var last int64
	var sourceHash, brokerHash []byte
	var pinned int
	err = tx.QueryRow(ctx, `SELECT f.status,f.manifest_hash,f.source_sequence,f.source_hash,f.broker_sequence,f.broker_hash,t.shards,t.routing FROM mtbt.feed_state f JOIN mtbt.subscriber_topology t USING(stream_id) WHERE f.stream_id=$1 FOR NO KEY UPDATE OF f`, m.StreamID).Scan(&status, &stored, &source, &sourceHash, &last, &brokerHash, &pinned, &routing)
	if err != nil {
		return err
	}
	if status == "INVALID" {
		return &fatalError{"stream is invalid; use a fresh stream"}
	}
	if stored != mh || pinned != count || routing != sharding.Version {
		return reject("dispatcher topology/manifest mismatch")
	}
	if int64(broker) <= last {
		if int64(broker) == last && !bytes.Equal(hash[:], brokerHash) {
			return reject("conflicting raw broker message identity")
		}
		return tx.Commit(ctx)
	}
	if int64(broker) != last+1 {
		return reject(fmt.Sprintf("raw broker sequence gap: expected %d got %d", last+1, broker))
	}
	if received.IsZero() {
		return reject("missing receive timestamp")
	}
	p, err := feed.Decode(raw)
	if err != nil {
		return reject("decode: " + err.Error())
	}
	if p.Stream != m.StreamID {
		return reject("unexpected source stream")
	}
	nextHash := sourceHash
	if p.Type == feed.Heartbeat {
		if p.LastSequence != source {
			return reject(fmt.Sprintf("heartbeat continuity mismatch: dispatched %d advertised %d", source, p.LastSequence))
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
			// Pin identities at the ordered boundary, including invisible trade references.
			// Tombstones prohibit cross-token ID reuse for the entire local stream.
			ids := []feed.OrderID{p.OrderID}
			if p.Type == feed.Trade {
				ids = []feed.OrderID{p.BuyID, p.SellID}
			}
			for _, id := range ids {
				if id == 0 {
					continue
				}
				var token int32
				err = tx.QueryRow(ctx, `INSERT INTO mtbt.subscriber_identities(stream_id,order_id,token) VALUES($1,$2,$3) ON CONFLICT(stream_id,order_id) DO UPDATE SET token=mtbt.subscriber_identities.token RETURNING token`, m.StreamID, id.Bytes(), p.Token).Scan(&token)
				if err != nil {
					return err
				}
				if token != p.Token {
					return reject("order identity moved across instruments")
				}
			}
			shard, _ := sharding.ForToken(p.Token, count)
			var seq int64
			if err = tx.QueryRow(ctx, `UPDATE mtbt.subscriber_shards SET dispatched=dispatched+1 WHERE stream_id=$1 AND shard=$2 RETURNING dispatched`, m.StreamID, shard).Scan(&seq); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `INSERT INTO mtbt.subscriber_outbox(stream_id,shard,sequence,source_broker,raw,received) VALUES($1,$2,$3,$4,$5,$6)`, m.StreamID, shard, seq, int64(broker), raw, received.UTC()); err != nil {
				return err
			}
			source = p.Sequence
			nextHash = hash[:]
			status = "VALID"
		}
	}
	_, err = tx.Exec(ctx, `UPDATE mtbt.feed_state SET source_sequence=$2,source_hash=$3,broker_sequence=$4,broker_hash=$5,status=$6,last_received=GREATEST(last_received,$7) WHERE stream_id=$1 AND status<>'INVALID'`, m.StreamID, source, nextHash, int64(broker), hash[:], status, received.UTC())
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) PendingDispatch(ctx context.Context, m runconfig.Manifest) ([]sharding.Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT shard,sequence,source_broker,raw,received FROM mtbt.subscriber_outbox WHERE stream_id=$1 ORDER BY source_broker,shard`, m.StreamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []sharding.Event
	for rows.Next() {
		var e sharding.Event
		if err = rows.Scan(&e.Shard, &e.Sequence, &e.SourceBroker, &e.Raw, &e.Received); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) ConfirmDispatch(ctx context.Context, m runconfig.Manifest, e sharding.Event) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM mtbt.subscriber_outbox WHERE stream_id=$1 AND shard=$2 AND sequence=$3`, m.StreamID, e.Shard, int64(e.Sequence))
	return err
}

func eventHash(e sharding.Event) [32]byte {
	h := sha256.New()
	var buf [8]byte
	for _, n := range []uint64{uint64(e.Shard), e.Sequence, e.SourceBroker, uint64(e.Received.UnixMicro())} {
		binary.LittleEndian.PutUint64(buf[:], n)
		h.Write(buf[:])
	}
	h.Write(e.Raw)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// ApplyShard changes the book and shard checkpoint atomically. It never locks the
// global checkpoint for update, so independent shard transactions can overlap.
func (s *Store) ApplyShard(ctx context.Context, m runconfig.Manifest, count int, e sharding.Event) error {
	if err := sharding.ValidateCount(count); err != nil {
		return err
	}
	if e.Shard < 0 || e.Shard >= count || e.Sequence == 0 || e.Sequence > math.MaxInt64 || e.SourceBroker == 0 || e.SourceBroker > math.MaxInt64 || e.Received.IsZero() {
		return fmt.Errorf("invalid shard event identity")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	reject := func(reason string) error { return s.parallelReject(ctx, tx, m, reason) }
	var status, routing string
	var pinned int
	if err = tx.QueryRow(ctx, `SELECT f.status,t.shards,t.routing FROM mtbt.feed_state f JOIN mtbt.subscriber_topology t USING(stream_id) WHERE f.stream_id=$1`, m.StreamID).Scan(&status, &pinned, &routing); err != nil {
		return err
	}
	if status == "INVALID" {
		return &fatalError{"stream is invalid; use a fresh stream"}
	}
	if pinned != count || routing != sharding.Version {
		return reject("worker topology mismatch")
	}
	var applied, dispatched, previousBroker int64
	var previousSource int32
	var previousHash []byte
	if err = tx.QueryRow(ctx, `SELECT applied,dispatched,applied_source,applied_source_broker,applied_hash FROM mtbt.subscriber_shards WHERE stream_id=$1 AND shard=$2 FOR UPDATE`, m.StreamID, e.Shard).Scan(&applied, &dispatched, &previousSource, &previousBroker, &previousHash); err != nil {
		return err
	}
	hash := eventHash(e)
	if int64(e.Sequence) <= applied {
		if int64(e.Sequence) == applied && !bytes.Equal(hash[:], previousHash) {
			return reject("conflicting shard redelivery")
		}
		return tx.Commit(ctx)
	}
	if int64(e.Sequence) != applied+1 || int64(e.Sequence) > dispatched {
		return reject("shard sequence gap or undispatched event")
	}
	p, err := feed.Decode(e.Raw)
	if err != nil {
		return reject("shard decode: " + err.Error())
	}
	shard, routeErr := sharding.ForToken(p.Token, count)
	if routeErr != nil || p.Type == feed.Heartbeat || p.Stream != m.StreamID || !m.TokenAllowed(p.Token) || shard != e.Shard {
		return reject("shard routing mismatch")
	}
	if p.Sequence <= previousSource || int64(e.SourceBroker) <= previousBroker {
		return reject("non-increasing shard source provenance")
	}
	if err = book.Apply(ctx, txBook{tx: tx, stream: m.StreamID}, p); err != nil {
		if book.IsInvariant(err) {
			return reject(err.Error())
		}
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE mtbt.subscriber_shards SET applied=$2,applied_source=$3,applied_source_broker=$4,applied_hash=$5 WHERE stream_id=$1 AND shard=$6`, m.StreamID, int64(e.Sequence), p.Sequence, int64(e.SourceBroker), hash[:], e.Shard)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
