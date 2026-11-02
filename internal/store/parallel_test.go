package store

import (
	"context"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/sharding"
)

// parallelSetup initializes a fresh store with a clean manifest for parallel testing.
// Unlike setup(), this creates a fresh StreamID and initializes for parallel mode.
func parallelSetup(t *testing.T, shardCount int) (*Store, runconfig.Manifest) {
	t.Helper()
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set MTBT_TEST_DATABASE_URL for PostgreSQL integration")
	}
	ctx := context.Background()
	s, e := Open(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	if e = s.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	m := runconfig.Manifest{
		Version:   2,
		Profile:   feed.Profile,
		StreamID:  1,
		Seed:      42,
		Baseline:  "empty",
		CreatedAt: time.Now().UTC(),
		Instruments: []runconfig.Instrument{
			{Token: 1, Symbol: "SIM1", TickSize: 5, BasePrice: 100},
			{Token: 2, Symbol: "SIM2", TickSize: 5, BasePrice: 200},
			{Token: 3, Symbol: "SIM3", TickSize: 5, BasePrice: 300},
		},
	}
	if e = s.Init(ctx, m); e != nil {
		t.Fatal(e)
	}
	if e = s.InitParallel(ctx, m, shardCount); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID)
	})
	return s, m
}

// Helper to create an order packet
func orderPkt(seq int32, orderID float64, token int32, side feed.Side, price int32) feed.Packet {
	oid, _ := feed.IDFromFloat64(orderID)
	return feed.Packet{
		Stream:    1,
		Sequence:  seq,
		Type:      feed.New,
		Timestamp: 1,
		OrderID:   oid,
		Token:     token,
		Side:      side,
		Price:     price,
		Quantity:  10,
	}
}

// Helper to create a trade packet
func tradePkt(seq int32, buyID, sellID float64, token int32, price int32) feed.Packet {
	bid, _ := feed.IDFromFloat64(buyID)
	sid, _ := feed.IDFromFloat64(sellID)
	return feed.Packet{
		Stream:    1,
		Sequence:  seq,
		Type:      feed.Trade,
		Timestamp: 1,
		BuyID:     bid,
		SellID:    sid,
		Token:     token,
		Price:     price,
		Quantity:  5,
	}
}

// Helper to create a heartbeat packet
func heartbeatPkt(lastSeq int32) feed.Packet {
	return feed.Packet{
		Stream:       1,
		Type:         feed.Heartbeat,
		LastSequence: lastSeq,
	}
}

// Helper to dispatch a packet
func dispatchPkt(t *testing.T, s *Store, m runconfig.Manifest, count int, p feed.Packet, broker uint64) error {
	t.Helper()
	raw, e := feed.Encode(p)
	if e != nil {
		t.Fatal(e)
	}
	return s.Dispatch(context.Background(), m, count, raw, broker, time.Now().UTC())
}

// TestParallelTopologyPin verifies that topology is immutable after initialization.
func TestParallelTopologyPin(t *testing.T) {
	s, m := parallelSetup(t, 4)
	ctx := context.Background()

	// Re-init with same count should succeed (idempotent)
	if e := s.InitParallel(ctx, m, 4); e != nil {
		t.Fatal("same count re-init failed:", e)
	}

	// Different count should reject
	if e := s.InitParallel(ctx, m, 3); e == nil {
		t.Fatal("different count re-init should fail")
	}

	// Verify topology is stored
	var shards, routing string
	if e := s.pool.QueryRow(ctx, `SELECT shards,routing FROM mtbt.subscriber_topology WHERE stream_id=$1`, m.StreamID).Scan(&shards, &routing); e != nil {
		t.Fatal(e)
	}
	if shards != "4" || routing != sharding.Version {
		t.Fatalf("unexpected topology: %s shards, %s routing", shards, routing)
	}
}

// TestParallelDispatchGapDetection verifies broker sequence gap validation.
func TestParallelDispatchGapDetection(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Valid first dispatch
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal("first dispatch failed:", e)
	}

	// Gap in broker sequence should reject
	p2 := orderPkt(2, 101, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p2, 3); !IsFatal(e) {
		t.Fatal("broker gap should fail closed, got:", e)
	}

	// Verify run is INVALID
	var status string
	if e := s.pool.QueryRow(ctx, `SELECT status FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&status); e != nil {
		t.Fatal(e)
	}
	if status != "INVALID" {
		t.Fatalf("expected INVALID, got %s", status)
	}
}

// TestParallelDispatchDuplicateDetection verifies conflicting duplicates fail closed.
func TestParallelDispatchDuplicateDetection(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	raw1, _ := feed.Encode(p1)

	// Dispatch at broker 1
	if e := s.Dispatch(ctx, m, 2, raw1, 1, time.Now().UTC()); e != nil {
		t.Fatal("first dispatch failed:", e)
	}

	// Same broker, same raw → idempotent
	if e := s.Dispatch(ctx, m, 2, raw1, 1, time.Now().UTC()); e != nil {
		t.Fatal("redelivery of same raw should be idempotent, got:", e)
	}

	// Same broker, conflicting raw → reject
	p1_conflict := orderPkt(1, 100, 1, feed.Buy, 105) // different price
	raw1_conflict, _ := feed.Encode(p1_conflict)
	if e := s.Dispatch(ctx, m, 2, raw1_conflict, 1, time.Now().UTC()); !IsFatal(e) {
		t.Fatal("conflicting broker duplicate should fail closed, got:", e)
	}

	// Verify run is INVALID
	var status string
	if e := s.pool.QueryRow(ctx, `SELECT status FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&status); e != nil {
		t.Fatal(e)
	}
	if status != "INVALID" {
		t.Fatalf("expected INVALID after conflict, got %s", status)
	}
}

// TestParallelSourceSequenceGaps verifies source sequence continuity.
func TestParallelSourceSequenceGaps(t *testing.T) {
	s, m := parallelSetup(t, 2)

	// Valid first source sequence
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}

	// Gap in source sequence should reject
	p2 := orderPkt(3, 101, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p2, 2); !IsFatal(e) {
		t.Fatal("source sequence gap should fail closed")
	}
}

// TestParallelSourceDuplicateIdentityConflict verifies cross-token identity rejection.
func TestParallelSourceDuplicateIdentityConflict(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch order on token 1
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}

	// Same identity on different token should reject
	p2 := orderPkt(2, 100, 2, feed.Buy, 200) // same orderID, different token
	if e := dispatchPkt(t, s, m, 2, p2, 2); !IsFatal(e) {
		t.Fatal("cross-token identity reuse should fail closed")
	}

	// Verify run is INVALID
	var status string
	if e := s.pool.QueryRow(ctx, `SELECT status FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&status); e != nil {
		t.Fatal(e)
	}
	if status != "INVALID" {
		t.Fatalf("expected INVALID, got %s", status)
	}
}

// TestParallelTradeIdentitiesPin verifies trade buy/sell identity tracking.
func TestParallelTradeIdentitiesPin(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Create two orders
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}
	p2 := orderPkt(2, 101, 1, feed.Sell, 100)
	if e := dispatchPkt(t, s, m, 2, p2, 2); e != nil {
		t.Fatal(e)
	}

	// Trade between them should succeed
	p3 := tradePkt(3, 100, 101, 1, 100)
	if e := dispatchPkt(t, s, m, 2, p3, 3); e != nil {
		t.Fatal("valid trade failed:", e)
	}

	// Verify identities are pinned
	var token1, token2 int32
	oid1 := orderPkt(1, 100, 1, feed.Buy, 100).OrderID
	oid2 := orderPkt(2, 101, 1, feed.Sell, 100).OrderID
	if e := s.pool.QueryRow(ctx, `SELECT token FROM mtbt.subscriber_identities WHERE stream_id=$1 AND order_id=$2`, m.StreamID, oid1.Bytes()).Scan(&token1); e != nil {
		t.Fatal(e)
	}
	if e := s.pool.QueryRow(ctx, `SELECT token FROM mtbt.subscriber_identities WHERE stream_id=$1 AND order_id=$2`, m.StreamID, oid2.Bytes()).Scan(&token2); e != nil {
		t.Fatal(e)
	}
	if token1 != 1 || token2 != 1 {
		t.Fatalf("identities not pinned correctly: %d, %d", token1, token2)
	}
}

// TestParallelOutboxShardRouting verifies outbox entries route to correct shard.
func TestParallelOutboxShardRouting(t *testing.T) {
	s, m := parallelSetup(t, 3)
	ctx := context.Background()

	// Dispatch packets to different tokens (will route to different shards)
	packets := []struct {
		token   int32
		orderID float64
		broker  uint64
	}{
		{1, 100, 1},
		{2, 200, 2},
		{3, 300, 3},
	}

	for _, pkg := range packets {
		p := orderPkt(int32(pkg.broker), pkg.orderID, pkg.token, feed.Buy, 100*pkg.token)
		if e := dispatchPkt(t, s, m, 3, p, pkg.broker); e != nil {
			t.Fatalf("dispatch token %d failed: %v", pkg.token, e)
		}
	}

	// Verify outbox entries and their shard assignments
	rows, e := s.pool.Query(ctx, `SELECT shard,sequence,source_broker FROM mtbt.subscriber_outbox WHERE stream_id=$1 ORDER BY source_broker`, m.StreamID)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()

	type outboxEntry struct {
		shard        int
		sequence     int64
		sourceBroker int64
	}
	var entries []outboxEntry
	for rows.Next() {
		var e outboxEntry
		if err := rows.Scan(&e.shard, &e.sequence, &e.sourceBroker); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 outbox entries, got %d", len(entries))
	}

	// Verify each entry's shard matches token routing
	expectedShards := map[int64]int{
		1: 1 % 3, // token 1 → shard 1
		2: 2 % 3, // token 2 → shard 2
		3: 3 % 3, // token 3 → shard 0
	}
	for _, entry := range entries {
		expectedShard := expectedShards[entry.sourceBroker]
		if entry.shard != expectedShard {
			t.Fatalf("broker %d: expected shard %d, got %d", entry.sourceBroker, expectedShard, entry.shard)
		}
	}
}

// TestParallelPendingDispatch verifies outbox drainage.
func TestParallelPendingDispatch(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch a few packets
	for i := 1; i <= 3; i++ {
		p := orderPkt(int32(i), float64(100+i), int32(i%2+1), feed.Buy, 100)
		if e := dispatchPkt(t, s, m, 2, p, uint64(i)); e != nil {
			t.Fatal(e)
		}
	}

	// Get pending events
	events, e := s.PendingDispatch(ctx, m)
	if e != nil {
		t.Fatal(e)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 pending events, got %d", len(events))
	}

	// Verify events are in source_broker order
	for i, ev := range events {
		if ev.SourceBroker != uint64(i+1) {
			t.Fatalf("event %d: expected source_broker %d, got %d", i, i+1, ev.SourceBroker)
		}
	}

	// Confirm first event
	if e := s.ConfirmDispatch(ctx, m, events[0]); e != nil {
		t.Fatal(e)
	}

	// Verify it's gone from outbox
	events, e = s.PendingDispatch(ctx, m)
	if e != nil {
		t.Fatal(e)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 pending events after confirm, got %d", len(events))
	}
}

// TestParallelApplyShardSequenceValidation verifies sequence continuity in ApplyShard.
func TestParallelApplyShardSequenceValidation(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch two packets to same shard
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}
	p2 := orderPkt(2, 101, 1, feed.Sell, 100)
	if e := dispatchPkt(t, s, m, 2, p2, 2); e != nil {
		t.Fatal(e)
	}

	// Get pending events
	events, e := s.PendingDispatch(ctx, m)
	if e != nil {
		t.Fatal(e)
	}

	// Find events on same shard
	var shardEvents []sharding.Event
	shardNum := -1
	for _, ev := range events {
		if shardNum == -1 {
			shardNum = ev.Shard
			shardEvents = append(shardEvents, ev)
		} else if ev.Shard == shardNum {
			shardEvents = append(shardEvents, ev)
		}
	}

	if len(shardEvents) < 2 {
		t.Skip("test needs multiple events on same shard")
	}

	// Apply first event
	if e := s.ApplyShard(ctx, m, 2, shardEvents[0]); e != nil {
		t.Fatal("first ApplyShard failed:", e)
	}

	// Apply second event should succeed
	if e := s.ApplyShard(ctx, m, 2, shardEvents[1]); e != nil {
		t.Fatal("second ApplyShard failed:", e)
	}

	// Attempting to apply out of sequence is a safe no-op (already applied)
	if e := s.ApplyShard(ctx, m, 2, shardEvents[0]); e != nil {
		t.Fatal("out-of-sequence reapply should be safe no-op:", e)
	}

	// Test independent future-gap case: applying event with gap should fail
	if len(shardEvents) > 2 {
		gapEvent := shardEvents[2]
		// Manually skip sequence to create gap
		if e := s.ApplyShard(ctx, m, 2, gapEvent); e == nil {
			t.Fatal("applying with sequence gap should fail")
		}
	}
}

// TestParallelApplyShardRedeliveryIdempotence verifies idempotent redelivery.
func TestParallelApplyShardRedeliveryIdempotence(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch a packet
	p := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p, 1); e != nil {
		t.Fatal(e)
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil || len(events) == 0 {
		t.Fatal(e)
	}

	// Apply the event
	if e := s.ApplyShard(ctx, m, 2, events[0]); e != nil {
		t.Fatal(e)
	}

	// Re-apply same event should be idempotent
	if e := s.ApplyShard(ctx, m, 2, events[0]); e != nil {
		t.Fatal("redelivery should be idempotent:", e)
	}

	// Verify shard checkpoint hasn't changed
	var applied int64
	if e := s.pool.QueryRow(ctx, `SELECT applied FROM mtbt.subscriber_shards WHERE stream_id=$1 AND shard=$2`, m.StreamID, events[0].Shard).Scan(&applied); e != nil {
		t.Fatal(e)
	}
	if applied != int64(events[0].Sequence) {
		t.Fatalf("expected applied=%d, got %d", events[0].Sequence, applied)
	}
}

// TestParallelApplyShardConflictingRedelivery verifies conflicting redelivery fails closed.
func TestParallelApplyShardConflictingRedelivery(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch a packet
	p := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p, 1); e != nil {
		t.Fatal(e)
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil || len(events) == 0 {
		t.Fatal(e)
	}
	ev1 := events[0]

	// Apply the event
	if e := s.ApplyShard(ctx, m, 2, ev1); e != nil {
		t.Fatal(e)
	}

	// Create a conflicting event (same sequence, different raw)
	p_conflict := orderPkt(1, 100, 1, feed.Buy, 105)
	raw_conflict, _ := feed.Encode(p_conflict)
	ev_conflict := sharding.Event{
		Shard:        ev1.Shard,
		Sequence:     ev1.Sequence,
		SourceBroker: ev1.SourceBroker,
		Raw:          raw_conflict,
		Received:     ev1.Received,
	}

	// Redelivering with conflicting raw should fail closed
	if e := s.ApplyShard(ctx, m, 2, ev_conflict); !IsFatal(e) {
		t.Fatal("conflicting redelivery should fail closed")
	}

	// Verify run is INVALID
	var status string
	if e := s.pool.QueryRow(ctx, `SELECT status FROM mtbt.feed_state WHERE stream_id=$1`, m.StreamID).Scan(&status); e != nil {
		t.Fatal(e)
	}
	if status != "INVALID" {
		t.Fatalf("expected INVALID after conflict, got %s", status)
	}
}

// TestParallelIndependentShardProgress verifies independent shard lock handling.
func TestParallelIndependentShardProgress(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch packets to different shards
	p1 := orderPkt(1, 100, 1, feed.Buy, 100) // token 1 → shard 1%2=1
	p2 := orderPkt(2, 200, 2, feed.Buy, 200) // token 2 → shard 2%2=0
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}
	if e := dispatchPkt(t, s, m, 2, p2, 2); e != nil {
		t.Fatal(e)
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil || len(events) != 2 {
		t.Fatal(e)
	}

	// Partition events by shard
	var shardEvents [2][]sharding.Event
	for _, ev := range events {
		shardEvents[ev.Shard] = append(shardEvents[ev.Shard], ev)
	}

	// Hold lock on shard 0, demonstrate shard 1 can still apply independently
	holdLock := make(chan struct{})
	lockAcquired := make(chan struct{})
	applyDone := make(chan error, 1)
	shard0BlockedAttempt := make(chan error, 1)

	go func() {
		// Hold lock on shard 0 in a transaction
		tx, e := s.pool.Begin(ctx)
		if e != nil {
			lockAcquired <- struct{}{}
			return
		}
		defer tx.Rollback(ctx)
		_, e = tx.Exec(ctx, `SELECT 1 FROM mtbt.subscriber_shards WHERE stream_id=$1 AND shard=$2 FOR UPDATE`, m.StreamID, 0)
		if e != nil {
			return
		}
		lockAcquired <- struct{}{}
		<-holdLock
	}()

	<-lockAcquired

	// Attempt to apply to shard 0 (will block since lock is held)
	go func() {
		if len(shardEvents[0]) > 0 {
			shard0BlockedAttempt <- s.ApplyShard(ctx, m, 2, shardEvents[0][0])
		} else {
			shard0BlockedAttempt <- nil
		}
	}()

	// Try to apply to independent shard 1 (should not block)
	go func() {
		if len(shardEvents[1]) > 0 {
			applyDone <- s.ApplyShard(ctx, m, 2, shardEvents[1][0])
		} else {
			applyDone <- nil
		}
	}()

	// Keep the shard-0 lock held until independent shard 1 has COMMITTED.
	// The deferred close also releases the lock if this assertion fails.
	defer close(holdLock)
	select {
	case err := <-shard0BlockedAttempt:
		t.Fatalf("locked shard completed early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Shard 1 must finish BEFORE releasing the shard-0 lock.
	select {
	case e := <-applyDone:
		if len(shardEvents[1]) > 0 && e != nil {
			t.Fatal("ApplyShard on independent shard should complete:", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ApplyShard blocked waiting for independent shard lock")
	}

	holdLock <- struct{}{}
	// Shard 0 should eventually complete now that lock is released
	select {
	case e := <-shard0BlockedAttempt:
		if len(shardEvents[0]) > 0 && e != nil {
			t.Fatal("ApplyShard on locked shard should eventually complete:", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ApplyShard on shard 0 never completed after lock released")
	}
}

// TestParallelApplyShardStateTransition verifies shard state from CATCHING_UP to VALID.
func TestParallelApplyShardStateTransition(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch packets to fill initial outbox
	packets := []struct {
		seq   int32
		token int32
	}{
		{1, 1},
		{2, 1},
		{3, 1},
	}

	for _, pkg := range packets {
		p := orderPkt(pkg.seq, float64(100+pkg.seq), pkg.token, feed.Buy, 100)
		if e := dispatchPkt(t, s, m, 2, p, uint64(pkg.seq)); e != nil {
			t.Fatal(e)
		}
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil {
		t.Fatal(e)
	}

	// Initially, shard should be in CATCHING_UP (pending > 0)
	var pending int64
	shardNum := events[0].Shard
	if e := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mtbt.subscriber_outbox WHERE stream_id=$1 AND shard=$2`, m.StreamID, shardNum).Scan(&pending); e != nil {
		t.Fatal(e)
	}
	if pending == 0 {
		t.Fatal("expected pending outbox entries")
	}

	// Apply all events
	for _, ev := range events {
		if ev.Shard == shardNum {
			if e := s.ApplyShard(ctx, m, 2, ev); e != nil {
				t.Fatal(e)
			}
		}
	}

	// After applying, outbox should remain (ApplyShard doesn't delete outbox)
	if e := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mtbt.subscriber_outbox WHERE stream_id=$1 AND shard=$2`, m.StreamID, shardNum).Scan(&pending); e != nil {
		t.Fatal(e)
	}
	if pending == 0 {
		t.Fatal("expected outbox to remain after ApplyShard")
	}

	// Confirm dispatch for each event - this marks publication and removes from outbox
	for _, ev := range events {
		if ev.Shard == shardNum {
			if e := s.ConfirmDispatch(ctx, m, ev); e != nil {
				t.Fatal(e)
			}
		}
	}

	// After confirming dispatch, outbox should be drained
	if e := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM mtbt.subscriber_outbox WHERE stream_id=$1 AND shard=$2`, m.StreamID, shardNum).Scan(&pending); e != nil {
		t.Fatal(e)
	}
	if pending != 0 {
		t.Fatalf("expected outbox to be drained after ConfirmDispatch, got %d pending", pending)
	}

	// Check apply lag via Snapshot.PendingEvents
	pkt, e := feed.Decode(events[0].Raw)
	if e != nil {
		t.Fatal(e)
	}
	snap, e := s.Snapshot(ctx, m, pkt.Token, 1)
	if e != nil {
		t.Fatal(e)
	}
	if snap.PendingEvents != 0 {
		t.Fatalf("expected no pending events in snapshot, got %d", snap.PendingEvents)
	}
}

// TestParallelHeartbeatTailFailClosed verifies heartbeat gap handling.
func TestParallelHeartbeatTailFailClosed(t *testing.T) {
	s, m := parallelSetup(t, 2)

	// Dispatch a packet at broker 1
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}

	// Heartbeat claiming a different lastSequence should fail
	hb := heartbeatPkt(2) // claims seq 2, but we've only seen seq 1
	if e := dispatchPkt(t, s, m, 2, hb, 2); !IsFatal(e) {
		t.Fatal("heartbeat continuity gap should fail closed")
	}
}

// TestParallelBadShardSequence verifies bad shard routing fails closed.
func TestParallelBadShardSequence(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch valid packet
	p := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p, 1); e != nil {
		t.Fatal(e)
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil || len(events) == 0 {
		t.Fatal(e)
	}
	ev := events[0]

	// Create an invalid event with bad shard
	badEv := sharding.Event{
		Shard:        999, // out of range
		Sequence:     ev.Sequence,
		SourceBroker: ev.SourceBroker,
		Raw:          ev.Raw,
		Received:     ev.Received,
	}

	// Should fail immediately on invalid identity
	if e := s.ApplyShard(ctx, m, 2, badEv); e == nil {
		t.Fatal("invalid shard should fail")
	}
}

// TestParallelUndispatchedEventGap verifies undispatched event detection.
func TestParallelUndispatchedEventGap(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch and then create an event that claims a sequence not yet dispatched
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil || len(events) == 0 {
		t.Fatal(e)
	}
	ev := events[0]

	// Create event claiming sequence 2 when only 1 is dispatched
	badEv := sharding.Event{
		Shard:        ev.Shard,
		Sequence:     2,
		SourceBroker: ev.SourceBroker,
		Raw:          ev.Raw,
		Received:     ev.Received,
	}

	// Should fail: undispatched event
	if e := s.ApplyShard(ctx, m, 2, badEv); !IsFatal(e) {
		t.Fatal("undispatched event should fail closed")
	}
}

// TestParallelCheckSerialRejectsMixedMode verifies CheckSerial prevents mode mixing.
func TestParallelCheckSerialRejectsMixedMode(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// After InitParallel, CheckSerial should reject
	if e := s.CheckSerial(ctx, m); e == nil {
		t.Fatal("CheckSerial should reject after InitParallel")
	}
}

// TestParallelStableReplayAfterCrash verifies outbox stable after crash.
func TestParallelStableReplayAfterCrash(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch several packets
	for i := 1; i <= 3; i++ {
		p := orderPkt(int32(i), float64(100+i), int32((i-1)%2+1), feed.Buy, 100)
		if e := dispatchPkt(t, s, m, 2, p, uint64(i)); e != nil {
			t.Fatal(e)
		}
	}

	// Snapshot outbox
	events1, e := s.PendingDispatch(ctx, m)
	if e != nil {
		t.Fatal(e)
	}

	// Apply some events
	for _, ev := range events1[:len(events1)/2] {
		if e := s.ApplyShard(ctx, m, 2, ev); e != nil {
			t.Fatal(e)
		}
	}

	// Simulate process restart: new store instance
	s2, _ := Open(ctx, os.Getenv("MTBT_TEST_DATABASE_URL"))
	defer s2.Close()
	if e := s2.Migrate(ctx); e != nil {
		t.Fatal(e)
	}

	// Check apply lag via Snapshot.PendingEvents (dispatched - applied)
	pkt1, e := feed.Decode(events1[0].Raw)
	if e != nil {
		t.Fatal(e)
	}
	snap, e := s2.Snapshot(ctx, m, pkt1.Token, 1)
	if e != nil {
		t.Fatal(e)
	}
	// After applying some, pending should be less than initial
	if snap.PendingEvents >= int64(len(events1)) {
		t.Fatalf("expected fewer pending events after applying some, got %d (was %d)", snap.PendingEvents, len(events1))
	}
	if snap.PendingEvents < 0 {
		t.Fatalf("pending events should not be negative, got %d", snap.PendingEvents)
	}
}

// TestParallelConcurrentShardApplies verifies concurrent independent shard applies complete.
func TestParallelConcurrentShardApplies(t *testing.T) {
	s, m := parallelSetup(t, 4)
	ctx := context.Background()

	// Dispatch packets to spread across shards
	// We want packets distributed across shards using allowed tokens (1,2,3 only)
	// Token 1: shard 1%4=1, Token 2: shard 2%4=2, Token 3: shard 3%4=3, Token 1 again: shard 0 (need shard 0 coverage)
	// Actually, with 4 shards and tokens 1,2,3: we get shards 1,2,3. Shard 0 won't have events.
	// So we use just 3 packets to avoid the sequence gap
	packets := []struct {
		seq   int32
		token int32
	}{
		{1, 1}, // shard 1%4=1
		{2, 2}, // shard 2%4=2
		{3, 3}, // shard 3%4=3
	}

	for _, pkg := range packets {
		p := orderPkt(pkg.seq, float64(100+pkg.seq), pkg.token, feed.Buy, 100)
		if e := dispatchPkt(t, s, m, 4, p, uint64(pkg.seq)); e != nil {
			t.Fatal(e)
		}
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil {
		t.Fatal(e)
	}

	// Apply events concurrently across shards
	var wg sync.WaitGroup
	var applyErrors atomic.Uint32
	for _, ev := range events {
		wg.Add(1)
		go func(evt sharding.Event) {
			defer wg.Done()
			if e := s.ApplyShard(ctx, m, 4, evt); e != nil {
				applyErrors.Add(1)
				t.Logf("ApplyShard error: %v", e)
			}
		}(ev)
	}
	wg.Wait()

	if applyErrors.Load() > 0 {
		t.Fatalf("concurrent ApplyShard had %d errors", applyErrors.Load())
	}

	// Confirm dispatch for each event to remove from outbox
	var confirmErrors atomic.Uint32
	for _, ev := range events {
		wg.Add(1)
		go func(evt sharding.Event) {
			defer wg.Done()
			if e := s.ConfirmDispatch(ctx, m, evt); e != nil {
				confirmErrors.Add(1)
				t.Logf("ConfirmDispatch error: %v", e)
			}
		}(ev)
	}
	wg.Wait()

	if confirmErrors.Load() > 0 {
		t.Fatalf("concurrent ConfirmDispatch had %d errors", confirmErrors.Load())
	}

	// Verify all events are applied and dispatched
	remaining, e := s.PendingDispatch(ctx, m)
	if e != nil {
		t.Fatal(e)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected no pending events, got %d", len(remaining))
	}
}

// TestParallelManifestMismatchRejects verifies manifest digest mismatch detection.
func TestParallelManifestMismatchRejects(t *testing.T) {
	s, m := parallelSetup(t, 2)

	// Dispatch a valid packet
	p := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p, 1); e != nil {
		t.Fatal(e)
	}

	// Create a different manifest (different seed)
	m2 := m
	m2.Seed = 99

	// Attempting to dispatch with different manifest should reject
	p2 := orderPkt(2, 101, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p2, 2); e != nil {
		t.Fatal(e)
	}
	if e := dispatchPkt(t, s, m2, 2, p2, 2); !IsFatal(e) {
		t.Fatal("manifest mismatch should fail closed")
	}
}

// TestParallelTopologyMismatchRejects verifies topology change detection.
func TestParallelTopologyMismatchRejects(t *testing.T) {
	s, m := parallelSetup(t, 2)

	// Dispatch with 2 shards
	p := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p, 1); e != nil {
		t.Fatal(e)
	}

	// Attempting to dispatch with different shard count should reject
	p2 := orderPkt(2, 101, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 3, p2, 2); !IsFatal(e) {
		t.Fatal("topology mismatch should fail closed")
	}
}

// TestParallelTickSizeValidation verifies price tick size enforcement.
func TestParallelTickSizeValidation(t *testing.T) {
	s, m := parallelSetup(t, 2)

	// Dispatch a packet with valid price (divisible by tick size 5)
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p1, 1); e != nil {
		t.Fatal(e)
	}

	// Dispatch with invalid price (not divisible by tick size 5)
	p2 := orderPkt(2, 101, 1, feed.Buy, 103) // 103 % 5 != 0
	if e := dispatchPkt(t, s, m, 2, p2, 2); !IsFatal(e) {
		t.Fatal("invalid tick price should fail closed")
	}
}

// TestParallelInvalidRunState verifies rejection when run is INVALID.
func TestParallelInvalidRunState(t *testing.T) {
	s, m := parallelSetup(t, 2)

	// Cause invalidation
	p1 := orderPkt(1, 100, 1, feed.Buy, 100)
	dispatchPkt(t, s, m, 2, p1, 1)

	p2 := orderPkt(2, 101, 1, feed.Buy, 100)
	dispatchPkt(t, s, m, 2, p2, 3) // gap: should invalidate

	// Try to dispatch to invalid run
	p3 := orderPkt(3, 102, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p3, 4); !IsFatal(e) {
		t.Fatal("dispatch to INVALID run should fail closed")
	}
}

// TestParallelZeroReceivedTimestamp verifies zero received time rejection.
func TestParallelZeroReceivedTimestamp(t *testing.T) {
	s, m := parallelSetup(t, 2)

	p := orderPkt(1, 100, 1, feed.Buy, 100)
	raw, _ := feed.Encode(p)

	// Dispatch with zero timestamp should fail
	if e := s.Dispatch(context.Background(), m, 2, raw, 1, time.Time{}); !IsFatal(e) {
		t.Fatal("zero received timestamp should fail closed")
	}
}

// TestParallelInvalidBrokerSequence verifies broker sequence bounds.
func TestParallelInvalidBrokerSequence(t *testing.T) {
	s, m := parallelSetup(t, 2)

	p := orderPkt(1, 100, 1, feed.Buy, 100)
	raw, _ := feed.Encode(p)

	// Broker 0 should return an error (ordinary error, not fatal)
	if e := s.Dispatch(context.Background(), m, 2, raw, 0, time.Now().UTC()); e == nil {
		t.Fatal("broker sequence 0 should fail")
	}

	// Broker > MaxInt64 should also return an ordinary error (both are validation errors)
	if e := s.Dispatch(context.Background(), m, 2, raw, math.MaxUint64, time.Now().UTC()); e == nil {
		t.Fatal("broker sequence > MaxInt64 should fail")
	}
}

// TestParallelApplyShardRouting verifies shard routing validation in ApplyShard.
func TestParallelApplyShardRouting(t *testing.T) {
	s, m := parallelSetup(t, 2)
	ctx := context.Background()

	// Dispatch packet to token 1 (shard 1)
	p := orderPkt(1, 100, 1, feed.Buy, 100)
	if e := dispatchPkt(t, s, m, 2, p, 1); e != nil {
		t.Fatal(e)
	}

	events, e := s.PendingDispatch(ctx, m)
	if e != nil || len(events) == 0 {
		t.Fatal(e)
	}
	ev := events[0]

	// Create event with wrong shard for the token
	wrongShardEv := sharding.Event{
		Shard:        (ev.Shard + 1) % 2, // different shard
		Sequence:     ev.Sequence,
		SourceBroker: ev.SourceBroker,
		Raw:          ev.Raw,
		Received:     ev.Received,
	}

	// Should reject: routing mismatch
	if e := s.ApplyShard(ctx, m, 2, wrongShardEv); !IsFatal(e) {
		t.Fatal("shard routing mismatch should fail closed")
	}
}
