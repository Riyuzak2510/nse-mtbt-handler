package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/store"
	"nse-mtbt-handler/internal/timingtrace"
)

// runParallel owns all shard subscriptions in one process under the stream lease.
// There is one ordered dispatcher and one sequential goroutine per shard, not per event.
func runParallel(parent context.Context, db *store.Store, c *broker.Client, rawSub *nats.Subscription, m runconfig.Manifest, count int, serverErr <-chan error, httpAddr string) error {
	return runParallelTimed(parent, db, c, rawSub, m, count, serverErr, httpAddr, nil)
}
func runParallelTimed(parent context.Context, db *store.Store, c *broker.Client, rawSub *nats.Subscription, m runconfig.Manifest, count int, serverErr <-chan error, httpAddr string, traces *timingtrace.Parallel) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	shards, err := broker.OpenShards(ctx, c, count)
	if err != nil {
		return err
	}
	defer shards.Close()
	subs := make([]*nats.Subscription, count)
	for i := range subs {
		subs[i], err = shards.Subscribe(i)
		if err != nil {
			return err
		}
	}
	results := make(chan error, count+1)
	var wg sync.WaitGroup
	launch := func(fn func() error) { wg.Add(1); go func() { defer wg.Done(); results <- fn() }() }
	for i := range subs {
		sub := subs[i]
		recorder := traces.Worker(i)
		launch(func() error { return consumeShard(ctx, db, shards, sub, m, count, recorder) })
	}
	launch(func() error { return dispatchRaw(ctx, db, c, shards, rawSub, m, count, traces.DispatcherRecorder()) })
	log.Printf("book service ready stream=%d http=%s shards=%d routing=token-mod-v1", m.StreamID, httpAddr, count)
	var result error
	select {
	case <-parent.Done():
	case result = <-results:
	case result = <-serverErr:
		if errors.Is(result, http.ErrServerClosed) {
			result = nil
		}
	}
	cancel()
	wg.Wait()
	if parent.Err() != nil {
		return nil
	}
	return result
}

func retryDB(ctx context.Context, fn func(context.Context) error) error {
	for ctx.Err() == nil {
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := fn(op)
		cancel()
		if err == nil || store.IsFatal(err) {
			return err
		}
		log.Printf("database processing failed; retrying without ack: %v", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return ctx.Err()
}
func failEnvelope(ctx context.Context, db *store.Store, m runconfig.Manifest, cause error) error {
	if err := retryDB(ctx, func(op context.Context) error { return db.Invalidate(op, m, cause.Error()) }); err != nil {
		return err
	}
	return fmt.Errorf("invalid broker envelope: %w", cause)
}
func drainDispatch(ctx context.Context, db *store.Store, c *broker.Shards, m runconfig.Manifest) error {
	var eventsErr error
	// Pending data is bounded by one raw input by construction. Retry DB failures,
	// but stop on publication ambiguity; restart reuses its stable shard sequence.
	events, err := db.PendingDispatch(ctx, m)
	if err != nil {
		return err
	}
	for _, e := range events {
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = c.Publish(op, e)
		cancel()
		if err != nil {
			return fmt.Errorf("shard publication failed; restart safely: %w", err)
		}
		eventsErr = retryDB(ctx, func(op context.Context) error { return db.ConfirmDispatch(op, m, e) })
		if eventsErr != nil {
			return eventsErr
		}
	}
	return nil
}
func dispatchRaw(ctx context.Context, db *store.Store, c *broker.Client, shards *broker.Shards, sub *nats.Subscription, m runconfig.Manifest, count int, trace *timingtrace.Recorder) error {
	if err := drainDispatch(ctx, db, shards, m); err != nil {
		return err
	}
	for ctx.Err() == nil {
		msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			return err
		}
		delivery := time.Now()
		msg := msgs[0]
		received, err := c.Validate(msg)
		if err != nil {
			return failEnvelope(ctx, db, m, err)
		}
		meta, err := msg.Metadata()
		if err != nil {
			return err
		}
		start := time.Now()
		var attempts int64
		if err = retryDB(ctx, func(op context.Context) error {
			attempts++
			return db.Dispatch(op, m, count, msg.Data, meta.Sequence.Stream, received)
		}); err != nil {
			return err
		}
		end := time.Now()
		routeStart := time.Now()
		if err = drainDispatch(ctx, db, shards, m); err != nil {
			return err
		}
		routeEnd := time.Now()
		ackStart := time.Now()
		err = msg.AckSync(nats.AckWait(3 * time.Second))
		ackEnd := time.Now()
		seq, kind := timingtrace.Identity(msg.Data)
		sample := timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: start.UnixNano(), EndUnixNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(), ReceivedUnixNS: received.UnixNano(), DeliveryUnixNS: delivery.UnixNano(), Attempts: attempts, SourceBroker: meta.Sequence.Stream, Shard: -1, RouteStartUnixNS: routeStart.UnixNano(), RouteEndUnixNS: routeEnd.UnixNano(), RouteDurationNS: routeEnd.Sub(routeStart).Nanoseconds(), AckDurationNS: ackEnd.Sub(ackStart).Nanoseconds()}
		if err == nil {
			sample.AckUnixNS = ackEnd.UnixNano()
		}
		trace.Record(sample)
		if err != nil {
			return fmt.Errorf("dispatch committed but raw ack failed; restart safely: %w", err)
		}
	}
	return nil
}
func consumeShard(ctx context.Context, db *store.Store, c *broker.Shards, sub *nats.Subscription, m runconfig.Manifest, count int, trace *timingtrace.Recorder) error {
	for ctx.Err() == nil {
		msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
		if errors.Is(err, nats.ErrTimeout) {
			continue
		}
		if err != nil {
			return err
		}
		delivery := time.Now()
		msg := msgs[0]
		event, err := c.Validate(msg)
		if err != nil {
			return failEnvelope(ctx, db, m, err)
		}
		start := time.Now()
		var attempts int64
		if err = retryDB(ctx, func(op context.Context) error { attempts++; return db.ApplyShard(op, m, count, event) }); err != nil {
			return err
		}
		end := time.Now()
		ackStart := time.Now()
		err = msg.AckSync(nats.AckWait(3 * time.Second))
		ackEnd := time.Now()
		seq, kind := timingtrace.Identity(event.Raw)
		sample := timingtrace.Sample{Sequence: seq, Type: kind, StartUnixNS: start.UnixNano(), EndUnixNS: end.UnixNano(), DurationNS: end.Sub(start).Nanoseconds(), ReceivedUnixNS: event.Received.UnixNano(), DeliveryUnixNS: delivery.UnixNano(), Attempts: attempts, Shard: event.Shard, ShardSequence: event.Sequence, SourceBroker: event.SourceBroker, AckDurationNS: ackEnd.Sub(ackStart).Nanoseconds()}
		if err == nil {
			sample.AckUnixNS = ackEnd.UnixNano()
		}
		trace.Record(sample)
		if err != nil {
			return fmt.Errorf("shard committed but ack failed; restart safely: %w", err)
		}
	}
	return nil
}
