package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"os"
	"strconv"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	m, e := runconfig.Load(os.Args[1])
	if e != nil {
		return e
	}
	count, e := strconv.Atoi(os.Args[2])
	if e != nil {
		return e
	}
	c, e := broker.Connect(context.Background(), os.Getenv("NATS_URL"), m)
	if e != nil {
		return e
	}
	defer c.Close()
	info, e := c.JS.StreamInfo(c.Stream)
	if e != nil {
		return e
	}
	if info.State.Msgs != uint64(count+1) || info.State.FirstSeq != 1 {
		return fmt.Errorf("stream not complete: %+v", info.State)
	}
	name := "VERIFY_ASYNC_20261024"
	if _, e = c.JS.ConsumerInfo(c.Stream, name); e != nats.ErrConsumerNotFound {
		return fmt.Errorf("fresh verification consumer required: %v", e)
	}
	_, e = c.JS.AddConsumer(c.Stream, &nats.ConsumerConfig{Durable: name, FilterSubject: c.Subject, AckPolicy: nats.AckNonePolicy, DeliverPolicy: nats.DeliverAllPolicy})
	if e != nil {
		return e
	}
	defer c.JS.DeleteConsumer(c.Stream, name)
	sub, e := c.JS.PullSubscribe(c.Subject, name, nats.Bind(c.Stream, name))
	if e != nil {
		return e
	}
	defer sub.Unsubscribe()
	seen := 0
	started := time.Now()
	catalog := len(m.Instruments)
	for seen < count+1 {
		batch := 512
		if left := count + 1 - seen; batch > left {
			batch = left
		}
		msgs, e := sub.Fetch(batch, nats.MaxWait(10*time.Second))
		if e != nil {
			return fmt.Errorf("fetch after %d verified messages: %w", seen, e)
		}
		for _, msg := range msgs {
			seen++
			meta, e := msg.Metadata()
			if e != nil {
				return e
			}
			if meta.Sequence.Stream != uint64(seen) || meta.NumDelivered != 1 {
				return fmt.Errorf("broker order/redelivery at%d", seen)
			}
			if _, e = c.Validate(msg); e != nil {
				return e
			}
			p, e := feed.Decode(msg.Data)
			if e != nil {
				return e
			}
			if seen == count+1 {
				if p.Type != feed.Heartbeat || p.LastSequence != int32(count) {
					return fmt.Errorf("badheartbeat")
				}
			} else {
				slot := (seen - 1) % (3 * catalog)
				inst := m.Instruments[slot%catalog]
				side, q := feed.Buy, int32(100)
				if slot >= catalog {
					side = feed.Sell
					q = 30
				}
				if slot >= 2*catalog {
					q = 70
				}
				if p.Sequence != int32(seen) || p.Stream != m.StreamID || p.Type != feed.New || p.Token != inst.Token || p.OrderID.Float64() != float64(seen) || p.Side != side || p.Quantity != q || p.Price != inst.BasePrice || p.Timestamp <= 0 {
					return fmt.Errorf("unexpectedpayloadat%d:%+v", seen, p)
				}
			}
		}
	}
	out := map[string]any{"verified_messages": seen, "data_messages": count, "heartbeat": true, "stream_sequence_equals_native": true, "exact_payload_and_envelope": true, "redeliveries": 0, "seconds": time.Since(started).Seconds(), "scope": "post-run retained readback; separate temporary validation consumer deleted, original book/matching consumer unchanged"}
	return json.NewEncoder(os.Stdout).Encode(out)
}
