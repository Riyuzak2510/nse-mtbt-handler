package broker

import (
	"context"
	"math"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/sharding"
)

func shardManifest() runconfig.Manifest {
	return runconfig.Manifest{Version: 2, Profile: feed.Profile, StreamID: 2, Seed: 42, Baseline: "empty", CreatedAt: time.Now().UTC(), Instruments: []runconfig.Instrument{{Token: 1, Symbol: "A", TickSize: 5, BasePrice: 100}, {Token: 2, Symbol: "B", TickSize: 5, BasePrice: 200}}}
}
func shardMessage(t *testing.T, c *Client, token int32) *nats.Msg {
	t.Helper()
	id, _ := feed.IDFromFloat64(1)
	raw, err := feed.Encode(feed.Packet{Stream: 2, Sequence: 1, Type: feed.New, Timestamp: 1, OrderID: id, Token: token, Side: feed.Buy, Price: 100, Quantity: 10})
	if err != nil {
		t.Fatal(err)
	}
	shard, _ := sharding.ForToken(token, 2)
	streamID := strconv.Itoa(int(c.manifest.StreamID))
	msg := nats.NewMsg("mtbtshard." + streamID + "." + strconv.Itoa(shard))
	msg.Data = raw
	for k, v := range map[string]string{ProfileHeader: feed.Profile, ManifestHeader: c.digest, VersionHeader: "2", ReceivedHeader: strconv.FormatInt(time.Now().UnixNano(), 10), "Mtbt-Shard": strconv.Itoa(shard), "Mtbt-Shards": "2", "Mtbt-Shard-Sequence": "1", "Mtbt-Source-Broker": "1", "Mtbt-Routing": sharding.Version} {
		msg.Header.Set(k, v)
	}
	return msg
}
func TestShardEnvelopeValidation(t *testing.T) {
	m := shardManifest()
	digest, _ := m.Digest()
	c := &Client{manifest: m, digest: digest}
	s := &Shards{client: c, count: 2}
	tests := []struct {
		name   string
		mutate func(*nats.Msg)
	}{
		{"valid", func(*nats.Msg) {}},
		{"subject", func(m *nats.Msg) { m.Subject = "mtbtshard.wrong.1" }},
		{"legacy-run-header", func(m *nats.Msg) { m.Header.Set("Mtbt-Run", "run") }},
		{"profile", func(m *nats.Msg) { m.Header.Set(ProfileHeader, "wrong") }},
		{"manifest", func(m *nats.Msg) { m.Header.Set(ManifestHeader, "wrong") }},
		{"version", func(m *nats.Msg) { m.Header.Set(VersionHeader, "1") }},
		{"received-zero", func(m *nats.Msg) { m.Header.Set(ReceivedHeader, "0") }},
		{"received-future", func(m *nats.Msg) {
			m.Header.Set(ReceivedHeader, strconv.FormatInt(time.Now().Add(time.Hour).UnixNano(), 10))
		}},
		{"sequence-zero", func(m *nats.Msg) { m.Header.Set("Mtbt-Shard-Sequence", "0") }},
		{"sequence-overflow", func(m *nats.Msg) {
			m.Header.Set("Mtbt-Shard-Sequence", strconv.FormatUint(uint64(math.MaxInt64)+1, 10))
		}},
		{"source-zero", func(m *nats.Msg) { m.Header.Set("Mtbt-Source-Broker", "0") }},
		{"source-overflow", func(m *nats.Msg) { m.Header.Set("Mtbt-Source-Broker", strconv.FormatUint(uint64(math.MaxInt64)+1, 10)) }},
		{"count", func(m *nats.Msg) { m.Header.Set("Mtbt-Shards", "3") }},
		{"routing", func(m *nats.Msg) { m.Header.Set("Mtbt-Routing", "wrong") }},
		{"truncated", func(m *nats.Msg) { m.Data = m.Data[:10] }},
		{"heartbeat", func(m *nats.Msg) { m.Data, _ = feed.Encode(feed.Packet{Stream: 2, Type: feed.Heartbeat}) }},
		{"wrong-stream", func(m *nats.Msg) { p, _ := feed.Decode(m.Data); p.Stream = 1; m.Data, _ = feed.Encode(p) }},
		{"unknown-token", func(m *nats.Msg) { p, _ := feed.Decode(m.Data); p.Token = 3; m.Data, _ = feed.Encode(p) }},
		{"wrong-token-shard", func(m *nats.Msg) { p, _ := feed.Decode(m.Data); p.Token = 2; m.Data, _ = feed.Encode(p) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := shardMessage(t, c, 1)
			tc.mutate(msg)
			e, err := s.Validate(msg)
			if (err == nil) != (tc.name == "valid") {
				t.Fatalf("Validate=%+v,%v", e, err)
			}
		})
	}
}
func TestShardConsumersIntegration(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set MTBT_TEST_NATS_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	m := shardManifest()
	c, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer c.JS.DeleteStream(c.Stream)
	s, err := OpenShards(ctx, c, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer c.JS.DeleteStream(s.streamName)
	defer s.Close()
	if _, err = OpenShards(ctx, c, 0); err == nil {
		t.Fatal("zero shards accepted")
	}
	for i := 0; i < 2; i++ {
		sub, err := s.Subscribe(i)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.Subscribe(i); err == nil {
			t.Fatal("second owner accepted")
		}
		// With MaxAckPending1, fetch then ack each event, not a batch without acks.
		token := int32(2 - i)
		for seq := uint64(1); seq <= 2; seq++ {
			msg := shardMessage(t, c, token)
			e := sharding.Event{Shard: i, Sequence: seq, SourceBroker: seq, Raw: msg.Data, Received: time.Now()}
			if err = s.Publish(ctx, e); err != nil {
				t.Fatal(err)
			}
		}
		for seq := uint64(1); seq <= 2; seq++ {
			msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
			if err != nil {
				t.Fatal(err)
			}
			e, err := s.Validate(msgs[0])
			if err != nil || e.Sequence != seq || e.Shard != i {
				t.Fatal(e, err)
			}
			if err = msgs[0].AckSync(); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Existing immutable policy must be checked, not silently changed.
	ci, err := c.JS.ConsumerInfo(s.streamName, "shard-0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := ci.Config
	cfg.MaxAckPending = 2
	if _, err = c.JS.UpdateConsumer(s.streamName, &cfg); err != nil {
		t.Fatal(err)
	}
	if _, err = OpenShards(ctx, c, 2); err == nil {
		t.Fatal("incompatible consumer accepted")
	}
}
