package broker

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNativeStreamNamesAndConfigurationPin(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("set isolated NATS integration URL")
	}
	ctx := context.Background()
	m := shadowManifest()
	m.StreamID = 9
	c, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer c.JS.DeleteStream(c.Stream)
	if c.Stream != "MTBT_9" || c.Subject != "mtbt.9" {
		t.Fatal(c.Stream, c.Subject)
	}
	m.CreatedAt = m.CreatedAt.Add(time.Hour)
	resumed, err := Connect(ctx, url, m)
	if err != nil {
		t.Fatal("creation metadata blocked resume", err)
	}
	resumed.Close()
	incompatible := m
	incompatible.Seed++
	if bad, err := Connect(ctx, url, incompatible); err == nil {
		bad.Close()
		t.Fatal("accepted incompatible configuration")
	}
	info, err := c.JS.StreamInfo(c.Stream)
	if err != nil || info.State.Msgs != 0 {
		t.Fatal("rejection altered stream", info, err)
	}
}

func TestRejectLegacyEnvelopeField(t *testing.T) {
	for _, key := range []string{"Mtbt-Run", "mtbt-run", "MTBT-RUN"} {
		for _, value := range []string{"", "legacy"} {
			c := &Client{}
			msg := &nats.Msg{Header: nats.Header{key: []string{value}}}
			if _, err := c.Validate(msg); err == nil {
				t.Fatal("accepted legacy header", key, value)
			}
			shards := &Shards{client: c}
			if _, err := shards.Validate(msg); err == nil {
				t.Fatal("accepted routed legacy header", key, value)
			}
			if !hasLegacyRunHeader(msg.Header) {
				t.Fatal("legacy field not recognized")
			}
		}
	}
}
