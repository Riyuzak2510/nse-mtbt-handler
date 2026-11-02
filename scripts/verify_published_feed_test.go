package main

import (
	"nse-mtbt-handler/internal/feed"
	"testing"
)

func TestVerifierHeartbeatDecode(t *testing.T) {
	b, e := feed.Encode(feed.Packet{Stream: 1, Type: feed.Heartbeat, LastSequence: 161000})
	if e != nil {
		t.Fatal(e)
	}
	p, e := feed.Decode(b)
	if e != nil || p.LastSequence != 161000 {
		t.Fatal(p, e)
	}
}
