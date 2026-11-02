package broker

import (
	"bytes"
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"os"
	"testing"
	"time"
)

func TestValidateAsyncAck(t *testing.T) {
	for _, a := range []*nats.PubAck{nil, {Stream: "bad", Sequence: 1}, {Stream: "MTBT_1", Sequence: 0}, {Stream: "MTBT_1", Sequence: 2}, {Stream: "MTBT_1", Sequence: 1, Duplicate: true}} {
		if ValidateAck(a, "MTBT_1", 1) == nil {
			t.Fatal(a)
		}
	}
	if e := ValidateAck(&nats.PubAck{Stream: "MTBT_1", Sequence: 1}, "MTBT_1", 1); e != nil {
		t.Fatal(e)
	}
}
func TestAsyncPublicationEnvelopeAndOwnership(t *testing.T) {
	c := &Client{Subject: "mtbt.1", Stream: "MTBT_1", digest: "hash"}
	raw := []byte{0, 1, 255}
	now := time.Now()
	m := c.publication(raw, now)
	if !bytes.Equal(m.Data, raw) || m.Subject != c.Subject || m.Header.Get(VersionHeader) != "2" || m.Header.Get(ManifestHeader) != "hash" || m.Header.Get(ReceivedHeader) == "" || hasLegacyRunHeader(m.Header) {
		t.Fatal(m)
	}
	m.Data[0] = 5
	if raw[0] != 5 {
		t.Fatal("test must expose caller-owned immutable buffer contract")
	}
}
func TestAsyncConfigBounds(t *testing.T) {
	c := &Client{}
	for _, n := range []int{-1, 0, 4097} {
		if _, e := c.AsyncPublisher(n, time.Second); e == nil {
			t.Fatal(n)
		}
	}
	if _, e := c.AsyncPublisher(1, 0); e == nil {
		t.Fatal("timeout")
	}
}
func TestAsyncRealOrderedAdmission(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("isolated NATS integration URL required")
	}
	m := shadowManifest()
	m.StreamID = 315
	c, e := Connect(context.Background(), url, m)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	info, _ := c.JS.StreamInfo(c.Stream)
	if info.State.Msgs != 0 {
		t.Fatal("fresh isolated stream required")
	}
	defer c.JS.DeleteStream(c.Stream)
	ap, e := c.AsyncPublisher(64, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer ap.Close()
	var futures []nats.PubAckFuture
	for i := 0; i < 64; i++ {
		f, e := ap.Submit([]byte(fmt.Sprintf("raw-%d", i)), time.Now())
		if e != nil {
			t.Fatal(e)
		}
		futures = append(futures, f)
	}
	for i, f := range futures {
		select {
		case ack := <-f.Ok():
			if e = ValidateAck(ack, c.Stream, uint64(i+1)); e != nil {
				t.Fatal(e)
			}
		case e = <-f.Err():
			t.Fatal(e)
		case <-time.After(3 * time.Second):
			t.Fatal("no ack")
		}
		msg, e := c.JS.GetMsg(c.Stream, uint64(i+1))
		if e != nil || string(msg.Data) != fmt.Sprintf("raw-%d", i) {
			t.Fatal(msg, e)
		}
		if _, e = c.Validate(&nats.Msg{Subject: msg.Subject, Data: msg.Data, Header: msg.Header}); e != nil {
			t.Fatal(e)
		}
	}
}

func TestAsyncRealRejectionNoRetry(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("isolated integration URL required")
	}
	m := shadowManifest()
	m.StreamID = 316
	c, e := Connect(context.Background(), url, m)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	info, e := c.JS.StreamInfo(c.Stream)
	if e != nil || info.State.Msgs != 0 {
		t.Fatal("freshstreamrequired", e)
	}
	defer c.JS.DeleteStream(c.Stream)
	ap, e := c.AsyncPublisher(8, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer ap.Close()
	f, e := ap.Submit(make([]byte, 70000), time.Now())
	if e == nil {
		select {
		case <-f.Ok():
			t.Fatal("oversizedpacketaccepted")
		case e = <-f.Err():
			if e == nil {
				t.Fatal("missingrejection")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("rejectiontimeout")
		}
	}
	info, e = c.JS.StreamInfo(c.Stream)
	if e != nil || info.State.Msgs != 0 {
		t.Fatal("failedpublishmutatedstream", e)
	}
}
