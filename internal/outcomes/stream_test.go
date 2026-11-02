package outcomes

import (
	"context"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"os"
	"testing"
	"time"
)

func TestExistingStreamConfigRefusedAndRetainedWhenFull(t *testing.T) {
	url := os.Getenv("MTBT_TEST_NATS_URL")
	if url == "" {
		t.Skip("realNATSrequired")
	}
	stream := int16(31001)
	cfg := DefaultConfig()
	cfg.MaxBytes = 8192
	pub, e := Connect(url, stream, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pub.Close()
	nc, e := nats.Connect(url)
	if e != nil {
		t.Fatal(e)
	}
	defer nc.Close()
	js, _ := nc.JetStream()
	name, _ := Names(stream)
	defer js.DeleteStream(name)
	different := cfg
	different.MaxBytes *= 2
	if other, e := Connect(url, stream, different); e == nil {
		other.Close()
		t.Fatal("silentlyupdatedstream")
	}
	ev := fixture(t)
	var accepted int
	for n := int32(1); n < 100; n++ {
		ev, e = New(stream, n, uint64(n), feed.New, "NEW", ev.Order, time.Now())
		if e != nil {
			t.Fatal(e)
		}
		raw, _ := Encode(ev)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, e = pub.Publish(ctx, ev, raw)
		cancel()
		if e != nil {
			break
		}
		accepted++
	}
	if accepted == 0 || accepted == 99 {
		t.Fatal("streamnotbounded", accepted)
	}
	info, e := js.StreamInfo(name)
	if e != nil || info.State.FirstSeq != 1 || info.State.Msgs != uint64(accepted) {
		t.Fatal(info, e)
	}
}
