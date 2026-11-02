package outcomerelay

import (
	"context"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"testing"
	"time"
)

func TestBrokerAckLostAfterAcceptanceRetriesSingleLogicalEvent(t *testing.T) {
	r, s, _, _ := setup(t)
	send(t, s, 1, feed.Buy, 100)
	f := &fake{err: context.DeadlineExceeded}
	if _, e := r.Step(context.Background(), f); e == nil {
		t.Fatal("acklossnotreported")
	}
	f.err = nil
	f.duplicate = true
	if ok, e := r.Step(context.Background(), f); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if f.ids[0] != f.ids[1] {
		t.Fatal("IDchanged")
	}
}
func TestRelayPublishesNumericThenWireIDOrder(t *testing.T) {
	r, s, _, _ := setup(t)
	send(t, s, 1, feed.Buy, 100)
	send(t, s, 2, feed.Sell, 30)
	f := &fake{}
	for n := 0; n < 3; n++ {
		if ok, e := r.Step(context.Background(), f); e != nil || !ok {
			t.Fatal(ok, e)
		}
	}
	var previous outcomes.Event
	for i, b := range f.payloads {
		e, err := outcomes.Decode(b)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && (e.SourceSequence < previous.SourceSequence || e.SourceSequence == previous.SourceSequence && e.Order.OrderIDHex < previous.Order.OrderIDHex) {
			t.Fatal("outoforder", f.ids)
		}
		previous = e
	}
}

type latePublisher struct {
	f fake
	r *Relay
}

func (p *latePublisher) Publish(c context.Context, e outcomes.Event, b []byte) (*nats.PubAck, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.r.conn.Close(ctx)
	return p.f.Publish(c, e, b)
}
func TestConnectionLossDuringPublishLeavesPending(t *testing.T) {
	r, s, _, _ := setup(t)
	send(t, s, 1, feed.Buy, 100)
	if _, e := r.Step(context.Background(), &latePublisher{r: r}); e == nil {
		t.Fatal("lostleaseignored")
	}
}
