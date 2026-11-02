package durable

import (
	"context"
	"nse-mtbt-handler/internal/feed"
	"testing"
	"time"
)

func TestTickerQueuedModifyCancelAndControl(t *testing.T) {
	s, m, dsn := intakeSetup(t)
	admit(t, s, 1, 1, 1, feed.Buy, 100)
	admit(t, s, 2, 2, 2, feed.Buy, 100)
	admit(t, s, 3, 1, 3, feed.Sell, 30)
	// Intake is ahead of matching; these requests must not overwrite pending NEW.
	p := packet(4, 1, feed.Buy, 50)
	p.Stream = m.StreamID
	p.Type = feed.Modify
	p.Price = 110
	raw, e := feed.Encode(p)
	must(t, e)
	must(t, s.Process(context.Background(), raw, 4, time.Now().UTC()))
	p.Sequence = 5
	p.Type = feed.Cancel
	p.Quantity = 0
	p.Price = 0
	raw, e = feed.Encode(p)
	must(t, e)
	must(t, s.Process(context.Background(), raw, 5, time.Now().UTC()))
	hb, e := feed.Encode(feed.Packet{Stream: m.StreamID, Type: feed.Heartbeat, LastSequence: 5})
	must(t, e)
	must(t, s.Process(context.Background(), hb, 6, time.Now().UTC()))
	a := ticker(t, dsn, m, 1)
	want(t, a, 1, 100, 0, "PENDING")
	cs := commands(t, s, 1)
	if len(cs) != 5 {
		t.Fatal("control must fan out to each ticker", len(cs))
	}
	for _, c := range cs {
		must(t, a.ProcessCommand(context.Background(), c))
	}
	want(t, a, 1, 0, 30, "CANCELLED")
	o, found, e := a.Order(context.Background(), p.OrderID)
	must(t, e)
	if !found || o.Accepted != 150 || o.Cancelled != 120 {
		t.Fatal("replace/cancel conservation", o)
	}
	a.Close()
	a = ticker(t, dsn, m, 1)
	if a.engine.ActiveOrders() != 0 {
		t.Fatal("terminal order restored")
	}
	// Other ticker is independently PENDING, not overwritten by ticker1 controls.
	b := ticker(t, dsn, m, 2)
	want(t, b, 2, 100, 0, "PENDING")
	for _, c := range commands(t, s, 2) {
		must(t, b.ProcessCommand(context.Background(), c))
	}
	want(t, b, 2, 100, 0, "OPEN")
}

func TestTickerRejectsOutOfOrderAndWrongOwner(t *testing.T) {
	s, m, dsn := intakeSetup(t)
	admit(t, s, 1, 1, 1, feed.Buy, 100)
	admit(t, s, 2, 1, 2, feed.Sell, 30)
	a := ticker(t, dsn, m, 1)
	b := ticker(t, dsn, m, 2)
	cs := commands(t, s, 1)
	if a.ProcessCommand(context.Background(), cs[1]) == nil {
		t.Fatal("out-of-order command accepted")
	}
	if b.ProcessCommand(context.Background(), cs[0]) == nil {
		t.Fatal("wrong instrument accepted")
	}
	want(t, a, 1, 100, 0, "PENDING")
}
