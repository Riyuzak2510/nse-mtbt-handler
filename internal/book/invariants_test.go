package book

import (
	"context"
	"errors"
	"nse-mtbt-handler/internal/feed"
	"testing"
)

func TestTradeValidFirstSideInvalidSecondDoesNotMutate(t *testing.T) {
	s := NewMemoryStore()
	ctx := context.Background()
	_ = s.Put(ctx, Order{ID: oid(1), Token: 1, Side: feed.Buy, Price: 100, Quantity: 10})
	_ = s.Put(ctx, Order{ID: oid(2), Token: 1, Side: feed.Sell, Price: 100, Quantity: 1})
	p := feed.Packet{Stream: 1, Sequence: 1, Type: feed.Trade, Timestamp: 1, BuyID: oid(1), SellID: oid(2), Token: 1, Price: 100, Quantity: 5}
	if e := Apply(ctx, s, p); !IsInvariant(e) {
		t.Fatal("expected invariant", e)
	}
	for _, want := range []Order{{ID: oid(1), Token: 1, Side: feed.Buy, Price: 100, Quantity: 10}, {ID: oid(2), Token: 1, Side: feed.Sell, Price: 100, Quantity: 1}} {
		got, ok, e := s.Get(ctx, want.ID)
		if e != nil || !ok || got != want {
			t.Fatal("partially applied trade", got, e)
		}
	}
}

type brokenStore struct{}

func (brokenStore) Get(context.Context, feed.OrderID) (Order, bool, error) {
	return Order{}, false, errors.New("database unavailable")
}
func (brokenStore) Put(context.Context, Order) error           { return nil }
func (brokenStore) Delete(context.Context, feed.OrderID) error { return nil }
func TestStorageFailureNotSourceInvariant(t *testing.T) {
	p := newPacket(feed.New, 1, 1)
	p.OrderID = oid(1)
	p.Token = 1
	p.Side = feed.Buy
	p.Price = 100
	p.Quantity = 1
	e := Apply(context.Background(), brokenStore{}, p)
	if e == nil || IsInvariant(e) {
		t.Fatal("DB error misclassified", e)
	}
}
