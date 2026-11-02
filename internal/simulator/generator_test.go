package simulator

import (
	"context"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"reflect"
	"sort"
	"testing"
	"time"
)

type memory map[feed.OrderID]book.Order

func (m memory) Get(_ context.Context, id feed.OrderID) (book.Order, bool, error) {
	o, ok := m[id]
	return o, ok, nil
}
func (m memory) Put(_ context.Context, o book.Order) error       { m[o.ID] = o; return nil }
func (m memory) Delete(_ context.Context, id feed.OrderID) error { delete(m, id); return nil }
func TestDeterministicAndBookEquivalence(t *testing.T) {
	manifest := runconfig.Manifest{Version: 2, Profile: feed.Profile, StreamID: 1, Baseline: "empty", CreatedAt: time.Unix(1, 0).UTC(), Seed: 42, Instruments: []runconfig.Instrument{{Token: 1, Symbol: "SIM", TickSize: 5, BasePrice: 10000}}}
	a, e := New(manifest)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := New(manifest)
	state := memory{}
	kinds := map[feed.Kind]bool{}
	for i := int64(1); i <= 10000; i++ {
		p, e := a.Next(i)
		if e != nil {
			t.Fatal(e)
		}
		q, e := b.Next(i)
		if e != nil || p != q {
			t.Fatal("not deterministic", i, e)
		}
		raw, e := feed.Encode(p)
		if e != nil {
			t.Fatal(e)
		}
		r, e := feed.Decode(raw)
		if e != nil {
			t.Fatal(e)
		}
		if e := book.Apply(context.Background(), state, r); e != nil {
			t.Fatalf("sequence %d: %v", i, e)
		}
		kinds[p.Type] = true
	}
	orders := []book.Order{}
	for _, o := range state {
		orders = append(orders, o)
	}
	sort.Slice(orders, func(i, j int) bool { return orders[i].ID < orders[j].ID })
	if !reflect.DeepEqual(a.Orders(), orders) {
		t.Fatal("independent generator differs from applied book")
	}
	for _, k := range []feed.Kind{feed.New, feed.Modify, feed.Cancel, feed.Trade} {
		if !kinds[k] {
			t.Fatal("missing action", k)
		}
	}
	if a.Heartbeat().Sequence != 0 || a.Heartbeat().LastSequence != 10000 {
		t.Fatal("heartbeat progress")
	}
}
