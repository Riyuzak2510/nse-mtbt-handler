package matching

import (
	"math/rand"
	"nse-mtbt-handler/internal/feed"
	"reflect"
	"sort"
	"testing"
)

func packet(kind feed.Kind, id int, side feed.Side, price, qty int32) feed.Packet {
	oid, _ := feed.IDFromFloat64(float64(id))
	return feed.Packet{Stream: 1, Sequence: 1, Type: kind, OrderID: oid, Token: 1, Side: side, Price: price, Quantity: qty}
}
func apply(t *testing.T, e *Engine, p feed.Packet) Result {
	t.Helper()
	r, err := e.Apply(p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func newEngine(t *testing.T, n int) *Engine {
	t.Helper()
	e, err := New(n)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func snapshot(t *testing.T, e *Engine, token int32) Depth {
	t.Helper()
	s, err := e.Snapshot(token, 100)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestFIFOAndPartialFills(t *testing.T) {
	e := newEngine(t, 10)
	a := packet(feed.New, 1, feed.Sell, 100, 5)
	b := packet(feed.New, 2, feed.Sell, 100, 6)
	apply(t, e, a)
	apply(t, e, b)
	incoming := packet(feed.New, 3, feed.Buy, 110, 8)
	r := apply(t, e, incoming)
	want := []Fill{{BuyID: incoming.OrderID, SellID: a.OrderID, Token: 1, Price: 100, Quantity: 5}, {BuyID: incoming.OrderID, SellID: b.OrderID, Token: 1, Price: 100, Quantity: 3}}
	if !reflect.DeepEqual(r.Fills, want) {
		t.Fatalf("fills=%+v want=%+v", r.Fills, want)
	}
	if got := snapshot(t, e, 1); !reflect.DeepEqual(got.Asks, []Level{{100, 3, 1}}) || len(got.Bids) != 0 {
		t.Fatal(got)
	}
	if e.ActiveOrders() != 1 || e.Stats().ExecutedQuantity != 8 {
		t.Fatal(e.Stats())
	}
}
func TestMultilevelBothDirectionsAndTokens(t *testing.T) {
	for _, side := range []feed.Side{feed.Buy, feed.Sell} {
		e := newEngine(t, 10)
		opposite := feed.Sell
		prices := []int32{105, 100, 110}
		limit := int32(110)
		want := []int32{100, 105, 110}
		if side == feed.Sell {
			opposite = feed.Buy
			limit = 100
			want = []int32{110, 105, 100}
		}
		for i, p := range prices {
			apply(t, e, packet(feed.New, i+1, opposite, p, 2))
		}
		other := packet(feed.New, 10, opposite, 100, 50)
		other.Token = 2
		apply(t, e, other)
		r := apply(t, e, packet(feed.New, 11, side, limit, 7))
		if len(r.Fills) != 3 {
			t.Fatal(r)
		}
		for i, f := range r.Fills {
			if f.Price != want[i] || f.Quantity != 2 {
				t.Fatal(r)
			}
		}
		if len(e.books) != 2 || e.ActiveOrders() != 2 {
			t.Fatal(e.ActiveOrders())
		}
	}
}
func TestReplaceLosesPriorityAndAbsentResubmits(t *testing.T) {
	e := newEngine(t, 10)
	a := packet(feed.New, 1, feed.Buy, 100, 5)
	b := packet(feed.New, 2, feed.Buy, 100, 5)
	apply(t, e, a)
	apply(t, e, b)
	a.Type = feed.Modify
	r := apply(t, e, a)
	if !r.Cancelled {
		t.Fatal(r)
	}
	r = apply(t, e, packet(feed.New, 3, feed.Sell, 100, 6))
	if len(r.Fills) != 2 || r.Fills[0].BuyID != b.OrderID || r.Fills[1].BuyID != a.OrderID {
		t.Fatal(r)
	}
	m := packet(feed.Modify, 3, feed.Sell, 101, 2)
	apply(t, e, m) // incoming3 was fully shadow-executed, M re-enters reported quantity.
	if e.ActiveOrders() != 2 {
		t.Fatal(e.ActiveOrders())
	}
	if e.Stats().Replacements != 2 {
		t.Fatal(e.Stats())
	}
}
func TestCancelObserveAndCleanup(t *testing.T) {
	e := newEngine(t, 10)
	a := packet(feed.New, 1, feed.Buy, 100, 5)
	apply(t, e, a)
	tr := feed.Packet{Stream: 1, Sequence: 2, Type: feed.Trade, Token: 1, BuyID: a.OrderID, Price: 100, Quantity: 4}
	r := apply(t, e, tr)
	if !r.Ignored || snapshot(t, e, 1).Bids[0].Quantity != 5 {
		t.Fatal(r)
	}
	a.Type = feed.Cancel
	a.Side = feed.Sell
	a.Price = 0
	a.Quantity = 0
	r = apply(t, e, a)
	if !r.Cancelled {
		t.Fatal(r)
	}
	r = apply(t, e, a)
	if r.Cancelled || e.ActiveOrders() != 0 || len(e.books) != 0 {
		t.Fatal(r)
	}
	apply(t, e, feed.Packet{Stream: 1, Type: feed.Heartbeat})
	if e.Stats().Cancels != 2 || e.Stats().ObservedTrades != 1 {
		t.Fatal(e.Stats())
	}
}
func TestCapacityCrossingAndInvalidAtomicity(t *testing.T) {
	e := newEngine(t, 1)
	a := packet(feed.New, 1, feed.Sell, 100, 5)
	apply(t, e, a)
	before := snapshot(t, e, 1)
	stats := e.Stats()
	for _, p := range []feed.Packet{a, packet(feed.New, 2, feed.Buy, 99, 3), packet(feed.Modify, 1, feed.Buy, 100, 1), func() feed.Packet { p := a; p.Type = feed.Cancel; p.Token = 2; return p }(), packet(feed.New, 3, feed.Buy, 100, 0)} {
		if _, err := e.Apply(p); err == nil {
			t.Fatal("accepted invalid command", p)
		}
		if !reflect.DeepEqual(before, snapshot(t, e, 1)) || stats != e.Stats() {
			t.Fatal("mutated on rejection")
		}
	}
	r := apply(t, e, packet(feed.New, 2, feed.Buy, 100, 7))
	if len(r.Fills) != 1 || e.ActiveOrders() != 1 || snapshot(t, e, 1).Bids[0].Quantity != 2 {
		t.Fatal(r)
	}
	if _, err := New(0); err == nil {
		t.Fatal("accepted0cap")
	}
	if _, err := New(1000001); err == nil {
		t.Fatal("acceptedlargecap")
	}
	if _, err := e.Snapshot(1, 0); err == nil {
		t.Fatal("accepteddepth0")
	}
}

// reference is deliberately a sorted slice with linear lookup, independent of engine heaps/FIFO links.
type reference struct{ orders []feed.Packet }

func (r *reference) apply(p feed.Packet) []Fill {
	if p.Type == feed.Trade || p.Type == feed.Heartbeat {
		return nil
	}
	if p.Type == feed.Cancel || p.Type == feed.Modify {
		for i, o := range r.orders {
			if o.OrderID == p.OrderID {
				r.orders = append(r.orders[:i], r.orders[i+1:]...)
				break
			}
		}
		if p.Type == feed.Cancel {
			return nil
		}
	}
	var fills []Fill
	for p.Quantity > 0 {
		best := -1
		for i, o := range r.orders {
			if o.Token != p.Token || o.Side == p.Side {
				continue
			}
			// Deliberately do not reuse the engine's crossing helper in the oracle.
			if (p.Side == feed.Buy && p.Price < o.Price) || (p.Side == feed.Sell && p.Price > o.Price) {
				continue
			}
			if best < 0 || (p.Side == feed.Buy && o.Price < r.orders[best].Price) || (p.Side == feed.Sell && o.Price > r.orders[best].Price) {
				best = i
			}
		}
		if best < 0 {
			break
		}
		o := r.orders[best]
		q := p.Quantity
		if o.Quantity < q {
			q = o.Quantity
		}
		f := Fill{Token: p.Token, Price: o.Price, Quantity: q, BuyID: p.OrderID, SellID: o.OrderID}
		if p.Side == feed.Sell {
			f.BuyID = o.OrderID
			f.SellID = p.OrderID
		}
		fills = append(fills, f)
		p.Quantity -= q
		r.orders[best].Quantity -= q
		if r.orders[best].Quantity == 0 {
			r.orders = append(r.orders[:best], r.orders[best+1:]...)
		}
	}
	if p.Quantity > 0 {
		r.orders = append(r.orders, p)
	}
	return fills
}
func (r *reference) depth(token int32) Depth {
	d := Depth{Bids: []Level{}, Asks: []Level{}}
	levels := map[feed.Side]map[int32]Level{feed.Buy: {}, feed.Sell: {}}
	for _, o := range r.orders {
		if o.Token != token {
			continue
		}
		v := levels[o.Side][o.Price]
		v.Price = o.Price
		v.Quantity += int64(o.Quantity)
		v.Orders++
		levels[o.Side][o.Price] = v
	}
	for _, v := range levels[feed.Buy] {
		d.Bids = append(d.Bids, v)
	}
	for _, v := range levels[feed.Sell] {
		d.Asks = append(d.Asks, v)
	}
	sort.Slice(d.Bids, func(i, j int) bool { return d.Bids[i].Price > d.Bids[j].Price })
	sort.Slice(d.Asks, func(i, j int) bool { return d.Asks[i].Price < d.Asks[j].Price })
	return d
}
func TestTenThousandCommandsAgainstIndependentReference(t *testing.T) {
	e := newEngine(t, 20000)
	ref := &reference{}
	rng := rand.New(rand.NewSource(42))
	id := 1
	for n := 0; n < 10000; n++ {
		side := feed.Buy
		if rng.Intn(2) == 1 {
			side = feed.Sell
		}
		p := packet(feed.New, id, side, int32(95+rng.Intn(11)), int32(1+rng.Intn(20)))
		id++
		p.Token = int32(1 + rng.Intn(3))
		p.Sequence = int32(n + 1)
		choice := rng.Intn(10)
		if len(ref.orders) > 0 && choice < 4 {
			o := ref.orders[rng.Intn(len(ref.orders))]
			p.OrderID = o.OrderID
			p.Token = o.Token
			p.Side = o.Side
			p.Type = feed.Modify
			if choice < 2 {
				p.Type = feed.Cancel
			}
		}
		if choice == 9 {
			p.Type = feed.Trade
			p.BuyID = p.OrderID
			p.SellID = 0
		}
		got := apply(t, e, p)
		want := ref.apply(p)
		if !reflect.DeepEqual(got.Fills, want) {
			t.Fatalf("step%d fills got%+v want%+v", n, got.Fills, want)
		}
		if e.ActiveOrders() != len(ref.orders) {
			t.Fatalf("step%d active mismatch", n)
		}
		for token := int32(1); token <= 3; token++ {
			if got, want := snapshot(t, e, token), ref.depth(token); !reflect.DeepEqual(got, want) {
				t.Fatalf("step%d token%d got%+v want%+v", n, token, got, want)
			}
		}
	}
}
func BenchmarkMatchRoundTrip(b *testing.B) {
	e, _ := New(2)
	sell := packet(feed.New, 1, feed.Sell, 100, 5)
	buy := packet(feed.New, 2, feed.Buy, 100, 5)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := e.Apply(sell); err != nil {
			b.Fatal(err)
		}
		if _, err := e.Apply(buy); err != nil {
			b.Fatal(err)
		}
	}
}
