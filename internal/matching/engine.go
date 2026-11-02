// Package matching implements a synthetic price-time shadow simulation, NOT an NSE execution engine.
// Its caller owns sequencing, catalog validation and synchronization.
package matching

import (
	"container/heap"
	"fmt"
	"nse-mtbt-handler/internal/feed"
	"sort"
)

type Fill struct {
	BuyID    feed.OrderID `json:"buy_id_bits,string"`
	SellID   feed.OrderID `json:"sell_id_bits,string"`
	Token    int32        `json:"token"`
	Price    int32        `json:"price"`
	Quantity int32        `json:"quantity"`
}
type Result struct {
	Fills     []Fill `json:"fills"`
	Cancelled bool   `json:"cancelled"`
	Ignored   bool   `json:"ignored"`
}
type Level struct {
	Price    int32 `json:"price"`
	Quantity int64 `json:"quantity"`
	Orders   int   `json:"orders"`
}
type Depth struct {
	Bids []Level `json:"bids"`
	Asks []Level `json:"asks"`
}
type Stats struct {
	NewOrders        uint64 `json:"new_orders"`
	Replacements     uint64 `json:"replacements"`
	Cancels          uint64 `json:"cancels"`
	ObservedTrades   uint64 `json:"observed_exchange_trades"`
	Executions       uint64 `json:"shadow_executions"`
	ExecutedQuantity uint64 `json:"shadow_executed_quantity"`
}
type entry struct {
	id              feed.OrderID
	token           int32
	side            feed.Side
	price, quantity int32
	level           *priceLevel
	prev, next      *entry
}
type priceLevel struct {
	price        int32
	quantity     int64
	count, index int
	head, tail   *entry
	owner        *sideBook
}
type priceHeap struct {
	levels []*priceLevel
	buy    bool
}

func (h priceHeap) Len() int { return len(h.levels) }
func (h priceHeap) Less(i, j int) bool {
	if h.buy {
		return h.levels[i].price > h.levels[j].price
	}
	return h.levels[i].price < h.levels[j].price
}
func (h priceHeap) Swap(i, j int) {
	h.levels[i], h.levels[j] = h.levels[j], h.levels[i]
	h.levels[i].index = i
	h.levels[j].index = j
}
func (h *priceHeap) Push(v any) {
	l := v.(*priceLevel)
	l.index = len(h.levels)
	h.levels = append(h.levels, l)
}
func (h *priceHeap) Pop() any {
	n := len(h.levels) - 1
	l := h.levels[n]
	h.levels[n] = nil
	h.levels = h.levels[:n]
	l.index = -1
	return l
}

type sideBook struct {
	prices map[int32]*priceLevel
	queue  priceHeap
}
type instrumentBook struct{ bids, asks sideBook }
type Engine struct {
	orders    map[feed.OrderID]*entry
	books     map[int32]*instrumentBook
	maxActive int
	stats     Stats
}

func New(maxActive int) (*Engine, error) {
	if maxActive < 1 || maxActive > 1000000 {
		return nil, fmt.Errorf("max-active must be1..1000000")
	}
	return &Engine{orders: make(map[feed.OrderID]*entry), books: make(map[int32]*instrumentBook), maxActive: maxActive}, nil
}
func (e *Engine) ActiveOrders() int { return len(e.orders) }
func (e *Engine) Stats() Stats      { return e.stats }
func (e *Engine) book(token int32) *instrumentBook {
	b := e.books[token]
	if b == nil {
		b = &instrumentBook{bids: sideBook{prices: make(map[int32]*priceLevel), queue: priceHeap{buy: true}}, asks: sideBook{prices: make(map[int32]*priceLevel)}}
		e.books[token] = b
	}
	return b
}
func (e *Engine) opposite(token int32, side feed.Side) *sideBook {
	b := e.books[token]
	if b == nil {
		return nil
	}
	if side == feed.Buy {
		return &b.asks
	}
	return &b.bids
}
func crosses(side feed.Side, limit, resting int32) bool {
	if side == feed.Buy {
		return limit >= resting
	}
	return limit <= resting
}
func (e *Engine) canCross(p feed.Packet) bool {
	s := e.opposite(p.Token, p.Side)
	return s != nil && s.queue.Len() > 0 && crosses(p.Side, p.Price, s.queue.levels[0].price)
}
func (e *Engine) remove(o *entry) {
	l := o.level
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	l.count--
	l.quantity -= int64(o.quantity)
	delete(e.orders, o.id)
	o.prev = nil
	o.next = nil
	o.level = nil
	if l.count == 0 {
		delete(l.owner.prices, l.price)
		heap.Remove(&l.owner.queue, l.index)
	}
	b := e.books[o.token]
	if len(b.bids.prices) == 0 && len(b.asks.prices) == 0 {
		delete(e.books, o.token)
	}
}
func (e *Engine) rest(p feed.Packet, quantity int32) {
	b := e.book(p.Token)
	s := &b.bids
	if p.Side == feed.Sell {
		s = &b.asks
	}
	l := s.prices[p.Price]
	if l == nil {
		l = &priceLevel{price: p.Price, owner: s}
		s.prices[p.Price] = l
		heap.Push(&s.queue, l)
	}
	o := &entry{id: p.OrderID, token: p.Token, side: p.Side, price: p.Price, quantity: quantity, level: l, prev: l.tail}
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.count++
	l.quantity += int64(quantity)
	e.orders[o.id] = o
}

// Apply rejects all validation/capacity errors before mutation. A crossing command at capacity
// either fully executes or removes at least one resting order before adding its remainder.
func (e *Engine) Apply(p feed.Packet) (Result, error) {
	if err := p.Validate(); err != nil {
		return Result{}, err
	}
	old := e.orders[p.OrderID]
	if p.Type == feed.New && old != nil {
		return Result{}, fmt.Errorf("duplicate active shadow order")
	}
	if (p.Type == feed.New || p.Type == feed.Modify || p.Type == feed.Cancel) && old != nil {
		if old.token != p.Token {
			return Result{}, fmt.Errorf("active shadow order token changed")
		}
		if p.Type == feed.Modify && old.side != p.Side {
			return Result{}, fmt.Errorf("active shadow order side changed")
		}
	}
	if (p.Type == feed.New || p.Type == feed.Modify) && old == nil && len(e.orders) >= e.maxActive && !e.canCross(p) {
		return Result{}, fmt.Errorf("shadow active-order capacity reached")
	}
	r := Result{}
	switch p.Type {
	case feed.Heartbeat:
		r.Ignored = true
		return r, nil
	case feed.Trade:
		e.stats.ObservedTrades++
		r.Ignored = true
		return r, nil
	case feed.Cancel:
		e.stats.Cancels++
		if old != nil {
			e.remove(old)
			r.Cancelled = true
		}
		return r, nil
	case feed.Modify:
		e.stats.Replacements++
		if old != nil {
			e.remove(old)
			r.Cancelled = true
		}
	case feed.New:
		e.stats.NewOrders++
	}
	remaining := p.Quantity
	for remaining > 0 {
		s := e.opposite(p.Token, p.Side)
		if s == nil || s.queue.Len() == 0 || !crosses(p.Side, p.Price, s.queue.levels[0].price) {
			break
		}
		o := s.queue.levels[0].head
		q := remaining
		if o.quantity < q {
			q = o.quantity
		}
		f := Fill{Token: p.Token, Price: o.price, Quantity: q, BuyID: p.OrderID, SellID: o.id}
		if p.Side == feed.Sell {
			f.BuyID = o.id
			f.SellID = p.OrderID
		}
		r.Fills = append(r.Fills, f)
		e.stats.Executions++
		e.stats.ExecutedQuantity += uint64(q)
		remaining -= q
		if q == o.quantity {
			e.remove(o)
		} else {
			o.quantity -= q
			o.level.quantity -= int64(q)
		}
	}
	if remaining > 0 {
		e.rest(p, remaining)
	}
	return r, nil
}

// Snapshot selects at most depth levels with bounded output allocation. It scans price levels
// (not orders), O(L*depth); expensive reads hold the caller's lock and are not a latency promise.
func (e *Engine) Snapshot(token int32, depth int) (Depth, error) {
	if depth < 1 || depth > 100 {
		return Depth{}, fmt.Errorf("depth must be1..100")
	}
	d := Depth{Bids: []Level{}, Asks: []Level{}}
	b := e.books[token]
	if b == nil {
		return d, nil
	}
	selectLevels := func(s *sideBook, buy bool) []Level {
		out := make([]Level, 0, depth)
		for _, l := range s.prices {
			i := sort.Search(len(out), func(i int) bool {
				if buy {
					return out[i].Price <= l.price
				}
				return out[i].Price >= l.price
			})
			if i >= depth {
				continue
			}
			v := Level{l.price, l.quantity, l.count}
			if len(out) < depth {
				out = append(out, Level{})
			}
			copy(out[i+1:], out[i:len(out)-1])
			out[i] = v
		}
		return out
	}
	d.Bids = selectLevels(&b.bids, true)
	d.Asks = selectLevels(&b.asks, false)
	return d, nil
}
