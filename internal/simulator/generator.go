// Package simulator produces synthetic order events; it is not an exchange matching engine.
package simulator

import (
	"fmt"
	"math"
	"math/rand"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"sort"
)

type Generator struct {
	m      runconfig.Manifest
	rng    *rand.Rand
	seq    int32
	nextID uint64
	orders []book.Order
}

func New(m runconfig.Manifest) (*Generator, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return &Generator{m: m, rng: rand.New(rand.NewSource(m.Seed)), nextID: 1}, nil
}

// Next uses an explicitly supplied timestamp. Identical manifest/seed/timestamps reproduce packets.
func (g *Generator) Next(timestamp int64) (feed.Packet, error) {
	if timestamp < 0 || g.seq == math.MaxInt32 {
		return feed.Packet{}, fmt.Errorf("timestamp invalid or source sequence exhausted; start a new run")
	}
	p := feed.Packet{Stream: g.m.StreamID, Sequence: g.seq + 1, Timestamp: timestamp}
	action := g.rng.Intn(100)
	if len(g.orders) == 0 || (len(g.orders) < 1000 && action < 40) {
		inst := g.m.Instruments[g.rng.Intn(len(g.m.Instruments))]
		if g.nextID > 1<<53 {
			return p, fmt.Errorf("exact synthetic ID range exhausted")
		}
		id, _ := feed.IDFromFloat64(float64(g.nextID))
		g.nextID++
		side := feed.Buy
		if g.rng.Intn(2) == 1 {
			side = feed.Sell
		}
		p.Type = feed.New
		p.OrderID = id
		p.Token = inst.Token
		p.Side = side
		p.Price = inst.BasePrice
		p.Quantity = int32(g.rng.Intn(100) + 1)
		g.orders = append(g.orders, book.Order{ID: id, Token: p.Token, Side: side, Price: p.Price, Quantity: p.Quantity})
	} else {
		i := g.rng.Intn(len(g.orders))
		o := g.orders[i]
		p.Token = o.Token
		p.Price = o.Price
		switch {
		case action < 65:
			p.Type = feed.Modify
			p.OrderID = o.ID
			p.Side = o.Side
			p.Quantity = int32(g.rng.Intn(100) + 1)
			var inst runconfig.Instrument
			for _, x := range g.m.Instruments {
				if x.Token == o.Token {
					inst = x
					break
				}
			}
			step := int64(g.rng.Intn(5)-2) * int64(inst.TickSize)
			price := int64(o.Price) + step
			if price <= 0 || price > math.MaxInt32 {
				price = int64(inst.BasePrice)
			}
			p.Price = int32(price)
			o.Price = p.Price
			o.Quantity = p.Quantity
			g.orders[i] = o
		case action < 80:
			p.Type = feed.Cancel
			p.OrderID = o.ID
			p.Side = o.Side
			p.Quantity = o.Quantity
			g.orders = append(g.orders[:i], g.orders[i+1:]...)
		default:
			p.Type = feed.Trade
			p.Quantity = int32(g.rng.Intn(int(o.Quantity)) + 1)
			// Opposite market side has zero ID; visible side is independently decremented here.
			if o.Side == feed.Buy {
				p.BuyID = o.ID
			} else {
				p.SellID = o.ID
			}
			o.Quantity -= p.Quantity
			if o.Quantity == 0 {
				g.orders = append(g.orders[:i], g.orders[i+1:]...)
			} else {
				g.orders[i] = o
			}
		}
	}
	if err := p.Validate(); err != nil {
		return feed.Packet{}, err
	}
	g.seq++
	return p, nil
}
func (g *Generator) Heartbeat() feed.Packet {
	return feed.Packet{Stream: g.m.StreamID, Type: feed.Heartbeat, LastSequence: g.seq}
}
func (g *Generator) Sequence() int32 { return g.seq }
func (g *Generator) Orders() []book.Order {
	o := append([]book.Order{}, g.orders...)
	sort.Slice(o, func(i, j int) bool { return o[i].ID < o[j].ID })
	return o
}
