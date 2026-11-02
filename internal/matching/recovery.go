package matching

import (
	"fmt"
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
)

// Get returns one active order without allocating a full-book snapshot.
func (e *Engine) Get(id feed.OrderID) (book.Order, bool) {
	o := e.orders[id]
	if o == nil {
		return book.Order{}, false
	}
	return book.Order{ID: o.id, Token: o.token, Side: o.side, Price: o.price, Quantity: o.quantity}, true
}

// Restore loads already committed resting state without executing it a second time.
// Call only on a fresh engine; callers choose a deterministic same-price loading order.
// Arrival priority is deliberately not persisted (ADR-028).
func (e *Engine) Restore(o book.Order) error {
	p := feed.Packet{Stream: 1, Sequence: 1, Type: feed.New, OrderID: o.ID, Token: o.Token, Side: o.Side, Price: o.Price, Quantity: o.Quantity}
	if err := p.Validate(); err != nil {
		return err
	}
	if e.orders[o.ID] != nil {
		return fmt.Errorf("duplicate restored identity")
	}
	if len(e.orders) >= e.maxActive {
		return fmt.Errorf("restored active orders exceed configured capacity")
	}
	if e.canCross(p) {
		return fmt.Errorf("committed matching book is crossed")
	}
	e.rest(p, o.Quantity)
	return nil
}

// Walk visits active orders without allocating an O(N) diagnostic slice.
// Engine callers must provide synchronization; the callback must not mutate it.
func (e *Engine) Walk(visit func(book.Order) error) error {
	for _, o := range e.orders {
		if err := visit(book.Order{ID: o.id, Token: o.token, Side: o.side, Price: o.price, Quantity: o.quantity}); err != nil {
			return err
		}
	}
	return nil
}
