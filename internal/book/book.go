// Package book applies visible-order changes. Its caller supplies transactional atomicity.
package book

import (
	"context"
	"errors"
	"fmt"
	"nse-mtbt-handler/internal/feed"
)

// InvariantError distinguishes invalid source transitions from storage/transport failures.
type InvariantError struct{ Reason string }

func (e *InvariantError) Error() string        { return e.Reason }
func IsInvariant(err error) bool               { var e *InvariantError; return errors.As(err, &e) }
func invalid(format string, args ...any) error { return &InvariantError{fmt.Sprintf(format, args...)} }

type Order struct {
	ID       feed.OrderID `json:"id_bits"`
	Token    int32        `json:"token"`
	Side     feed.Side    `json:"side"`
	Price    int32        `json:"price"`
	Quantity int32        `json:"quantity"`
}
type Store interface {
	Get(context.Context, feed.OrderID) (Order, bool, error)
	Put(context.Context, Order) error
	Delete(context.Context, feed.OrderID) error
}

func Apply(ctx context.Context, s Store, p feed.Packet) error {
	if err := p.Validate(); err != nil {
		return invalid("invalid packet: %v", err)
	}
	if p.Type == feed.Heartbeat {
		return nil
	}
	if p.Type == feed.Trade {
		return trade(ctx, s, p)
	}
	old, found, err := s.Get(ctx, p.OrderID)
	if err != nil {
		return err
	}
	if found && old.Token != p.Token {
		return invalid("order token changed")
	}
	switch p.Type {
	case feed.New:
		if found {
			return invalid("duplicate active order")
		}
		return s.Put(ctx, Order{p.OrderID, p.Token, p.Side, p.Price, p.Quantity})
	case feed.Modify:
		if found && old.Side != p.Side {
			return invalid("order side changed")
		}
		// A previously invisible stop-loss order can first appear as M.
		return s.Put(ctx, Order{p.OrderID, p.Token, p.Side, p.Price, p.Quantity})
	case feed.Cancel:
		// Cancel payload price and quantity may be stale. The stored identity is authoritative.
		if !found {
			return nil
		}
		return s.Delete(ctx, p.OrderID)
	default:
		return invalid("unsupported book action")
	}
}
func trade(ctx context.Context, s Store, p feed.Packet) error {
	type change struct {
		o      Order
		remove bool
	}
	changes := make([]change, 0, 2)
	// Validate BOTH visible sides before writing either, even in nontransactional test stores.
	for _, ref := range []struct {
		id   feed.OrderID
		side feed.Side
	}{{p.BuyID, feed.Buy}, {p.SellID, feed.Sell}} {
		if ref.id == 0 {
			continue
		}
		o, ok, err := s.Get(ctx, ref.id)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if o.Token != p.Token || o.Side != ref.side {
			return invalid("trade reference token/side mismatch")
		}
		if p.Quantity > o.Quantity {
			return invalid("execution exceeds visible remaining quantity")
		}
		o.Quantity -= p.Quantity
		changes = append(changes, change{o, o.Quantity == 0})
	}
	for _, c := range changes {
		var err error
		if c.remove {
			err = s.Delete(ctx, c.o.ID)
		} else {
			err = s.Put(ctx, c.o)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
