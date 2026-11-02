package matching

import (
	"nse-mtbt-handler/internal/book"
	"nse-mtbt-handler/internal/feed"
	"testing"
)

func TestRestoreAndBoundedLookup(t *testing.T) {
	e, _ := New(2)
	id, _ := feed.IDFromFloat64(1)
	o := book.Order{ID: id, Token: 1, Side: feed.Buy, Price: 100, Quantity: 70}
	if err := e.Restore(o); err != nil {
		t.Fatal(err)
	}
	if got, ok := e.Get(id); !ok || got != o {
		t.Fatal(got, ok)
	}
	if err := e.Restore(o); err == nil {
		t.Fatal("duplicate restored")
	}
	other := o
	other.ID, _ = feed.IDFromFloat64(2)
	other.Side = feed.Sell
	if err := e.Restore(other); err == nil {
		t.Fatal("crossed database restored")
	}
	other.Price = 110
	if err := e.Restore(other); err != nil {
		t.Fatal(err)
	}
	if err := e.Walk(func(got book.Order) error {
		if got.Quantity != 70 {
			t.Error(got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	third := other
	third.ID, _ = feed.IDFromFloat64(3)
	if err := e.Restore(third); err == nil {
		t.Fatal("capacity bypass")
	}
	if e.Stats().Executions != 0 || e.Stats().NewOrders != 0 {
		t.Fatal("restore executed orders")
	}
}
