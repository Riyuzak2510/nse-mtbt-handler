package book

import (
	"context"
	"sync"
	"testing"

	"nse-mtbt-handler/internal/feed"
)

// MemoryStore is a simple in-memory implementation of Store for testing.
type MemoryStore struct {
	mu     sync.RWMutex
	orders map[uint64]Order
}

// NewMemoryStore creates a new in-memory order store for testing.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		orders: make(map[uint64]Order),
	}
}

func (m *MemoryStore) Get(ctx context.Context, id feed.OrderID) (Order, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	order, found := m.orders[uint64(id)]
	return order, found, nil
}

func (m *MemoryStore) Put(ctx context.Context, order Order) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.orders[uint64(order.ID)] = order
	return nil
}

func (m *MemoryStore) Delete(ctx context.Context, id feed.OrderID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.orders, uint64(id))
	return nil
}

// Helper to create an OrderID from a float64
func oid(f float64) feed.OrderID {
	id, _ := feed.IDFromFloat64(f)
	return id
}

// Helper to create a Packet
func newPacket(typ feed.Kind, stream int16, seq int32) feed.Packet {
	return feed.Packet{
		Stream:    stream,
		Sequence:  seq,
		Type:      typ,
		Timestamp: 1000,
	}
}

// TestApplyNewValid: successful new order insertion.
func TestApplyNewValid(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	pkt := newPacket(feed.New, 1, 1)
	pkt.OrderID = oid(100.0)
	pkt.Token = 101
	pkt.Side = feed.Buy
	pkt.Price = 10000
	pkt.Quantity = 500

	err := Apply(ctx, store, pkt)
	if err != nil {
		t.Fatalf("Apply N failed: %v", err)
	}

	// Verify order was inserted
	order, found, _ := store.Get(ctx, pkt.OrderID)
	if !found {
		t.Fatalf("Order not found after Apply N")
	}
	if order.Side != feed.Buy || order.Price != 10000 || order.Quantity != 500 {
		t.Errorf("Order fields mismatch: %+v", order)
	}
}

// TestApplyNewDuplicate: rejects duplicate order ID.
func TestApplyNewDuplicate(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	pkt := newPacket(feed.New, 1, 1)
	pkt.OrderID = oid(200.0)
	pkt.Token = 102
	pkt.Side = feed.Sell
	pkt.Price = 10500
	pkt.Quantity = 300

	// First insert should succeed
	err := Apply(ctx, store, pkt)
	if err != nil {
		t.Fatalf("First Apply N failed: %v", err)
	}

	// Second insert with same ID should fail
	err = Apply(ctx, store, pkt)
	if err == nil {
		t.Fatalf("Apply N should reject duplicate ID")
	}
}

// TestApplyNewInvalidSide: rejects invalid side.
func TestApplyNewInvalidSide(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	pkt := newPacket(feed.New, 1, 1)
	pkt.OrderID = oid(300.0)
	pkt.Token = 103
	pkt.Side = 'X' // invalid
	pkt.Price = 11000
	pkt.Quantity = 200

	err := Apply(ctx, store, pkt)
	if err == nil {
		t.Fatalf("Apply N should reject invalid side")
	}
}

// TestApplyNewInvalidPrice: rejects non-positive price.
func TestApplyNewInvalidPrice(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	tests := []int32{0, -1}
	for _, price := range tests {
		pkt := newPacket(feed.New, 1, 1)
		pkt.OrderID = oid(300.0 + float64(price))
		pkt.Token = 103
		pkt.Side = feed.Buy
		pkt.Price = price
		pkt.Quantity = 100

		err := Apply(ctx, store, pkt)
		if err == nil {
			t.Errorf("Apply N should reject price %d", price)
		}
	}
}

// TestApplyNewInvalidQuantity: rejects non-positive quantity.
func TestApplyNewInvalidQuantity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	tests := []int32{0, -1}
	for _, qty := range tests {
		pkt := newPacket(feed.New, 1, 1)
		pkt.OrderID = oid(400.0 + float64(qty))
		pkt.Token = 104
		pkt.Side = feed.Buy
		pkt.Price = 10000
		pkt.Quantity = qty

		err := Apply(ctx, store, pkt)
		if err == nil {
			t.Errorf("Apply N should reject quantity %d", qty)
		}
	}
}

// TestApplyModifyExistingOrder: updates existing order.
func TestApplyModifyExistingOrder(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	id := oid(500.0)

	// Insert original
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = id
	pkt1.Token = 105
	pkt1.Side = feed.Buy
	pkt1.Price = 10000
	pkt1.Quantity = 500
	Apply(ctx, store, pkt1)

	// Modify
	pkt2 := newPacket(feed.Modify, 1, 2)
	pkt2.OrderID = id
	pkt2.Token = 105
	pkt2.Side = feed.Buy
	pkt2.Price = 10100  // new price
	pkt2.Quantity = 400 // new quantity

	err := Apply(ctx, store, pkt2)
	if err != nil {
		t.Fatalf("Apply M failed: %v", err)
	}

	// Verify update
	order, _, _ := store.Get(ctx, id)
	if order.Price != 10100 || order.Quantity != 400 {
		t.Errorf("Modify failed to update: price %d, qty %d", order.Price, order.Quantity)
	}
}

// TestApplyModifyMissingOrder: M with missing ID inserts new order.
func TestApplyModifyMissingOrder(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	id := oid(600.0)

	// Modify nonexistent order
	pkt := newPacket(feed.Modify, 1, 1)
	pkt.OrderID = id
	pkt.Token = 106
	pkt.Side = feed.Sell
	pkt.Price = 10500
	pkt.Quantity = 300

	err := Apply(ctx, store, pkt)
	if err != nil {
		t.Fatalf("Apply M (insert) failed: %v", err)
	}

	// Verify order was inserted
	order, found, _ := store.Get(ctx, id)
	if !found {
		t.Fatalf("Order not found after Apply M (insert)")
	}
	if order.Side != feed.Sell {
		t.Errorf("Inserted order has wrong side")
	}
}

// TestApplyCancelExistingOrder: removes order by ID.
func TestApplyCancelExistingOrder(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	id := oid(700.0)

	// Insert order
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = id
	pkt1.Token = 107
	pkt1.Side = feed.Buy
	pkt1.Price = 9500
	pkt1.Quantity = 600
	Apply(ctx, store, pkt1)

	// Cancel
	pkt2 := newPacket(feed.Cancel, 1, 2)
	pkt2.OrderID = id
	pkt2.Token = 107
	pkt2.Side = feed.Buy
	pkt2.Price = 0 // stale, ignored
	pkt2.Quantity = 0

	err := Apply(ctx, store, pkt2)
	if err != nil {
		t.Fatalf("Apply X failed: %v", err)
	}

	// Verify order was deleted
	_, found, _ := store.Get(ctx, id)
	if found {
		t.Fatalf("Order still exists after cancel")
	}
}

// TestApplyCancelMissingOrder: X on missing ID is a no-op.
func TestApplyCancelMissingOrder(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	id := oid(800.0)

	// Cancel nonexistent order
	pkt := newPacket(feed.Cancel, 1, 1)
	pkt.OrderID = id
	pkt.Token = 108
	pkt.Side = feed.Buy
	pkt.Price = 0
	pkt.Quantity = 0

	err := Apply(ctx, store, pkt)
	if err != nil {
		t.Fatalf("Apply X (missing) should not error: %v", err)
	}
}

// TestApplyCancelTokenMismatch: rejects token mismatch.
func TestApplyCancelTokenMismatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	id := oid(900.0)

	// Insert with token 109
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = id
	pkt1.Token = 109
	pkt1.Side = feed.Buy
	pkt1.Price = 9000
	pkt1.Quantity = 700
	Apply(ctx, store, pkt1)

	// Try to cancel with different token
	pkt2 := newPacket(feed.Cancel, 1, 2)
	pkt2.OrderID = id
	pkt2.Token = 110 // mismatch
	pkt2.Side = feed.Buy
	pkt2.Price = 0
	pkt2.Quantity = 0

	err := Apply(ctx, store, pkt2)
	if err == nil {
		t.Fatalf("Apply X should reject token mismatch")
	}
}

// TestApplyTradeReduceBoth: reduces both buy and sell orders.
func TestApplyTradeReduceBoth(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	buyID := oid(1000.0)
	sellID := oid(1001.0)

	// Insert buy order
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = buyID
	pkt1.Token = 110
	pkt1.Side = feed.Buy
	pkt1.Price = 10000
	pkt1.Quantity = 1000
	Apply(ctx, store, pkt1)

	// Insert sell order
	pkt2 := newPacket(feed.New, 1, 2)
	pkt2.OrderID = sellID
	pkt2.Token = 110
	pkt2.Side = feed.Sell
	pkt2.Price = 10000
	pkt2.Quantity = 1000
	Apply(ctx, store, pkt2)

	// Trade
	pkt3 := newPacket(feed.Trade, 1, 3)
	pkt3.BuyID = buyID
	pkt3.SellID = sellID
	pkt3.Token = 110
	pkt3.Price = 10000
	pkt3.Quantity = 500

	err := Apply(ctx, store, pkt3)
	if err != nil {
		t.Fatalf("Apply T failed: %v", err)
	}

	// Verify both orders reduced
	buyOrd, _, _ := store.Get(ctx, buyID)
	if buyOrd.Quantity != 500 {
		t.Errorf("Buy order qty should be 500, got %d", buyOrd.Quantity)
	}
	sellOrd, _, _ := store.Get(ctx, sellID)
	if sellOrd.Quantity != 500 {
		t.Errorf("Sell order qty should be 500, got %d", sellOrd.Quantity)
	}
}

// TestApplyTradeRemoveAtZero: removes order when reduced to zero.
func TestApplyTradeRemoveAtZero(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	buyID := oid(1100.0)

	// Insert order
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = buyID
	pkt1.Token = 111
	pkt1.Side = feed.Buy
	pkt1.Price = 10500
	pkt1.Quantity = 100
	Apply(ctx, store, pkt1)

	// Trade for exactly all of it
	pkt2 := newPacket(feed.Trade, 1, 2)
	pkt2.BuyID = buyID
	pkt2.SellID = 0
	pkt2.Token = 111
	pkt2.Price = 10500
	pkt2.Quantity = 100

	err := Apply(ctx, store, pkt2)
	if err != nil {
		t.Fatalf("Apply T (remove at zero) failed: %v", err)
	}

	// Verify order removed
	_, found, _ := store.Get(ctx, buyID)
	if found {
		t.Fatalf("Order should be removed at zero quantity")
	}
}

// TestApplyTradeZeroReferences: ignores zero buy/sell IDs.
func TestApplyTradeZeroReferences(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	// Trade with both zero IDs (unknown orders)
	pkt := newPacket(feed.Trade, 1, 1)
	pkt.BuyID = 0
	pkt.SellID = 0
	pkt.Token = 112
	pkt.Price = 10000
	pkt.Quantity = 50

	err := Apply(ctx, store, pkt)
	if err != nil {
		t.Fatalf("Apply T (zero IDs) should succeed: %v", err)
	}
}

// TestApplyTradeUnknownOrder: ignores unknown order IDs.
func TestApplyTradeUnknownOrder(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	unknownID := oid(2000.0)

	// Trade with unknown order
	pkt := newPacket(feed.Trade, 1, 1)
	pkt.BuyID = unknownID
	pkt.SellID = 0
	pkt.Token = 113
	pkt.Price = 10000
	pkt.Quantity = 100

	err := Apply(ctx, store, pkt)
	if err != nil {
		t.Fatalf("Apply T (unknown order) should not error: %v", err)
	}
}

// TestApplyTradeSideMismatch: rejects if reference is wrong side.
func TestApplyTradeSideMismatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	orderID := oid(2100.0)

	// Insert SELL order
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = orderID
	pkt1.Token = 114
	pkt1.Side = feed.Sell
	pkt1.Price = 10000
	pkt1.Quantity = 100
	Apply(ctx, store, pkt1)

	// Try to trade it as buy
	pkt2 := newPacket(feed.Trade, 1, 2)
	pkt2.BuyID = orderID // but it's actually a sell order
	pkt2.SellID = 0
	pkt2.Token = 114
	pkt2.Price = 10000
	pkt2.Quantity = 50

	err := Apply(ctx, store, pkt2)
	if err == nil {
		t.Fatalf("Apply T should reject side mismatch")
	}
}

// TestApplyTradeNegativeQuantity: rejects negative result.
func TestApplyTradeNegativeQuantity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	orderID := oid(2200.0)

	// Insert order with qty 50
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = orderID
	pkt1.Token = 115
	pkt1.Side = feed.Buy
	pkt1.Price = 10000
	pkt1.Quantity = 50
	Apply(ctx, store, pkt1)

	// Try to trade 100 (would go negative)
	pkt2 := newPacket(feed.Trade, 1, 2)
	pkt2.BuyID = orderID
	pkt2.SellID = 0
	pkt2.Token = 115
	pkt2.Price = 10000
	pkt2.Quantity = 100

	err := Apply(ctx, store, pkt2)
	if err == nil {
		t.Fatalf("Apply T should reject quantity going negative")
	}
}

// TestApplyTradeTokenMismatch: rejects token mismatch.
func TestApplyTradeTokenMismatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	orderID := oid(2300.0)

	// Insert order with token 116
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = orderID
	pkt1.Token = 116
	pkt1.Side = feed.Buy
	pkt1.Price = 10000
	pkt1.Quantity = 100
	Apply(ctx, store, pkt1)

	// Try to trade with different token
	pkt2 := newPacket(feed.Trade, 1, 2)
	pkt2.BuyID = orderID
	pkt2.SellID = 0
	pkt2.Token = 117 // mismatch
	pkt2.Price = 10000
	pkt2.Quantity = 50

	err := Apply(ctx, store, pkt2)
	if err == nil {
		t.Fatalf("Apply T should reject token mismatch")
	}
}

// TestApplyHeartbeat: heartbeat does not change book.
func TestApplyHeartbeat(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	// Insert an order
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = oid(3000.0)
	pkt1.Token = 118
	pkt1.Side = feed.Buy
	pkt1.Price = 10000
	pkt1.Quantity = 100
	Apply(ctx, store, pkt1)

	// Apply heartbeat
	pkt2 := newPacket(feed.Heartbeat, 1, 0)
	pkt2.LastSequence = 100

	err := Apply(ctx, store, pkt2)
	if err != nil {
		t.Fatalf("Apply Z failed: %v", err)
	}

	// Verify order still exists and is unchanged
	order, found, _ := store.Get(ctx, oid(3000.0))
	if !found || order.Quantity != 100 {
		t.Fatalf("Heartbeat changed book state")
	}
}

// TestApplySequenceOfOperations: realistic order book sequence.
func TestApplySequenceOfOperations(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	// 1. New buy order
	pkt1 := newPacket(feed.New, 1, 1)
	pkt1.OrderID = oid(100.0)
	pkt1.Token = 1
	pkt1.Side = feed.Buy
	pkt1.Price = 10000
	pkt1.Quantity = 1000
	Apply(ctx, store, pkt1)

	// 2. New sell order
	pkt2 := newPacket(feed.New, 1, 2)
	pkt2.OrderID = oid(200.0)
	pkt2.Token = 1
	pkt2.Side = feed.Sell
	pkt2.Price = 10100
	pkt2.Quantity = 500
	Apply(ctx, store, pkt2)

	// 3. Trade
	pkt3 := newPacket(feed.Trade, 1, 3)
	pkt3.BuyID = oid(100.0)
	pkt3.SellID = oid(200.0)
	pkt3.Token = 1
	pkt3.Price = 10050
	pkt3.Quantity = 300
	Apply(ctx, store, pkt3)

	// 4. Modify remaining buy
	pkt4 := newPacket(feed.Modify, 1, 4)
	pkt4.OrderID = oid(100.0)
	pkt4.Token = 1
	pkt4.Side = feed.Buy
	pkt4.Price = 9950
	pkt4.Quantity = 700
	Apply(ctx, store, pkt4)

	// 5. Cancel sell
	pkt5 := newPacket(feed.Cancel, 1, 5)
	pkt5.OrderID = oid(200.0)
	pkt5.Token = 1
	pkt5.Side = feed.Sell
	pkt5.Price = 0
	pkt5.Quantity = 0
	Apply(ctx, store, pkt5)

	// Verify final state
	buyOrd, found, _ := store.Get(ctx, oid(100.0))
	if !found || buyOrd.Quantity != 700 {
		t.Errorf("Buy order not correctly modified")
	}

	_, found, _ = store.Get(ctx, oid(200.0))
	if found {
		t.Errorf("Sell order should be deleted")
	}
}

// TestApplyInvalidType: unsupported packet type.
func TestApplyInvalidType(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	pkt := newPacket(feed.Kind(0xFF), 1, 1)
	err := Apply(ctx, store, pkt)
	if err == nil {
		t.Fatalf("Apply should reject invalid type")
	}
}

// BenchmarkApplyNew benchmarks new order insertion.
func BenchmarkApplyNew(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	pkt := newPacket(feed.New, 1, 1)
	pkt.Token = 1
	pkt.Side = feed.Buy
	pkt.Price = 10000
	pkt.Quantity = 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pkt.OrderID = oid(float64(i))
		Apply(ctx, store, pkt)
	}
}

// BenchmarkApplyTrade benchmarks trade execution.
func BenchmarkApplyTrade(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()

	// Pre-insert orders
	for i := 0; i < 1000; i++ {
		pkt := newPacket(feed.New, 1, int32(i+1))
		pkt.OrderID = oid(float64(i * 2))
		pkt.Token = 1
		pkt.Side = feed.Buy
		pkt.Price = 10000
		pkt.Quantity = 1000
		Apply(ctx, store, pkt)
	}

	tradePkt := newPacket(feed.Trade, 1, 2000)
	tradePkt.Token = 1
	tradePkt.Price = 10000
	tradePkt.Quantity = 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tradePkt.BuyID = oid(float64(i % 1000 * 2))
		tradePkt.SellID = 0
		Apply(ctx, store, tradePkt)
	}
}
