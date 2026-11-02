package feed

import (
	"bytes"
	"math"
	"testing"
)

// TestOrderIDFromFloat64ValidPositive tests lossless conversion of positive finite doubles.
func TestOrderIDFromFloat64ValidPositive(t *testing.T) {
	tests := []float64{
		0.0,
		1.0,
		12345.0,
		1e10,
		math.MaxFloat64 / 2, // Large but finite
	}
	for _, f := range tests {
		oid, err := IDFromFloat64(f)
		if err != nil {
			t.Errorf("IDFromFloat64(%v) failed: %v", f, err)
			continue
		}
		back := oid.Float64()
		if back != f {
			t.Errorf("IDFromFloat64(%v).Float64() = %v, want %v", f, back, f)
		}
	}
}

// TestOrderIDFromFloat64Negative rejects negative and -0.0.
func TestOrderIDFromFloat64Negative(t *testing.T) {
	tests := []float64{
		-1.0,
		math.Copysign(0, -1),
		-1e10,
	}
	for _, f := range tests {
		_, err := IDFromFloat64(f)
		if err == nil {
			t.Errorf("IDFromFloat64(%v) should reject negative, but succeeded", f)
		}
	}
}

// TestOrderIDFromFloat64NonFinite rejects Inf and NaN.
func TestOrderIDFromFloat64NonFinite(t *testing.T) {
	tests := []float64{
		math.Inf(1),
		math.Inf(-1),
		math.NaN(),
	}
	for _, f := range tests {
		_, err := IDFromFloat64(f)
		if err == nil {
			t.Errorf("IDFromFloat64(%v) should reject non-finite, but succeeded", f)
		}
	}
}

// TestOrderIDBytes round-trip.
func TestOrderIDBytes(t *testing.T) {
	tests := []float64{0.0, 1.0, 42.0, 1e6}
	for _, f := range tests {
		oid, _ := IDFromFloat64(f)
		buf := oid.Bytes()
		oid2, err := IDFromBytes(buf)
		if err != nil {
			t.Errorf("IDFromBytes failed: %v", err)
			continue
		}
		if oid != oid2 {
			t.Errorf("Bytes round-trip failed for %v", f)
		}
	}
}

// TestIDFromBytesInvalidLength.
func TestIDFromBytesInvalidLength(t *testing.T) {
	tests := [][]byte{
		nil,
		{1, 2, 3},
		{1, 2, 3, 4, 5, 6, 7},
		{1, 2, 3, 4, 5, 6, 7, 8, 9},
	}
	for _, buf := range tests {
		_, err := IDFromBytes(buf)
		if err == nil {
			t.Errorf("IDFromBytes(%d bytes) should reject, but succeeded", len(buf))
		}
	}
}

// TestEncodeOrderNew: valid N packet.
func TestEncodeOrderNew(t *testing.T) {
	oid, _ := IDFromFloat64(123.0)
	p := Packet{
		Stream:    1,
		Sequence:  5,
		Type:      New,
		Timestamp: 1000000000,
		OrderID:   oid,
		Token:     101,
		Side:      Buy,
		Price:     10000,
		Quantity:  100,
	}
	buf, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if len(buf) != 38 {
		t.Errorf("N packet length should be 38, got %d", len(buf))
	}
	// Verify round-trip
	p2, err := Decode(buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	if p2.Type != New || p2.Stream != 1 || p2.Sequence != 5 {
		t.Errorf("Order round-trip failed: %+v vs %+v", p, p2)
	}
}

// TestEncodeOrderCancel: X with nonnegative price/qty.
func TestEncodeOrderCancel(t *testing.T) {
	oid, _ := IDFromFloat64(456.0)
	p := Packet{
		Stream:    2,
		Sequence:  10,
		Type:      Cancel,
		Timestamp: 2000000000,
		OrderID:   oid,
		Token:     102,
		Side:      Sell,
		Price:     0, // stale, allowed for cancel
		Quantity:  0,
	}
	buf, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode cancel failed: %v", err)
	}
	p2, _ := Decode(buf)
	if p2.Type != Cancel {
		t.Errorf("Cancel type mismatch")
	}
}

// TestEncodeOrderNegativePrice rejects invalid orders.
func TestEncodeOrderNegativePrice(t *testing.T) {
	oid, _ := IDFromFloat64(1.0)
	tests := []struct {
		name  string
		typ   Kind
		price int32
		qty   int32
	}{
		{"N negative price", New, -1, 100},
		{"M zero price", Modify, 0, 100},
		{"N zero quantity", New, 100, 0},
	}
	for _, tc := range tests {
		_, err := Encode(Packet{
			Stream:    1,
			Sequence:  0,
			Type:      tc.typ,
			Timestamp: 0,
			OrderID:   oid,
			Token:     1,
			Side:      Buy,
			Price:     tc.price,
			Quantity:  tc.qty,
		})
		if err == nil {
			t.Errorf("%s should reject but succeeded", tc.name)
		}
	}
}

// TestEncodeTrade: valid T packet.
func TestEncodeTrade(t *testing.T) {
	buyID, _ := IDFromFloat64(100.0)
	sellID, _ := IDFromFloat64(200.0)
	p := Packet{
		Stream:    1,
		Sequence:  20,
		Type:      Trade,
		Timestamp: 3000000000,
		BuyID:     buyID,
		SellID:    sellID,
		Token:     103,
		Price:     10500,
		Quantity:  50,
	}
	buf, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode trade failed: %v", err)
	}
	if len(buf) != 45 {
		t.Errorf("T packet length should be 45, got %d", len(buf))
	}
	p2, _ := Decode(buf)
	if p2.Type != Trade || p2.BuyID != buyID || p2.SellID != sellID {
		t.Errorf("Trade round-trip failed")
	}
}

// TestEncodeTradeZeroIDs: zeros allowed.
func TestEncodeTradeZeroIDs(t *testing.T) {
	p := Packet{
		Stream:    1,
		Sequence:  21,
		Type:      Trade,
		Timestamp: 4000000000,
		BuyID:     0,
		SellID:    0,
		Token:     104,
		Price:     10500,
		Quantity:  25,
	}
	buf, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode trade with zero IDs failed: %v", err)
	}
	p2, _ := Decode(buf)
	if p2.BuyID != 0 || p2.SellID != 0 {
		t.Errorf("Zero trade IDs not preserved")
	}
}

// TestEncodeTradeEqualNonzeroIDs rejects same buy/sell.
func TestEncodeTradeEqualNonzeroIDs(t *testing.T) {
	id, _ := IDFromFloat64(100.0)
	p := Packet{
		Stream:    1,
		Sequence:  22,
		Type:      Trade,
		Timestamp: 5000000000,
		BuyID:     id,
		SellID:    id, // same as buy
		Token:     105,
		Price:     11000,
		Quantity:  30,
	}
	_, err := Encode(p)
	if err == nil {
		t.Errorf("Trade with equal buy/sell IDs should reject")
	}
}

// TestEncodeHeartbeat: valid Z packet.
func TestEncodeHeartbeat(t *testing.T) {
	p := Packet{
		Stream:       1,
		Sequence:     0, // required for heartbeat
		Type:         Heartbeat,
		Timestamp:    0,
		LastSequence: 100,
	}
	buf, err := Encode(p)
	if err != nil {
		t.Fatalf("Encode heartbeat failed: %v", err)
	}
	if len(buf) != 13 {
		t.Errorf("Z packet length should be 13, got %d", len(buf))
	}
	p2, _ := Decode(buf)
	if p2.Type != Heartbeat || p2.LastSequence != 100 {
		t.Errorf("Heartbeat round-trip failed")
	}
}

// TestEncodeHeartbeatSequenceNonzero rejects invalid heartbeat.
func TestEncodeHeartbeatSequenceNonzero(t *testing.T) {
	p := Packet{
		Stream:       1,
		Sequence:     5, // should be 0
		Type:         Heartbeat,
		LastSequence: 100,
	}
	_, err := Encode(p)
	if err == nil {
		t.Errorf("Heartbeat with non-zero sequence should reject")
	}
}

// TestDecodeOrderInvalidStream rejects stream <= 0.
func TestDecodeOrderInvalidStream(t *testing.T) {
	buf := make([]byte, 38)
	// Length = 38
	buf[0], buf[1] = 38, 0
	// Stream = 0 (invalid)
	buf[2], buf[3] = 0, 0
	// Sequence = 0
	buf[4], buf[5], buf[6], buf[7] = 0, 0, 0, 0
	// Type = N
	buf[8] = 'N'

	_, err := Decode(buf)
	if err == nil {
		t.Errorf("Decode should reject stream <= 0")
	}
}

// TestDecodeInvalidLength.
func TestDecodeInvalidLength(t *testing.T) {
	tests := []struct {
		name   string
		typ    Kind
		length uint16
	}{
		{"N wrong length", New, 30},
		{"T wrong length", Trade, 40},
		{"Z wrong length", Heartbeat, 10},
	}
	for _, tc := range tests {
		buf := make([]byte, tc.length)
		// Set header length to match buffer
		buf[0] = byte(tc.length)
		buf[1] = byte(tc.length >> 8)
		// Valid stream
		buf[2], buf[3] = 1, 0
		// Sequence = 0 (ok for heartbeat)
		buf[4], buf[5], buf[6], buf[7] = 0, 0, 0, 0
		// Type
		buf[8] = byte(tc.typ)

		_, err := Decode(buf)
		if err == nil {
			t.Errorf("%s should reject", tc.name)
		}
	}
}

// TestMalformedPacketTooShort.
func TestMalformedPacketTooShort(t *testing.T) {
	_, err := Decode([]byte{1, 2, 3})
	if err == nil {
		t.Errorf("Decode should reject packet < 9 bytes")
	}
}

// TestEncodedBytesHaveCorrectOffsets: verify golden offsets match documentation.
func TestEncodedBytesHaveCorrectOffsets(t *testing.T) {
	// Order packet with known values
	oid, _ := IDFromFloat64(123.456)
	p := Packet{
		Stream:    1,
		Sequence:  42,
		Type:      New,
		Timestamp: 987654321,
		OrderID:   oid,
		Token:     555,
		Side:      Buy,
		Price:     10000,
		Quantity:  200,
	}
	buf, _ := Encode(p)

	// Verify header
	if bytes.Compare(buf[0:2], []byte{38, 0}) != 0 {
		t.Errorf("Length offset mismatch")
	}
	if int16(buf[2])|int16(buf[3])<<8 != 1 {
		t.Errorf("Stream offset mismatch at [2:4]")
	}

	// Verify type byte at offset 8
	if buf[8] != 'N' {
		t.Errorf("Type byte should be at offset 8")
	}

	// Verify that side is at offset 29 (after 8-byte timestamp, 8-byte OrderID, 4-byte token)
	if buf[29] != 'B' {
		t.Errorf("Side byte should be at offset 29")
	}
}

// TestRoundTripPreservesBytes: Encode/Decode preserves all fields.
func TestRoundTripPreservesBytes(t *testing.T) {
	tests := []Packet{
		{
			Stream:    5,
			Sequence:  100,
			Type:      New,
			Timestamp: 1234567890123,
			OrderID:   func() OrderID { oid, _ := IDFromFloat64(99.99); return oid }(),
			Token:     777,
			Side:      Sell,
			Price:     50000,
			Quantity:  1000,
		},
		{
			Stream:    3,
			Sequence:  50,
			Type:      Trade,
			Timestamp: 9876543210987,
			BuyID:     func() OrderID { oid, _ := IDFromFloat64(88.88); return oid }(),
			SellID:    func() OrderID { oid, _ := IDFromFloat64(77.77); return oid }(),
			Token:     888,
			Price:     25000,
			Quantity:  500,
		},
	}

	for i, orig := range tests {
		buf, err := Encode(orig)
		if err != nil {
			t.Errorf("Test %d: Encode failed: %v", i, err)
			continue
		}

		decoded, err := Decode(buf)
		if err != nil {
			t.Errorf("Test %d: Decode failed: %v", i, err)
			continue
		}

		// Compare key fields
		if decoded.Stream != orig.Stream || decoded.Sequence != orig.Sequence ||
			decoded.Type != orig.Type || decoded.Timestamp != orig.Timestamp {
			t.Errorf("Test %d: header fields mismatch", i)
		}

		if orig.Type == New || orig.Type == Modify || orig.Type == Cancel {
			if decoded.OrderID != orig.OrderID || decoded.Token != orig.Token ||
				decoded.Side != orig.Side || decoded.Price != orig.Price ||
				decoded.Quantity != orig.Quantity {
				t.Errorf("Test %d: order fields mismatch", i)
			}
		} else if orig.Type == Trade {
			if decoded.BuyID != orig.BuyID || decoded.SellID != orig.SellID ||
				decoded.Token != orig.Token || decoded.Price != orig.Price ||
				decoded.Quantity != orig.Quantity {
				t.Errorf("Test %d: trade fields mismatch", i)
			}
		}
	}
}

// TestInvalidStreamNegative and zero.
func TestInvalidStream(t *testing.T) {
	tests := []int16{-1, 0}
	for _, stream := range tests {
		_, err := Encode(Packet{
			Stream:   stream,
			Sequence: 0,
			Type:     New,
			OrderID:  0,
			Side:     Buy,
			Price:    100,
			Quantity: 1,
		})
		if err == nil {
			t.Errorf("Encode should reject stream %d", stream)
		}
	}
}

// TestInvalidSequenceNegative.
func TestInvalidSequence(t *testing.T) {
	_, err := Encode(Packet{
		Stream:   1,
		Sequence: -1,
		Type:     New,
		OrderID:  0,
		Side:     Buy,
		Price:    100,
		Quantity: 1,
	})
	if err == nil {
		t.Errorf("Encode should reject negative sequence")
	}
}

// TestInvalidTimestampNegative.
func TestInvalidTimestamp(t *testing.T) {
	_, err := Encode(Packet{
		Stream:    1,
		Sequence:  0,
		Type:      New,
		Timestamp: -1,
		OrderID:   0,
		Side:      Buy,
		Price:     100,
		Quantity:  1,
	})
	if err == nil {
		t.Errorf("Encode should reject negative timestamp")
	}
}

// TestDoubleLosslessIntegralValues preserves exact integral doubles.
func TestDoubleLosslessIntegralValues(t *testing.T) {
	// Test that small integral values can be represented losslessly as doubles
	tests := []float64{
		1.0,
		100.0,
		1000.0,
		1000000.0,
		9007199254740992.0, // 2^53, max safe integer in double
	}
	for _, f := range tests {
		oid, _ := IDFromFloat64(f)
		back := oid.Float64()
		if back != f {
			t.Errorf("Lossless double failed for %v: got %v", f, back)
		}
	}
}

// BenchmarkEncode benchmarks packet encoding.
func BenchmarkEncode(b *testing.B) {
	oid, _ := IDFromFloat64(123.0)
	p := Packet{
		Stream:    1,
		Sequence:  1,
		Type:      New,
		Timestamp: 1000000000,
		OrderID:   oid,
		Token:     101,
		Side:      Buy,
		Price:     10000,
		Quantity:  100,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Encode(p)
	}
}

// BenchmarkDecode benchmarks packet decoding.
func BenchmarkDecode(b *testing.B) {
	oid, _ := IDFromFloat64(456.0)
	p := Packet{
		Stream:    1,
		Sequence:  1,
		Type:      New,
		Timestamp: 1000000000,
		OrderID:   oid,
		Token:     101,
		Side:      Sell,
		Price:     10500,
		Quantity:  150,
	}
	buf, _ := Encode(p)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Decode(buf)
	}
}

// FuzzEncodeDecode panics if Encode/Decode fails on valid input.
func FuzzEncodeDecode(f *testing.F) {
	f.Add(int16(1), int32(0), byte('N'), int64(0), uint64(0), int32(1), byte('B'), int32(100), int32(1))
	f.Fuzz(func(t *testing.T, stream int16, seq int32, typ byte, ts int64, orderBits uint64, token int32, side byte, price int32, qty int32) {
		if stream <= 0 || seq < 0 || ts < 0 || price <= 0 || qty <= 0 {
			return // skip invalid combinations
		}
		if typ != 'N' && typ != 'M' && typ != 'X' {
			return // focus on orders for this fuzz
		}
		if side != 'B' && side != 'S' {
			return
		}

		oid := OrderID(orderBits)
		// Try to create a valid double; skip if the bits don't form a valid double per our rules
		f64 := oid.Float64()
		if math.IsNaN(f64) || math.IsInf(f64, 0) || f64 < 0 {
			return
		}

		p := Packet{
			Stream:    stream,
			Sequence:  seq,
			Type:      Kind(typ),
			Timestamp: ts,
			OrderID:   oid,
			Token:     token,
			Side:      Side(side),
			Price:     price,
			Quantity:  qty,
		}

		buf, err := Encode(p)
		if err != nil {
			return // encoding might fail for invalid doubles
		}

		decoded, err := Decode(buf)
		if err != nil {
			t.Fatalf("Decode failed after valid Encode: %v", err)
		}

		if decoded.Stream != p.Stream || decoded.Sequence != p.Sequence ||
			decoded.Type != p.Type || decoded.OrderID != p.OrderID {
			t.Fatalf("Fuzz: round-trip failed")
		}
	})
}
