package feed

import "testing"

var benchmarkPacket Packet
var benchmarkBytes []byte

func BenchmarkDecodeOrder(b *testing.B) {
	id, _ := IDFromFloat64(12345)
	raw, err := Encode(Packet{Stream: 1, Sequence: 1, Type: New, Timestamp: 1476921600000000000, OrderID: id, Token: 900001, Side: Buy, Price: 100000, Quantity: 100})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, e := Decode(raw)
		if e != nil {
			b.Fatal(e)
		}
		benchmarkPacket = p
	}
}
func BenchmarkEncodeOrder(b *testing.B) {
	id, _ := IDFromFloat64(12345)
	p := Packet{Stream: 1, Sequence: 1, Type: New, Timestamp: 1476921600000000000, OrderID: id, Token: 900001, Side: Buy, Price: 100000, Quantity: 100}
	b.ReportAllocs()
	b.SetBytes(38)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, e := Encode(p)
		if e != nil {
			b.Fatal(e)
		}
		benchmarkBytes = raw
	}
}
