package feed

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestGoldenOrderAndHeartbeat(t *testing.T) {
	// Manually assembled: length38, stream1, seq1, N, timestamp1, double1, token1, B,100paise,10qty.
	raw, e := hex.DecodeString("26000100010000004e0100000000000000000000000000f03f0100000042640000000a000000")
	if e != nil {
		t.Fatal(e)
	}
	p, e := Decode(raw)
	if e != nil {
		t.Fatal(e)
	}
	if p.OrderID.Float64() != 1 || p.Quantity != 10 || p.Price != 100 || p.Timestamp != 1 || p.Sequence != 1 || p.Token != 1 {
		t.Fatal(p)
	}
	b, e := Encode(p)
	if e != nil || !bytes.Equal(b, raw) {
		t.Fatal("golden order mismatch", e)
	}
	hb, _ := hex.DecodeString("0d000100000000005a64000000")
	p, e = Decode(hb)
	if e != nil || p.LastSequence != 100 || p.Type != Heartbeat {
		t.Fatal(p, e)
	}
}
func FuzzDecodeNoPanic(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{13, 0, 1, 0, 0, 0, 0, 0, 'Z', 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		p, e := Decode(b)
		if e == nil {
			out, e := Encode(p)
			if e != nil || !bytes.Equal(out, b) {
				t.Fatal("noncanonical accepted payload", e)
			}
		}
	})
}
