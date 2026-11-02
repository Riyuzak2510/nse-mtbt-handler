package outcomes

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math"
	"nse-mtbt-handler/internal/feed"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) Event {
	t.Helper()
	o := OrderSnapshot{"000000000000f03f", 1, "B", 100, "OPEN", "100", "0", "0", 100}
	e, err := New(1, 10, 11, feed.New, "NEW", o, time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func TestSerializationAndWireKey(t *testing.T) {
	e := fixture(t)
	b, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := Encode(e)
	if !bytes.Equal(b, again) {
		t.Fatal("unstable bytes")
	}
	id, _ := feed.IDFromFloat64(1)
	if e.EventID != EventID(1, 10, id) {
		t.Fatal(e.EventID)
	}
	v, err := ValidateKey(b, 1, 10, id.Bytes())
	if err != nil || v != e {
		t.Fatal(v, err)
	}
	k, _ := e.GetKey()
	if hex.EncodeToString(k.OrderIDBytes[:]) != "000000000000f03f" {
		t.Fatal(k)
	}
	for _, bad := range []struct {
		s int16
		q int32
		b []byte
	}{{2, 10, id.Bytes()}, {1, 11, id.Bytes()}, {1, 10, []byte{1}}} {
		if _, err = ValidateKey(b, bad.s, bad.q, bad.b); err == nil {
			t.Fatal("bad key accepted")
		}
	}
}
func TestValidationRejectsInvalidFields(t *testing.T) {
	tests := []func(*Event){func(e *Event) { e.SchemaVersion = 2 }, func(e *Event) { e.EventID = "bad" }, func(e *Event) { e.Synthetic = false }, func(e *Event) { e.OrderVersion = 9 }, func(e *Event) { e.InputType = "T" }, func(e *Event) { e.Cause = "CANCEL" }, func(e *Event) { e.InputBrokerSequence = "01" }, func(e *Event) { e.InputBrokerSequence = "0" }, func(e *Event) { e.RecordedAt = "bad" }, func(e *Event) { e.Order.AcceptedQty = "0100" }, func(e *Event) { e.Order.FilledQty = "-1" }, func(e *Event) { e.Order.AcceptedQty = "101" }, func(e *Event) { e.Order.Side = "X" }, func(e *Event) { e.Order.OrderIDHex = "0000000000000000" }, func(e *Event) { e.Order.Status = "FILLED" }, func(e *Event) { e.Order.RemainingQty = -1 }}
	for i, mutate := range tests {
		e := fixture(t)
		mutate(&e)
		raw, _ := json.Marshal(e)
		if _, err := Decode(raw); err == nil {
			t.Fatalf("accepted case%d", i)
		}
	}
	if _, err := Decode([]byte(strings.Repeat("x", 8193))); err == nil {
		t.Fatal("oversize")
	}
}
func TestLifecycleConservationAndPrecision(t *testing.T) {
	for _, o := range []OrderSnapshot{{"000000000000f03f", 1, "B", 100, "PARTIALLY_FILLED", "100", "30", "0", 70}, {"000000000000f03f", 1, "B", 100, "FILLED", "150", "100", "50", 0}, {"000000000000f03f", 1, "B", 100, "CANCELLED", "150", "30", "120", 0}, {"000000000000f03f", 1, "B", 100, "FILLED", "9223372036854775807", "9223372036854775807", "0", 0}} {
		e := fixture(t)
		e.Order = o
		if _, err := Encode(e); err != nil {
			t.Fatal(o, err)
		}
	}
	e := fixture(t)
	e.Order.AcceptedQty = "9223372036854775807"
	e.Order.FilledQty = "9223372036854775807"
	e.Order.CancelledQty = "1"
	if _, err := Encode(e); err == nil {
		t.Fatal("overflow")
	}
	id, _ := feed.IDFromFloat64(math.Ldexp(1, 60))
	e = fixture(t)
	e.Order.OrderIDHex = hex.EncodeToString(id.Bytes())
	e.EventID = EventID(1, e.SourceSequence, id)
	e.InputBrokerSequence = "18446744073709551615"
	if _, err := Encode(e); err != nil {
		t.Fatal(err)
	}
}
func TestCausesMatchInputs(t *testing.T) {
	for _, pair := range []struct {
		typ   feed.Kind
		cause string
	}{{feed.New, "NEW"}, {feed.New, "MATCH"}, {feed.Modify, "MODIFY"}, {feed.Modify, "MATCH"}, {feed.Cancel, "CANCEL"}} {
		e := fixture(t)
		_, err := New(1, 1, 1, pair.typ, pair.cause, e.Order, time.Now())
		if err != nil {
			t.Fatal(pair, err)
		}
	}
}
