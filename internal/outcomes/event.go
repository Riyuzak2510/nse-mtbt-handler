// Package outcomes describes immutable local synthetic order-state notifications.
package outcomes

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"nse-mtbt-handler/internal/feed"
	"strconv"
	"strings"
	"time"
)

const (
	MaxPayloadBytes = 8192
	SchemaVersion   = 1
	EventType       = "order.state_changed"
)

type OrderSnapshot struct {
	OrderIDHex   string `json:"order_id_hex"`
	Token        int32  `json:"token"`
	Side         string `json:"side"`
	PricePaise   int32  `json:"price_paise"`
	Status       string `json:"status"`
	AcceptedQty  string `json:"accepted_quantity"`
	FilledQty    string `json:"filled_quantity"`
	CancelledQty string `json:"cancelled_quantity"`
	RemainingQty int32  `json:"remaining_quantity"`
}
type Event struct {
	SchemaVersion       int32         `json:"schema_version"`
	EventType           string        `json:"event_type"`
	EventID             string        `json:"event_id"`
	Synthetic           bool          `json:"synthetic"`
	StreamID            int16         `json:"stream_id"`
	SourceSequence      int32         `json:"source_sequence"`
	InputBrokerSequence string        `json:"input_broker_sequence"`
	OrderVersion        int32         `json:"order_version"`
	InputType           string        `json:"input_type"`
	Cause               string        `json:"cause"`
	Order               OrderSnapshot `json:"order"`
	RecordedAt          string        `json:"recorded_at"`
}
type EventKey struct {
	StreamID       int16
	SourceSequence int32
	OrderIDBytes   [8]byte
}

func EventID(stream int16, source int32, id feed.OrderID) string {
	return fmt.Sprintf("matching.order-state.v1:%d:%d:%s", stream, source, hex.EncodeToString(id.Bytes()))
}
func New(stream int16, source int32, broker uint64, input feed.Kind, cause string, order OrderSnapshot, recorded time.Time) (Event, error) {
	e := Event{SchemaVersion, EventType, fmt.Sprintf("matching.order-state.v1:%d:%d:%s", stream, source, order.OrderIDHex), true, stream, source, strconv.FormatUint(broker, 10), source, string(input), cause, order, recorded.UTC().Format(time.RFC3339Nano)}
	return e, e.validate()
}
func canonical(s string) (int64, error) {
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil || n < 0 || strconv.FormatInt(n, 10) != s {
		return 0, fmt.Errorf("noncanonical nonnegative quantity")
	}
	return n, nil
}
func validateOrderSnapshot(o OrderSnapshot) error {
	b, e := hex.DecodeString(o.OrderIDHex)
	if e != nil || len(b) != 8 || o.OrderIDHex != strings.ToLower(o.OrderIDHex) {
		return fmt.Errorf("invalid wire order ID")
	}
	id, e := feed.IDFromBytes(b)
	if e != nil {
		return e
	}
	if id == 0 {
		return fmt.Errorf("zero order identity")
	}
	if _, e = feed.IDFromFloat64(id.Float64()); e != nil {
		return e
	}
	if o.Token <= 0 || o.PricePaise <= 0 || o.Side != "B" && o.Side != "S" || o.RemainingQty < 0 {
		return fmt.Errorf("invalid order fields")
	}
	a, e := canonical(o.AcceptedQty)
	if e != nil || a <= 0 {
		return fmt.Errorf("invalid accepted quantity")
	}
	f, e := canonical(o.FilledQty)
	if e != nil {
		return e
	}
	c, e := canonical(o.CancelledQty)
	if e != nil {
		return e
	}
	r := int64(o.RemainingQty)
	if f > math.MaxInt64-c || f+c > math.MaxInt64-r || a != f+c+r {
		return fmt.Errorf("quantity conservation violated")
	}
	switch o.Status {
	case "OPEN":
		if r <= 0 || f != 0 {
			return fmt.Errorf("invalid OPEN")
		}
	case "PARTIALLY_FILLED":
		if r <= 0 || f <= 0 {
			return fmt.Errorf("invalid PARTIALLY_FILLED")
		}
	case "FILLED":
		if r != 0 || f <= 0 {
			return fmt.Errorf("invalid FILLED")
		}
	case "CANCELLED":
		if r != 0 {
			return fmt.Errorf("invalid CANCELLED")
		}
	default:
		return fmt.Errorf("invalid status")
	}
	return nil
}
func (e Event) validate() error {
	if e.SchemaVersion != 1 || e.EventType != EventType || !e.Synthetic || e.StreamID <= 0 || e.SourceSequence <= 0 || e.OrderVersion != e.SourceSequence {
		return fmt.Errorf("invalid event schema/version")
	}
	if e.EventID != fmt.Sprintf("matching.order-state.v1:%d:%d:%s", e.StreamID, e.SourceSequence, e.Order.OrderIDHex) {
		return fmt.Errorf("event ID mismatch")
	}
	b, err := strconv.ParseUint(e.InputBrokerSequence, 10, 64)
	if err != nil || b == 0 || strconv.FormatUint(b, 10) != e.InputBrokerSequence {
		return fmt.Errorf("invalid broker position")
	}
	switch e.InputType {
	case "N":
		if e.Cause != "NEW" && e.Cause != "MATCH" {
			return fmt.Errorf("invalid N cause")
		}
	case "M":
		if e.Cause != "MODIFY" && e.Cause != "MATCH" {
			return fmt.Errorf("invalid M cause")
		}
	case "X":
		if e.Cause != "CANCEL" {
			return fmt.Errorf("invalid X cause")
		}
	default:
		return fmt.Errorf("invalid input type")
	}
	t, err := time.Parse(time.RFC3339Nano, e.RecordedAt)
	if err != nil || t.IsZero() || t.UTC().Format(time.RFC3339Nano) != e.RecordedAt {
		return fmt.Errorf("invalid UTC recorded_at")
	}
	return validateOrderSnapshot(e.Order)
}
func Encode(e Event) ([]byte, error) {
	if err := e.validate(); err != nil {
		return nil, err
	}
	b, err := json.Marshal(e)
	if len(b) > MaxPayloadBytes {
		return nil, fmt.Errorf("outcome payload exceeds8192bytes")
	}
	return b, err
}
func Decode(b []byte) (Event, error) {
	var e Event
	if len(b) == 0 || len(b) > MaxPayloadBytes {
		return e, fmt.Errorf("invalid payload size")
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return e, err
	}
	return e, e.validate()
}
func ValidateKey(b []byte, stream int16, source int32, id []byte) (Event, error) {
	e, err := Decode(b)
	if err != nil {
		return e, err
	}
	if len(id) != 8 || e.StreamID != stream || e.SourceSequence != source || e.Order.OrderIDHex != hex.EncodeToString(id) {
		return e, fmt.Errorf("outbox key/payload mismatch")
	}
	return e, nil
}
func (e Event) GetKey() (EventKey, error) {
	var k EventKey
	if err := e.validate(); err != nil {
		return k, err
	}
	k.StreamID = e.StreamID
	k.SourceSequence = e.SourceSequence
	b, _ := hex.DecodeString(e.Order.OrderIDHex)
	copy(k.OrderIDBytes[:], b)
	return k, nil
}
