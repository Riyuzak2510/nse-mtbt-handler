// Package feed implements the approved synthetic CM profile, not a certified live NSE decoder.
// Fields are packed little endian; timestamps explicitly occupy eight bytes (see ADR-009).
package feed

import (
	"encoding/binary"
	"fmt"
	"math"
)

const Profile = "cm-mtbt-le64-v1"

type Kind byte

const (
	New       Kind = 'N'
	Modify    Kind = 'M'
	Cancel    Kind = 'X'
	Trade     Kind = 'T'
	Heartbeat Kind = 'Z'
)

type Side byte

const (
	Buy  Side = 'B'
	Sell Side = 'S'
)

// OrderID preserves the DOUBLE's bits, rather than interpreting them as an integer ID.
type OrderID uint64

func IDFromFloat64(f float64) (OrderID, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) || math.Signbit(f) {
		return 0, fmt.Errorf("invalid order ID %v", f)
	}
	return OrderID(math.Float64bits(f)), nil
}
func (id OrderID) Float64() float64 { return math.Float64frombits(uint64(id)) }
func (id OrderID) Bytes() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(id))
	return b
}
func IDFromBytes(b []byte) (OrderID, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("order ID needs 8 bytes")
	}
	return IDFromFloat64(math.Float64frombits(binary.LittleEndian.Uint64(b)))
}

type Packet struct {
	Stream                 int16
	Sequence               int32
	Type                   Kind
	Timestamp              int64
	OrderID, BuyID, SellID OrderID
	Token                  int32
	Side                   Side
	Price, Quantity        int32
	LastSequence           int32
}

func (p Packet) Validate() error {
	if p.Stream <= 0 {
		return fmt.Errorf("stream must be positive")
	}
	if p.Type == Heartbeat {
		if p.Sequence != 0 || p.LastSequence < 0 {
			return fmt.Errorf("invalid heartbeat progress")
		}
		return nil
	}
	if p.Sequence <= 0 || p.Timestamp < 0 || p.Token <= 0 {
		return fmt.Errorf("invalid data sequence, timestamp or token")
	}
	switch p.Type {
	case New, Modify, Cancel:
		if _, err := IDFromFloat64(p.OrderID.Float64()); err != nil {
			return err
		}
		if p.OrderID == 0 {
			return fmt.Errorf("zero order identity")
		}
		if p.Side != Buy && p.Side != Sell {
			return fmt.Errorf("invalid order side")
		}
		if p.Price < 0 || p.Quantity < 0 || (p.Type != Cancel && (p.Price == 0 || p.Quantity == 0)) {
			return fmt.Errorf("invalid order price/quantity")
		}
	case Trade:
		for _, id := range []OrderID{p.BuyID, p.SellID} {
			if _, err := IDFromFloat64(id.Float64()); err != nil {
				return err
			}
		}
		if p.BuyID != 0 && p.BuyID == p.SellID {
			return fmt.Errorf("same buy/sell identity")
		}
		if p.Price <= 0 || p.Quantity <= 0 {
			return fmt.Errorf("invalid trade price/quantity")
		}
	default:
		return fmt.Errorf("unsupported message type %q", byte(p.Type))
	}
	return nil
}
func Encode(p Packet) ([]byte, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	size := 38
	if p.Type == Trade {
		size = 45
	}
	if p.Type == Heartbeat {
		size = 13
	}
	b := make([]byte, size)
	le := binary.LittleEndian
	le.PutUint16(b[0:2], uint16(size))
	le.PutUint16(b[2:4], uint16(p.Stream))
	le.PutUint32(b[4:8], uint32(p.Sequence))
	b[8] = byte(p.Type)
	if p.Type == Heartbeat {
		le.PutUint32(b[9:13], uint32(p.LastSequence))
		return b, nil
	}
	le.PutUint64(b[9:17], uint64(p.Timestamp))
	if p.Type == Trade {
		le.PutUint64(b[17:25], uint64(p.BuyID))
		le.PutUint64(b[25:33], uint64(p.SellID))
		le.PutUint32(b[33:37], uint32(p.Token))
		le.PutUint32(b[37:41], uint32(p.Price))
		le.PutUint32(b[41:45], uint32(p.Quantity))
	} else {
		le.PutUint64(b[17:25], uint64(p.OrderID))
		le.PutUint32(b[25:29], uint32(p.Token))
		b[29] = byte(p.Side)
		le.PutUint32(b[30:34], uint32(p.Price))
		le.PutUint32(b[34:38], uint32(p.Quantity))
	}
	return b, nil
}
func Decode(b []byte) (Packet, error) {
	var p Packet
	if len(b) < 9 {
		return p, fmt.Errorf("packet shorter than header and type")
	}
	le := binary.LittleEndian
	if int(le.Uint16(b[:2])) != len(b) {
		return p, fmt.Errorf("length field does not match datagram")
	}
	p.Stream = int16(le.Uint16(b[2:4]))
	p.Sequence = int32(le.Uint32(b[4:8]))
	p.Type = Kind(b[8])
	size := 0
	switch p.Type {
	case New, Modify, Cancel:
		size = 38
	case Trade:
		size = 45
	case Heartbeat:
		size = 13
	default:
		return Packet{}, fmt.Errorf("unsupported message type %q", b[8])
	}
	if len(b) != size {
		return Packet{}, fmt.Errorf("%c packet needs %d bytes, got %d", p.Type, size, len(b))
	}
	if p.Type == Heartbeat {
		p.LastSequence = int32(le.Uint32(b[9:13]))
	} else {
		p.Timestamp = int64(le.Uint64(b[9:17]))
		if p.Type == Trade {
			p.BuyID = OrderID(le.Uint64(b[17:25]))
			p.SellID = OrderID(le.Uint64(b[25:33]))
			p.Token = int32(le.Uint32(b[33:37]))
			p.Price = int32(le.Uint32(b[37:41]))
			p.Quantity = int32(le.Uint32(b[41:45]))
		} else {
			p.OrderID = OrderID(le.Uint64(b[17:25]))
			p.Token = int32(le.Uint32(b[25:29]))
			p.Side = Side(b[29])
			p.Price = int32(le.Uint32(b[30:34]))
			p.Quantity = int32(le.Uint32(b[34:38]))
		}
	}
	if err := p.Validate(); err != nil {
		return Packet{}, err
	}
	return p, nil
}
