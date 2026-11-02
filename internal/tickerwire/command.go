// Package tickerwire pins the instrument-addressed, digest-chained command protocol.
package tickerwire

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"strconv"
	"time"
)

const (
	CommandVersion  = 1
	MaxRawBytes     = 4096
	MaxCommandBytes = 16384
)

type Command struct {
	Version        int       `json:"version"`
	StreamID       int16     `json:"stream_id"`
	Token          int32     `json:"token"`
	Sequence       uint64    `json:"sequence"`
	SourceBroker   uint64    `json:"source_broker"`
	Received       time.Time `json:"received"`
	Raw            []byte    `json:"raw"`
	PreviousDigest string    `json:"previous_digest"`
	Digest         string    `json:"digest"`
}

func New(m runconfig.Manifest, token int32, seq, broker uint64, received time.Time, raw []byte, previous string) (Command, error) {
	c := Command{CommandVersion, m.StreamID, token, seq, broker, received.UTC(), append([]byte(nil), raw...), previous, ""}
	var e error
	c.Digest, e = digest(m, c)
	if e != nil {
		return Command{}, e
	}
	e = Validate(m, c)
	return c, e
}
func shape(c Command) error {
	if c.Version != 1 || c.StreamID <= 0 || c.Token <= 0 || c.Sequence == 0 || c.Sequence > math.MaxInt64 || c.SourceBroker == 0 || c.SourceBroker > math.MaxInt64 {
		return fmt.Errorf("invalid routed identity")
	}
	if len(c.Raw) == 0 || len(c.Raw) > MaxRawBytes || c.Received.IsZero() || c.Received.After(time.Now().Add(time.Minute)) {
		return fmt.Errorf("invalid routed packet/timestamp")
	}
	_, offset := c.Received.Zone()
	if offset != 0 {
		return fmt.Errorf("timestamp must be UTC")
	}
	if c.Sequence == 1 {
		if c.PreviousDigest != "" {
			return fmt.Errorf("first command has predecessor")
		}
	} else if !hexDigest(c.PreviousDigest) {
		return fmt.Errorf("invalid predecessor digest")
	}
	if !hexDigest(c.Digest) {
		return fmt.Errorf("invalid command digest")
	}
	return nil
}
func hexDigest(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func digest(m runconfig.Manifest, c Command) (string, error) {
	h, e := m.Digest()
	if e != nil {
		return "", e
	}
	c.Digest = ""
	b, e := json.Marshal(struct {
		Manifest string  `json:"manifest"`
		Command  Command `json:"command"`
	}{h, c})
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
func Validate(m runconfig.Manifest, c Command) error {
	if e := m.Validate(); e != nil {
		return e
	}
	if e := shape(c); e != nil {
		return e
	}
	if c.StreamID != m.StreamID || !m.TokenAllowed(c.Token) {
		return fmt.Errorf("routed instrument outside manifest")
	}
	p, e := feed.Decode(c.Raw)
	if e != nil {
		return e
	}
	if p.Stream != c.StreamID || (p.Type != feed.Heartbeat && p.Token != c.Token) {
		return fmt.Errorf("native/routed identity differs")
	}
	h, e := digest(m, c)
	if e != nil {
		return e
	}
	if h != c.Digest {
		return fmt.Errorf("routed payload digest mismatch")
	}
	return nil
}
func Encode(c Command) ([]byte, error) {
	if e := shape(c); e != nil {
		return nil, e
	}
	b, e := json.Marshal(c)
	if len(b) > MaxCommandBytes {
		return nil, fmt.Errorf("routed envelope too large")
	}
	return b, e
}
func Decode(b []byte) (Command, error) {
	var c Command
	if len(b) == 0 || len(b) > MaxCommandBytes {
		return c, fmt.Errorf("invalid routed envelope size")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if e := d.Decode(&c); e != nil {
		return c, e
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return c, fmt.Errorf("trailing routed JSON")
	}
	return c, shape(c)
}
func ID(c Command) string {
	return fmt.Sprintf("mtbt-match-v1:%d:%d:%d", c.StreamID, c.Token, c.Sequence)
}
func Subject(stream int16, token int32) string {
	return "mtbtmatch." + strconv.Itoa(int(stream)) + "." + strconv.Itoa(int(token))
}
func StreamName(stream int16) string { return "MATCH_" + strconv.Itoa(int(stream)) }
