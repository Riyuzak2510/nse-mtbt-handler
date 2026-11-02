package runconfig

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Instrument represents a tradable security.
type Instrument struct {
	Token     int32  `json:"token"`
	Symbol    string `json:"symbol"`
	TickSize  int32  `json:"tick_size"`
	BasePrice int32  `json:"base_price"`
}

// Manifest configures a source stream. Stream identity comes from the native stream ID;
// a new sequence-one source requires an explicitly clean database/broker environment.
type Manifest struct {
	Version     int          `json:"version"`
	Profile     string       `json:"profile"`
	StreamID    int16        `json:"stream_id"`
	Seed        int64        `json:"seed"`
	Baseline    string       `json:"baseline"`
	CreatedAt   time.Time    `json:"created_at"`
	Instruments []Instrument `json:"instruments"`
}

// Validate checks the manifest for correctness.
func (m *Manifest) Validate() error {
	if m.Version != 2 {
		return fmt.Errorf("unsupported version: %d, expected stream-scoped manifest version 2", m.Version)
	}

	if m.Profile != "cm-mtbt-le64-v1" {
		return fmt.Errorf("unsupported profile: %s, expected cm-mtbt-le64-v1", m.Profile)
	}

	if m.StreamID <= 0 {
		return fmt.Errorf("stream_id must be positive, got %d", m.StreamID)
	}

	if m.Baseline != "empty" {
		return fmt.Errorf("baseline must be empty, got: %s", m.Baseline)
	}

	if m.CreatedAt.IsZero() {
		return fmt.Errorf("created_at is required")
	}
	if len(m.Instruments) == 0 {
		return fmt.Errorf("instruments list must not be empty")
	}

	// Check for reasonable instrument count (e.g., < 10000)
	if len(m.Instruments) > 10000 {
		return fmt.Errorf("instrument count %d exceeds reasonable limit", len(m.Instruments))
	}

	// Verify uniqueness and validity of tokens and symbols
	tokenSeen := make(map[int32]bool)
	symbolSeen := make(map[string]bool)

	for i, inst := range m.Instruments {
		if inst.Token <= 0 {
			return fmt.Errorf("token must be positive at index %d", i)
		}
		if tokenSeen[inst.Token] {
			return fmt.Errorf("duplicate token %d at index %d", inst.Token, i)
		}
		tokenSeen[inst.Token] = true

		if symbolSeen[inst.Symbol] {
			return fmt.Errorf("duplicate symbol %q at index %d", inst.Symbol, i)
		}
		if inst.Symbol == "" {
			return fmt.Errorf("symbol must not be empty at index %d", i)
		}
		symbolSeen[inst.Symbol] = true

		// Verify positive base price aligned to tick size
		if inst.BasePrice <= 0 {
			return fmt.Errorf("base_price must be positive at index %d, got %d", i, inst.BasePrice)
		}
		if inst.TickSize <= 0 {
			return fmt.Errorf("tick_size must be positive at index %d, got %d", i, inst.TickSize)
		}
		if inst.BasePrice%inst.TickSize != 0 {
			return fmt.Errorf("base_price %d not aligned to tick_size %d at index %d",
				inst.BasePrice, inst.TickSize, i)
		}
	}

	return nil
}

// Digest returns the canonical SHA256 hex digest of the manifest.
func (m *Manifest) Digest() (string, error) {
	// Marshal to JSON with canonical formatting for deterministic digest
	canonical := *m
	// Creation time is metadata, not an implicit replacement for removed run identity.
	canonical.CreatedAt = time.Time{}
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}

	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

// TokenAllowed returns true if the given token is valid for this manifest.
func (m *Manifest) TokenAllowed(token int32) bool {
	for _, inst := range m.Instruments {
		if inst.Token == token {
			return true
		}
	}
	return false
}
