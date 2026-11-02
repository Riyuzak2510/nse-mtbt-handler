package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"nse-mtbt-handler/internal/feed"
)

// TestGenerateExpectedDeterministicFrame tests deterministic order generation.
func TestGenerateExpectedDeterministicFrame(t *testing.T) {
	tests := []struct {
		name      string
		total     int32
		token     int32
		wantCount int
		validate  func(orders []expectedOrder) error
	}{
		{
			name:      "simple triplet",
			total:     3,
			token:     1,
			wantCount: 3,
			validate: func(orders []expectedOrder) error {
				// Order 1: buy100 filled
				if orders[0].Side != "B" || orders[0].FilledQuantity != 100 || orders[0].Status != "FILLED" {
					return errorf("order 1: expected B/100/FILLED, got %s/%d/%s",
						orders[0].Side, orders[0].FilledQuantity, orders[0].Status)
				}
				// Order 2: sell30 filled
				if orders[1].Side != "S" || orders[1].FilledQuantity != 30 || orders[1].Status != "FILLED" {
					return errorf("order 2: expected S/30/FILLED, got %s/%d/%s",
						orders[1].Side, orders[1].FilledQuantity, orders[1].Status)
				}
				// Order 3: sell70 filled
				if orders[2].Side != "S" || orders[2].FilledQuantity != 70 || orders[2].Status != "FILLED" {
					return errorf("order 3: expected S/70/FILLED, got %s/%d/%s",
						orders[2].Side, orders[2].FilledQuantity, orders[2].Status)
				}
				return nil
			},
		},
		{
			name:      "full cycles with trailing buy",
			total:     4,
			token:     1,
			wantCount: 4,
			validate: func(orders []expectedOrder) error {
				// Triplet cycle filled
				for i := 0; i < 3; i++ {
					if orders[i].Status != "FILLED" {
						return errorf("order %d in cycle: expected FILLED, got %s", i+1, orders[i].Status)
					}
				}
				// Order 4: buy100 open (trailing after full triplet)
				if orders[3].Side != "B" || orders[3].FilledQuantity != 0 || orders[3].Status != "OPEN" {
					return errorf("order 4 (trailing): expected B/0/OPEN, got %s/%d/%s",
						orders[3].Side, orders[3].FilledQuantity, orders[3].Status)
				}
				return nil
			},
		},
		{
			name:      "full cycles with trailing pair",
			total:     5,
			token:     1,
			wantCount: 5,
			validate: func(orders []expectedOrder) error {
				// Triplet cycle filled
				for i := 0; i < 3; i++ {
					if orders[i].Status != "FILLED" {
						return errorf("order %d in cycle: expected FILLED, got %s", i+1, orders[i].Status)
					}
				}
				// Order 4: buy100 partial (filled by sell30)
				if orders[3].Side != "B" || orders[3].FilledQuantity != 30 || orders[3].Status != "PARTIALLY_FILLED" {
					return errorf("order 4 (partial buy): expected B/30/PARTIAL, got %s/%d/%s",
						orders[3].Side, orders[3].FilledQuantity, orders[3].Status)
				}
				// Order 5: sell30 filled
				if orders[4].Side != "S" || orders[4].FilledQuantity != 30 || orders[4].Status != "FILLED" {
					return errorf("order 5 (sell): expected S/30/FILLED, got %s/%d/%s",
						orders[4].Side, orders[4].FilledQuantity, orders[4].Status)
				}
				return nil
			},
		},
		{
			name:      "full cycles only 9999",
			total:     9999,
			token:     42,
			wantCount: 9999,
			validate: func(orders []expectedOrder) error {
				// 9999 = 3333 full triplets (no trailing)
				// Each triplet: buy100, sell30, sell70 - fully conserved
				bought := int64(0)
				sold := int64(0)
				for _, o := range orders {
					if o.Side == "B" {
						bought += int64(o.AcceptedQuantity)
					} else {
						sold += int64(o.AcceptedQuantity)
					}
				}
				if bought != sold {
					return errorf("quantity conservation: bought=%d, sold=%d", bought, sold)
				}
				return nil
			},
		},
		{
			name:      "unique native IDs",
			total:     100,
			token:     1,
			wantCount: 100,
			validate: func(orders []expectedOrder) error {
				seen := make(map[string]bool)
				for i, o := range orders {
					if seen[o.ID] {
						return errorf("order %d: duplicate ID %s", i+1, o.ID)
					}
					seen[o.ID] = true
					// Validate ID encoding: should be native seq float in little-endian
					if o.ID == "" {
						return errorf("order %d: empty ID", i+1)
					}
				}
				if len(seen) != len(orders) {
					return errorf("ID uniqueness: %d unique out of %d", len(seen), len(orders))
				}
				return nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orders := generateExpected(tt.total, tt.token)
			if len(orders) != tt.wantCount {
				t.Fatalf("count: got %d, want %d", len(orders), tt.wantCount)
			}
			if err := tt.validate(orders); err != nil {
				t.Errorf("%v", err)
			}
		})
	}
}

// TestExpectedOracleQuantityConservation validates quantity conservation across state transitions.
func TestExpectedOracleQuantityConservation(t *testing.T) {
	tests := []int32{1, 3, 4, 5, 100, 999, 1000, 10000}
	for _, total := range tests {
		t.Run(string(rune(total)), func(t *testing.T) {
			orders := generateExpected(total, 1)
			for i, o := range orders {
				// Conservation: accepted_quantity = filled + remaining + cancelled
				computed := o.FilledQuantity + o.RemainingQuantity + o.CancelledQuantity
				if o.AcceptedQuantity != computed {
					t.Errorf("order %d: accepted=%d, but filled=%d + remaining=%d + cancelled=%d = %d",
						i+1, o.AcceptedQuantity, o.FilledQuantity, o.RemainingQuantity,
						o.CancelledQuantity, computed)
				}
			}
		})
	}
}

// TestExpectedOracleBounds tests sequence bounds validation.
func TestExpectedOracleBounds(t *testing.T) {
	tests := []struct {
		name    string
		total   int32
		wantErr bool
	}{
		{"total 0", 0, false}, // 0 yields empty, no error
		{"total 1", 1, false},
		{"total 100000", 100000, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orders := generateExpected(tt.total, 1)
			if len(orders) != int(tt.total) {
				t.Errorf("count: got %d, want %d", len(orders), tt.total)
			}
		})
	}
}

// TestExpectedOracleTrailingStates validates trailing order logic for partial fills.
func TestExpectedOracleTrailingStates(t *testing.T) {
	tests := []struct {
		name          string
		total         int32
		checkOrder    int // 1-indexed
		wantStatus    string
		wantFilled    int32
		wantRemaining int32
	}{
		// total % 3 == 1: trailing buy is open
		{"total 1 buy open", 1, 1, "OPEN", 0, 100},
		{"total 4 buy open", 4, 4, "OPEN", 0, 100},
		{"total 7 buy open", 7, 7, "OPEN", 0, 100},

		// total % 3 == 2: buy partial, sell filled
		{"total 2 buy partial", 2, 1, "PARTIALLY_FILLED", 30, 70},
		{"total 2 sell filled", 2, 2, "FILLED", 30, 0},
		{"total 5 buy partial", 5, 4, "PARTIALLY_FILLED", 30, 70},
		{"total 5 sell filled", 5, 5, "FILLED", 30, 0},

		// total % 3 == 0: all filled (no trailing)
		{"total 3 all filled", 3, 3, "FILLED", 70, 0},
		{"total 6 cycle end", 6, 6, "FILLED", 70, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orders := generateExpected(tt.total, 1)
			if len(orders) < tt.checkOrder {
				t.Fatalf("total=%d but checking order %d", tt.total, tt.checkOrder)
			}
			o := orders[tt.checkOrder-1]
			if o.Status != tt.wantStatus {
				t.Errorf("status: got %s, want %s", o.Status, tt.wantStatus)
			}
			if o.FilledQuantity != tt.wantFilled {
				t.Errorf("filled: got %d, want %d", o.FilledQuantity, tt.wantFilled)
			}
			if o.RemainingQuantity != tt.wantRemaining {
				t.Errorf("remaining: got %d, want %d", o.RemainingQuantity, tt.wantRemaining)
			}
		})
	}
}

// TestExpectedOracleIndependentSequence validates that each order uses its own sequence.
func TestExpectedOracleIndependentSequence(t *testing.T) {
	orders := generateExpected(100, 1)
	for i, o := range orders {
		// Each order's ID should encode sequence i+1 as a native float
		nativeSeq := float64(i + 1)
		expectedID, _ := feed.IDFromFloat64(nativeSeq)
		idBytes := expectedID.Bytes()
		expectedHex := fmt.Sprintf("%02x", idBytes)
		if o.ID != expectedHex {
			t.Errorf("order %d: ID mismatch, got %s, expected %s", i+1, o.ID, expectedHex)
		}
	}
}

// TestGenerateExpectedJSON validates JSON marshaling.
func TestGenerateExpectedJSON(t *testing.T) {
	orders := generateExpected(10, 1)
	data, err := json.Marshal(orders)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var unmarshaled []expectedOrder
	if err := json.Unmarshal(data, &unmarshaled); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(unmarshaled) != len(orders) {
		t.Errorf("count: got %d, want %d", len(unmarshaled), len(orders))
	}
	for i, o := range orders {
		if unmarshaled[i] != o {
			t.Errorf("order %d mismatch", i+1)
		}
	}
}

// TestWriteJSONExclusive tests that writeJSON refuses to overwrite.
func TestWriteJSONExclusive(t *testing.T) {
	tmpdir := t.TempDir()
	path := filepath.Join(tmpdir, "test.json")

	// First write should succeed
	orders := generateExpected(10, 1)
	if err := writeJSON(path, orders); err != nil {
		t.Fatalf("first write: %v", err)
	}

	// Second write should fail (exclusive)
	if err := writeJSON(path, orders); err == nil {
		t.Fatal("second write should fail with exclusive flag")
	}
}

// TestGenerateExpectedBoundaryValues tests edge cases around 100k limit.
func TestGenerateExpectedBoundaryValues(t *testing.T) {
	tests := []int32{1, 3, 99999, 100000}
	for _, total := range tests {
		t.Run(string(rune(total)), func(t *testing.T) {
			orders := generateExpected(total, 1)
			if int32(len(orders)) != total {
				t.Errorf("count: got %d, want %d", len(orders), total)
			}
			// Check all are valid
			for i, o := range orders {
				if o.ID == "" {
					t.Errorf("order %d: empty ID", i+1)
				}
				if o.TokenID != 1 {
					t.Errorf("order %d: token mismatch", i+1)
				}
				if o.Price != 10000 {
					t.Errorf("order %d: price mismatch", i+1)
				}
				if o.Side != "B" && o.Side != "S" {
					t.Errorf("order %d: invalid side %s", i+1, o.Side)
				}
			}
		})
	}
}

// TestExpectedOracleSidePattern validates the triplet cycling pattern.
func TestExpectedOracleSidePattern(t *testing.T) {
	orders := generateExpected(9, 1)
	// Pattern should repeat: B, S, S, B, S, S, B, S, S
	wantSide := []string{"B", "S", "S", "B", "S", "S", "B", "S", "S"}
	for i, want := range wantSide {
		if orders[i].Side != want {
			t.Errorf("order %d: side mismatch, got %s, want %s", i+1, orders[i].Side, want)
		}
	}
}

// TestExpectedOracleQuantityPattern validates the quantity pattern per triplet.
func TestExpectedOracleQuantityPattern(t *testing.T) {
	orders := generateExpected(9, 1)
	// Pattern per triplet: 100, 30, 70, 100, 30, 70, 100, 30, 70
	wantQty := []int32{100, 30, 70, 100, 30, 70, 100, 30, 70}
	for i, want := range wantQty {
		if orders[i].AcceptedQuantity != want {
			t.Errorf("order %d: qty mismatch, got %d, want %d", i+1, orders[i].AcceptedQuantity, want)
		}
	}
}

// Helpers

func errorf(format string, args ...interface{}) error {
	return &testError{msg: fmt.Sprintf(format, args...)}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

// TestNativeIDEncoding validates that sequence numbers encode correctly as native floats.
func TestNativeIDEncoding(t *testing.T) {
	tests := []int32{1, 2, 3, 10, 100, 1000, 10000, 100000}
	for _, seq := range tests {
		t.Run(string(rune(seq)), func(t *testing.T) {
			nativeID := float64(seq)
			orderID, err := feed.IDFromFloat64(nativeID)
			if err != nil {
				t.Fatalf("IDFromFloat64(%v): %v", nativeID, err)
			}
			// Round-trip validation
			roundTrip := orderID.Float64()
			if roundTrip != nativeID {
				t.Errorf("round-trip: %v -> %v -> %v", nativeID, orderID, roundTrip)
			}
		})
	}
}

// TestExpectedOracleFileWrite validates file I/O and directory creation.
func TestExpectedOracleFileWrite(t *testing.T) {
	tmpdir := t.TempDir()

	orders := generateExpected(50, 7)
	outpath := filepath.Join(tmpdir, "expected.json")

	if err := writeJSON(outpath, orders); err != nil {
		t.Fatalf("writeJSON: %v", err)
	}

	// Validate file exists and can be read
	data, err := os.ReadFile(outpath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	var readOrders []expectedOrder
	if err := json.Unmarshal(data, &readOrders); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if len(readOrders) != len(orders) {
		t.Errorf("count: got %d, want %d", len(readOrders), len(orders))
	}
}

// TestExpectedOracleDeterminism validates that same inputs yield same outputs.
func TestExpectedOracleDeterminism(t *testing.T) {
	orders1 := generateExpected(100, 42)
	orders2 := generateExpected(100, 42)

	for i := range orders1 {
		if orders1[i] != orders2[i] {
			t.Errorf("determinism failed at order %d", i+1)
		}
	}
}

// TestGenerateExpectedEmptyTotal validates edge case of total 0.
func TestGenerateExpectedEmptyTotal(t *testing.T) {
	orders := generateExpected(0, 1)
	if len(orders) != 0 {
		t.Errorf("empty total should yield empty list, got %d", len(orders))
	}
}
