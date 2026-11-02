package outcomes

import (
	"testing"
	"time"

	"nse-mtbt-handler/internal/feed"
)

func TestShouldApply_FirstEvent(t *testing.T) {
	order := OrderSnapshot{
		OrderIDHex:   "000000000000f03f",
		Token:        1,
		Side:         "B",
		PricePaise:   10000,
		Status:       "OPEN",
		AcceptedQty:  "100",
		FilledQty:    "0",
		CancelledQty: "0",
		RemainingQty: 100,
	}

	e, _ := New(1, 100, 1, feed.New, "NEW", order, time.Now())

	// No prior version (0)
	if !ShouldApply(0, e) {
		t.Fatalf("ShouldApply should return true for first event")
	}
}

func TestShouldApply_NewerVersion(t *testing.T) {
	order := OrderSnapshot{
		OrderIDHex:   "000000000000f03f",
		Token:        1,
		Side:         "B",
		PricePaise:   10000,
		Status:       "OPEN",
		AcceptedQty:  "100",
		FilledQty:    "0",
		CancelledQty: "0",
		RemainingQty: 100,
	}

	e, _ := New(1, 200, 1, feed.New, "NEW", order, time.Now())

	// Current version is 100, event is 200
	if !ShouldApply(100, e) {
		t.Fatalf("ShouldApply should return true for newer version")
	}
}

func TestShouldApply_SameVersion(t *testing.T) {
	order := OrderSnapshot{
		OrderIDHex:   "000000000000f03f",
		Token:        1,
		Side:         "B",
		PricePaise:   10000,
		Status:       "OPEN",
		AcceptedQty:  "100",
		FilledQty:    "0",
		CancelledQty: "0",
		RemainingQty: 100,
	}

	e, _ := New(1, 100, 1, feed.New, "NEW", order, time.Now())

	// Current version is 100, event is 100
	if ShouldApply(100, e) {
		t.Fatalf("ShouldApply should return false for same version")
	}
}

func TestShouldApply_OlderVersion(t *testing.T) {
	order := OrderSnapshot{
		OrderIDHex:   "000000000000f03f",
		Token:        1,
		Side:         "B",
		PricePaise:   10000,
		Status:       "OPEN",
		AcceptedQty:  "100",
		FilledQty:    "0",
		CancelledQty: "0",
		RemainingQty: 100,
	}

	e, _ := New(1, 50, 1, feed.New, "NEW", order, time.Now())

	// Current version is 100, event is 50
	if ShouldApply(100, e) {
		t.Fatalf("ShouldApply should return false for older version")
	}
}

func TestShouldApply_Deduplication(t *testing.T) {
	order := OrderSnapshot{
		OrderIDHex:   "000000000000f03f",
		Token:        1,
		Side:         "B",
		PricePaise:   10000,
		Status:       "PARTIALLY_FILLED",
		AcceptedQty:  "100",
		FilledQty:    "50",
		CancelledQty: "0",
		RemainingQty: 50,
	}

	e, _ := New(1, 1000, 1, feed.Modify, "MATCH", order, time.Now())

	// Simulate receiving the same event twice
	currentVersion := int32(0)

	if !ShouldApply(currentVersion, e) {
		t.Fatalf("ShouldApply should return true on first receipt")
	}

	// After applying, update currentVersion
	currentVersion = e.OrderVersion

	if ShouldApply(currentVersion, e) {
		t.Fatalf("ShouldApply should return false on second receipt of same version")
	}
}

func TestShouldApply_ManyVersions(t *testing.T) {
	order := OrderSnapshot{
		OrderIDHex:   "000000000000f03f",
		Token:        1,
		Side:         "B",
		PricePaise:   10000,
		Status:       "OPEN",
		AcceptedQty:  "100",
		FilledQty:    "0",
		CancelledQty: "0",
		RemainingQty: 100,
	}

	// Simulate a sequence of events
	currentVersion := int32(0)
	sequences := []int32{10, 20, 30, 25, 40}

	for i, seq := range sequences {
		e, _ := New(1, seq, 1, feed.New, "NEW", order, time.Now())
		shouldApply := ShouldApply(currentVersion, e)

		// Events 10, 20, 30, 40 should apply (increasing)
		// Event 25 should not apply (less than current 30)
		expectApply := (i < 2) || (i == 2) || (i == 4) // indices 0, 1, 2, 4
		if i == 3 {                                    // index 3 is the 25 after 30
			expectApply = false
		}

		if shouldApply != expectApply {
			t.Errorf("Sequence %d: ShouldApply=%v, want %v", seq, shouldApply, expectApply)
		}

		if shouldApply {
			currentVersion = e.OrderVersion
		}
	}
}
