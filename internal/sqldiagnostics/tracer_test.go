package sqldiagnostics

import (
	"testing"
)

// TestClassifyOperation verifies operation classification logic.
func TestClassifyOperation(t *testing.T) {
	tests := []struct {
		sql    string
		expect string
	}{
		// Transaction control
		{"BEGIN", "begin"},
		{"begin", "begin"},
		{"BEGIN TRANSACTION", "begin"},
		{"COMMIT", "commit"},
		{"commit", "commit"},
		{"ROLLBACK", "rollback"},
		{"rollback", "rollback"},

		// Checkpoint operations
		{"SELECT pg_advisory_xact_lock(19462471)", "checkpoint_lock"},
		{"select pg_advisory_lock(123)", "checkpoint_lock"},
		{"UPDATE mtbt.feed_state SET status='VALID'", "checkpoint_update"},
		{"update mtbt.feed_state SET source_sequence=$2", "checkpoint_update"},

		// Order operations
		{"SELECT token,side,price,quantity FROM mtbt.active_orders WHERE stream_id=$1", "order_select"},
		{"select * from mtbt.active_orders", "order_select"},
		{"INSERT INTO mtbt.active_orders(...) VALUES(...) ON CONFLICT(...) DO UPDATE SET", "order_upsert"},
		{"insert into mtbt.active_orders", "order_upsert"},
		{"DELETE FROM mtbt.active_orders WHERE stream_id=$1", "order_upsert"},
		{"delete from mtbt.active_orders", "order_upsert"},

		{"SELECT source_sequence FROM mtbt.feed_state WHERE stream_id=$1 FOR UPDATE", "checkpoint_lock"},
		{"SELECT * FROM mtbt.orders WHERE order_id=$1 FOR UPDATE", "order_select"},
		{"INSERT INTO mtbt.orders VALUES($1) ON CONFLICT DO UPDATE SET", "order_upsert"},
		{"UPDATE mtbt.orders SET filled_quantity=$1", "order_upsert"},
		{"  BEGIN", "begin"},
		// Other queries
		{"SELECT count(*) FROM users", "other"},
		{"UPDATE other_table SET x=1", "other"},
		{"", "other"},
		{"INSERT INTO other_table VALUES (1)", "other"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got := classifyOperation(tt.sql)
			if got != tt.expect {
				t.Errorf("classifyOperation(%q) = %q, want %q", tt.sql, got, tt.expect)
			}
		})
	}
}

// TestStartsWithKeyword verifies keyword matching.
func TestStartsWithKeyword(t *testing.T) {
	tests := []struct {
		sql     string
		keyword string
		expect  bool
	}{
		{"BEGIN", "BEGIN", true},
		{"begin", "BEGIN", true},
		{"BEGIN TRANSACTION", "BEGIN", true},
		{"BEGINNING", "BEGIN", false}, // Not a word boundary
		{"BEGIN(", "BEGIN", true},     // Parenthesis is a boundary
		{"COMMIT", "BEGIN", false},
		{"", "BEGIN", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql+"_"+tt.keyword, func(t *testing.T) {
			got := startsWithKeyword(tt.sql, tt.keyword)
			if got != tt.expect {
				t.Errorf("startsWithKeyword(%q, %q) = %v, want %v", tt.sql, tt.keyword, got, tt.expect)
			}
		})
	}
}

// TestContains verifies substring matching (case-insensitive).
func TestContains(t *testing.T) {
	tests := []struct {
		sql    string
		substr string
		expect bool
	}{
		{"SELECT * FROM mtbt.active_orders", "active_orders", true},
		{"SELECT * FROM MTBT.ACTIVE_ORDERS", "active_orders", true},
		{"SELECT * FROM feed_state", "active_orders", false},
		{"", "active_orders", false},
		{"pg_advisory_lock(123)", "pg_advisory", true},
		{"PG_ADVISORY_XACT_LOCK(456)", "pg_advisory", true},
	}

	for _, tt := range tests {
		t.Run(tt.sql+"_"+tt.substr, func(t *testing.T) {
			got := contains(tt.sql, tt.substr)
			if got != tt.expect {
				t.Errorf("contains(%q, %q) = %v, want %v", tt.sql, tt.substr, got, tt.expect)
			}
		})
	}
}
