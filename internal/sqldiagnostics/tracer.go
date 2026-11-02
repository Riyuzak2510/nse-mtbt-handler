// Package sqldiagnostics provides opt-in bounded SQL timing diagnostics via pgx.QueryTracer.
// It records client-observed query durations without SQL strings or parameters.
package sqldiagnostics

import (
	"context"
	"github.com/jackc/pgx/v5"
	"strings"
	"time"
)

// contextKey is a unique type for context values to avoid collisions.
type contextKey string

// tracedQuery holds SQL classification and timing context.
type tracedQuery struct {
	operation string
	startTime time.Time
}

// QueryTracer implements pgx.QueryTracer for SQL diagnostics.
// It records client-side timing from TraceQueryStart to TraceQueryEnd,
// mapping SQL queries to stable operation labels without storing SQL text or params.
type QueryTracer struct {
	recorder *Recorder
}

// NewQueryTracer creates a new QueryTracer that records to the provided Recorder.
// Pass nil recorder for no-op tracing.
func NewQueryTracer(r *Recorder) *QueryTracer {
	return &QueryTracer{recorder: r}
}

// TraceQueryStart is called at the start of query execution.
// We extract the operation label and store timing context.
func (t *QueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if t.recorder == nil {
		return ctx
	}
	// Classify operation from SQL command (don't store the SQL text itself).
	op := classifyOperation(data.SQL)
	// Store the operation label and start time in context for later retrieval in TraceQueryEnd.
	queryKey := contextKey("sqldiag.query")
	return context.WithValue(ctx, queryKey, tracedQuery{operation: op, startTime: time.Now()})
}

// TraceQueryEnd is called at the end of query execution.
// It retrieves the operation label and start time, then records the client duration.
func (t *QueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if t.recorder == nil {
		return
	}

	// Retrieve traced query from context.
	queryKey := contextKey("sqldiag.query")
	tq, ok := ctx.Value(queryKey).(tracedQuery)
	if !ok {
		// No traced query in context; skip recording.
		return
	}

	// Calculate monotonic client duration.
	duration := time.Since(tq.startTime).Nanoseconds()

	// Record the metric.
	_ = t.recorder.recordQuery(tq.operation, duration, data.Err)
}

// classifyOperation maps SQL query strings to stable operation labels.
// Returns one of: begin, commit, rollback, checkpoint_lock, checkpoint_update, order_select, order_upsert, other.
func classifyOperation(sql string) string {
	sql = strings.TrimSpace(sql)
	switch {
	case startsWithKeyword(sql, "BEGIN"):
		return "begin"
	case startsWithKeyword(sql, "COMMIT"):
		return "commit"
	case startsWithKeyword(sql, "ROLLBACK"):
		return "rollback"
	case startsWithKeyword(sql, "SELECT"):
		if contains(sql, "pg_advisory") || (contains(sql, "mtbt.feed_state") && contains(sql, "FOR UPDATE")) {
			return "checkpoint_lock"
		}
		if contains(sql, "mtbt.orders") || contains(sql, "active_orders") {
			return "order_select"
		}
	case startsWithKeyword(sql, "UPDATE"), startsWithKeyword(sql, "INSERT"), startsWithKeyword(sql, "DELETE"):
		if contains(sql, "mtbt.feed_state") {
			return "checkpoint_update"
		}
		if contains(sql, "mtbt.orders") || contains(sql, "active_orders") {
			return "order_upsert"
		}
	}
	return "other"
}

// startsWithKeyword checks if sql (case-insensitive) starts with keyword.
func startsWithKeyword(sql, keyword string) bool {
	if len(sql) < len(keyword) {
		return false
	}
	for i := 0; i < len(keyword); i++ {
		c := sql[i]
		if c >= 'a' && c <= 'z' {
			c -= 32
		}
		kc := keyword[i]
		if kc >= 'a' && kc <= 'z' {
			kc -= 32
		}
		if c != kc {
			return false
		}
	}
	// Ensure we're at a word boundary (next char is space, tab, or end).
	if len(sql) > len(keyword) {
		nc := sql[len(keyword)]
		if nc != ' ' && nc != '\t' && nc != '\n' && nc != '(' {
			return false
		}
	}
	return true
}

// contains checks if sql contains substring (case-insensitive).
func contains(sql, substr string) bool {
	for i := 0; i <= len(sql)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			c := sql[i+j]
			if c >= 'a' && c <= 'z' {
				c -= 32
			}
			sc := substr[j]
			if sc >= 'a' && sc <= 'z' {
				sc -= 32
			}
			if c != sc {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// Verify QueryTracer implements pgx.QueryTracer interface.
var _ pgx.QueryTracer = (*QueryTracer)(nil)
