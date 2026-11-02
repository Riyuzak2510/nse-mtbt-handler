package outcomes

// ShouldApply determines whether an outcome event should be applied given the current order version.
// It implements version-aware deduplication for subscribers.
//
// currentVersion: the last source_sequence applied for this (stream_id, order_id) pair in durable state
// event: the incoming outcome event
//
// Returns true if the event's version is strictly greater than current, false otherwise.
// Subscribers must atomically persist both the new state and the updated version in their
// local durable transaction, ensuring no version regresses and no state change is lost.
//
// Responsibility of the caller:
// - Maintain persistent currentVersion for each (stream_id, order_id)
// - Apply the event state change and update currentVersion atomically
// - Query events by (stream_id, order_id) and apply in increasing source_sequence order
// - Do not depend solely on broker deduplication; apply your own version checks
// - Handle repeated/stale events gracefully (no side effects if version does not advance)
//
// Example durable consumer pattern:
//
//	durable state: lastAppliedVersion[stream][order_id] = source_sequence
//	on event receipt:
//	  begin durable transaction
//	    lock/read current version for (event.StreamID, orderID)
//	    if ShouldApply(currentVersion, event) {
//	      apply event.Order state to local state
//	      persist event.OrderVersion alongside that state
//	    }
//	  commit successfully (otherwise retry without acknowledging)
//	  acknowledge event to broker
//
// The version check must be inside the same serialized transaction as the write,
// not performed against an unlocked cached value before beginning it.
func ShouldApply(currentVersion int32, event Event) bool {
	return event.OrderVersion > currentVersion
}
