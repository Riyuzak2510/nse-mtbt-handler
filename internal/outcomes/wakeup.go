package outcomes

import "fmt"

// WakeChannel carries disposable commit hints, never business events. Native
// stream IDs are validated by callers. The generated identifier is not user SQL.
func WakeChannel(stream int16) string {
	return fmt.Sprintf("mtbt_outcomes_ready_%d", stream)
}
