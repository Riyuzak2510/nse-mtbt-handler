package broker

import (
	"fmt"
	"os"
	"strconv"
)

const DefaultMaxBytes int64 = 64 << 20

// StreamMaxBytes is an explicit local capacity setting, not native identity.
// Every owner of a stream must agree; existing incompatible state is refused.
func StreamMaxBytes() (int64, error) {
	v := os.Getenv("MTBT_STREAM_MAX_BYTES")
	if v == "" {
		return DefaultMaxBytes, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < DefaultMaxBytes || n > 1<<40 {
		return 0, fmt.Errorf("MTBT_STREAM_MAX_BYTES must be integer bytes between64MiB and1TiB")
	}
	return n, nil
}
