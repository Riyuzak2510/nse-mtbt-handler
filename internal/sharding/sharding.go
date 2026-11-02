// Package sharding defines the pinned local parallel-feed routing contract.
// It does not change native packet bytes or implement order matching.
package sharding

import (
	"fmt"
	"time"
)

const Version = "token-mod-v1"
const MaxShards = 16

func ValidateCount(count int) error {
	if count < 1 || count > MaxShards {
		return fmt.Errorf("shard count must be 1..%d", MaxShards)
	}
	return nil
}
func ForToken(token int32, count int) (int, error) {
	if err := ValidateCount(count); err != nil {
		return 0, err
	}
	if token <= 0 {
		return 0, fmt.Errorf("nonpositive instrument token")
	}
	return int(token % int32(count)), nil
}

// Event is created by the ordered dispatcher, durably before broker publication.
// Sequence is contiguous per shard; SourceBroker is provenance from the raw stream.
type Event struct {
	Shard        int
	Sequence     uint64
	SourceBroker uint64
	Raw          []byte
	Received     time.Time
}
