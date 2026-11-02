package matching

import (
	"crypto/sha256"
	"fmt"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"sync"
	"time"
)

// Controller serializes mutation and snapshot reads; it never persists or publishes executions.
// ReplayThrough is a startup lower bound, not a watermark for unseen later broker traffic.
type Controller struct {
	mu                            sync.RWMutex
	engine                        *Engine
	manifest                      runconfig.Manifest
	ticks                         map[int32]int32
	replayThrough, brokerSequence uint64
	sourceSequence                int32
	brokerHash, sourceHash        [32]byte
	lastReceived                  time.Time
	invalid                       string
	connected                     bool
	duplicates                    uint64
	peakActive                    int
	recent                        [100]Execution
	recentCount, recentNext       int
}

// Execution is a bounded diagnostic record, not a durable trade or external settlement event.
type Execution struct {
	Fill
	SourceSequence int32  `json:"source_sequence"`
	BrokerSequence uint64 `json:"broker_sequence"`
}
type Outcome struct {
	Result
	Sequence       int32  `json:"source_sequence"`
	BrokerSequence uint64 `json:"broker_sequence"`
	Duplicate      bool   `json:"duplicate"`
}
type Snapshot struct {
	StreamID            int16       `json:"stream_id"`
	Policy              string      `json:"policy"`
	Synthetic           bool        `json:"synthetic"`
	Ephemeral           bool        `json:"ephemeral"`
	Status              string      `json:"status"`
	Reason              string      `json:"reason,omitempty"`
	SourceSequence      int32       `json:"source_sequence"`
	BrokerSequence      uint64      `json:"broker_sequence"`
	ReplayThrough       uint64      `json:"startup_replay_through"`
	ActiveOrders        int         `json:"active_orders"`
	PeakActiveOrders    int         `json:"peak_active_orders"`
	DuplicateDeliveries uint64      `json:"duplicate_deliveries"`
	LastReceived        *time.Time  `json:"last_received,omitempty"`
	Stats               Stats       `json:"stats"`
	RecentExecutions    []Execution `json:"recent_shadow_executions"`
	Depth
}

func NewController(m runconfig.Manifest, maxActive int, replayThrough uint64) (*Controller, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	e, err := New(maxActive)
	if err != nil {
		return nil, err
	}
	ticks := make(map[int32]int32, len(m.Instruments))
	for _, i := range m.Instruments {
		ticks[i.Token] = i.TickSize
	}
	return &Controller{engine: e, manifest: m, ticks: ticks, replayThrough: replayThrough, connected: true}, nil
}
func (c *Controller) Invalidate(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.invalid == "" {
		c.invalid = reason
	}
}
func (c *Controller) SetConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = connected
}

// Process enforces consecutive broker positions from one and source positions from one.
// Only the most recent identical broker/source duplicate is accepted; historical out-of-order
// deliveries fail closed rather than retaining an unbounded identity/receipt archive.
func (c *Controller) Process(raw []byte, position uint64, received time.Time) (Outcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	reject := func(reason string) (Outcome, error) {
		c.invalid = reason
		return Outcome{}, fmt.Errorf("shadow invalid: %s", reason)
	}
	if c.invalid != "" {
		return Outcome{}, fmt.Errorf("shadow invalid: %s", c.invalid)
	}
	if position == 0 {
		return reject("zero broker position")
	}
	if received.IsZero() || received.After(time.Now().Add(time.Minute)) {
		return reject("invalid receiver timestamp")
	}
	hash := sha256.Sum256(raw)
	if position == c.brokerSequence && position > 0 {
		if hash != c.brokerHash {
			return reject("conflicting broker duplicate")
		}
		c.duplicates++
		return Outcome{Sequence: c.sourceSequence, BrokerSequence: position, Duplicate: true}, nil
	}
	if position != c.brokerSequence+1 {
		return reject(fmt.Sprintf("broker gap/reset: expected%d got%d", c.brokerSequence+1, position))
	}
	p, err := feed.Decode(raw)
	if err != nil {
		return reject(err.Error())
	}
	if p.Stream != c.manifest.StreamID {
		return reject("stream mismatch")
	}
	result := Result{}
	duplicate := false
	if p.Type == feed.Heartbeat {
		if p.LastSequence != c.sourceSequence {
			return reject("heartbeat source continuity mismatch")
		}
	} else {
		tick, ok := c.ticks[p.Token]
		if !ok {
			return reject("unknown token")
		}
		if p.Type != feed.Cancel && p.Price%tick != 0 {
			return reject("off-tick price")
		}
		if p.Sequence == c.sourceSequence {
			if hash != c.sourceHash {
				return reject("conflicting source duplicate")
			}
			duplicate = true
			c.duplicates++
		} else {
			if int64(p.Sequence) != int64(c.sourceSequence)+1 {
				return reject(fmt.Sprintf("source gap/reset: expected%d got%d", int64(c.sourceSequence)+1, p.Sequence))
			}
			result, err = c.engine.Apply(p)
			if err != nil {
				return reject(err.Error())
			}
			for _, fill := range result.Fills {
				c.recent[c.recentNext] = Execution{Fill: fill, SourceSequence: p.Sequence, BrokerSequence: position}
				c.recentNext = (c.recentNext + 1) % len(c.recent)
				if c.recentCount < len(c.recent) {
					c.recentCount++
				}
			}
			c.sourceSequence = p.Sequence
			c.sourceHash = hash
			if c.engine.ActiveOrders() > c.peakActive {
				c.peakActive = c.engine.ActiveOrders()
			}
		}
	}
	c.brokerSequence = position
	c.brokerHash = hash
	if received.After(c.lastReceived) {
		c.lastReceived = received
	}
	return Outcome{Result: result, Sequence: p.Sequence, BrokerSequence: position, Duplicate: duplicate}, nil
}
func (c *Controller) Snapshot(token int32, depth int, now time.Time, stale time.Duration) (Snapshot, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if _, ok := c.ticks[token]; !ok {
		return Snapshot{}, fmt.Errorf("unknown token")
	}
	d, err := c.engine.Snapshot(token, depth)
	if err != nil {
		return Snapshot{}, err
	}
	status := "READY"
	reason := ""
	switch {
	case c.invalid != "":
		status = "INVALID"
		reason = c.invalid
	case !c.connected:
		status = "DISCONNECTED"
	case c.brokerSequence < c.replayThrough:
		status = "REBUILDING"
	case c.sourceSequence == 0:
		status = "UNINITIALIZED"
	case c.lastReceived.IsZero() || now.Sub(c.lastReceived) > stale || c.lastReceived.After(now.Add(time.Minute)):
		status = "STALE"
	}
	s := Snapshot{StreamID: c.manifest.StreamID, Policy: "shadow-price-time-replace-v1", Synthetic: true, Ephemeral: true, Status: status, Reason: reason,
		SourceSequence: c.sourceSequence, BrokerSequence: c.brokerSequence, ReplayThrough: c.replayThrough,
		ActiveOrders: c.engine.ActiveOrders(), PeakActiveOrders: c.peakActive, DuplicateDeliveries: c.duplicates, Stats: c.engine.Stats(), Depth: d}
	s.RecentExecutions = make([]Execution, c.recentCount)
	for i := range s.RecentExecutions {
		s.RecentExecutions[i] = c.recent[(c.recentNext-c.recentCount+i+len(c.recent))%len(c.recent)]
	}
	if !c.lastReceived.IsZero() {
		t := c.lastReceived
		s.LastReceived = &t
	}
	return s, nil
}
