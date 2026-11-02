// Package broker provides sharded parallel feed subscription via token-mod routing.
// Shards routes feed subscribers across independent NATS consumers without matching.
package broker

import (
	"context"
	"fmt"
	"github.com/nats-io/nats.go"
	"math"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/sharding"
	"strconv"
	"time"
)

// Shards manages parallel independent shard consumers for token-modulo routing.
// Each shard has its own durable consumer on a separate filtered subject.
type Shards struct {
	client        *Client
	count         int
	subscriptions map[int]*nats.Subscription
	streamName    string
	streamSubject string
}

// OpenShards creates a new sharded consumer group with 'count' independent durable consumers.
// Creates a new NATS stream SHARDS_<stream_id> with subjects mtbtshard.<stream_id>.* using file storage,
// 24h retention, 64 MiB size limit, 65535 byte max message, discard-new policy, 1 replica.
// Each shard gets a durable consumer named shard-<id> with exact subject filtering,
// explicit ack, MaxAckPending=1, AckWait=30s, DeliverAll, ReplayInstant.
func OpenShards(ctx context.Context, c *Client, count int) (*Shards, error) {
	if err := sharding.ValidateCount(count); err != nil {
		return nil, err
	}

	streamID := strconv.Itoa(int(c.manifest.StreamID))
	streamName := "SHARDS_" + streamID
	streamSubject := "mtbtshard." + streamID + ".*"

	// Ensure shards stream exists with correct configuration
	desired := &nats.StreamConfig{
		Name:        streamName,
		Description: "mtbt-manifest-v2:" + c.digest,
		Subjects:    []string{streamSubject},
		Retention:   nats.LimitsPolicy,
		Storage:     nats.FileStorage,
		Discard:     nats.DiscardNew,
		MaxAge:      24 * time.Hour,
		MaxBytes:    64 << 20, // 64 MiB
		Replicas:    1,
		MaxMsgSize:  65535,
	}

	info, err := c.JS.StreamInfo(streamName, nats.Context(ctx))
	if err == nats.ErrStreamNotFound {
		info, err = c.JS.AddStream(desired, nats.Context(ctx))
		if err != nil {
			info, err = c.JS.StreamInfo(streamName, nats.Context(ctx))
		}
	}
	if err != nil {
		return nil, fmt.Errorf("shard stream: %w", err)
	}

	// Validate stream configuration
	v := info.Config
	if v.Description != desired.Description || len(v.Subjects) != 1 || v.Subjects[0] != streamSubject ||
		v.Retention != desired.Retention ||
		v.Storage != desired.Storage ||
		v.Discard != desired.Discard ||
		v.MaxAge != desired.MaxAge ||
		v.MaxBytes != desired.MaxBytes ||
		v.Replicas != 1 ||
		v.MaxMsgSize != desired.MaxMsgSize || v.NoAck {
		return nil, fmt.Errorf("shard stream has incompatible configuration")
	}

	s := &Shards{
		client:        c,
		count:         count,
		subscriptions: make(map[int]*nats.Subscription),
		streamName:    streamName,
		streamSubject: streamSubject,
	}

	// Create or verify durable consumers for each shard
	for i := 0; i < count; i++ {
		durableName := "shard-" + strconv.Itoa(i)
		shardSubject := "mtbtshard." + streamID + "." + strconv.Itoa(i)

		ci, err := c.JS.ConsumerInfo(streamName, durableName, nats.Context(ctx))
		if err == nats.ErrConsumerNotFound {
			ci, err = c.JS.AddConsumer(streamName, &nats.ConsumerConfig{
				Durable:       durableName,
				FilterSubject: shardSubject,
				AckPolicy:     nats.AckExplicitPolicy,
				AckWait:       30 * time.Second,
				MaxAckPending: 1,
				DeliverPolicy: nats.DeliverAllPolicy,
				ReplayPolicy:  nats.ReplayInstantPolicy,
			}, nats.Context(ctx))
			if err != nil {
				ci, err = c.JS.ConsumerInfo(streamName, durableName, nats.Context(ctx))
			}
		}
		if err != nil {
			return nil, fmt.Errorf("shard %d consumer: %w", i, err)
		}

		// Validate consumer configuration
		v2 := ci.Config
		if v2.Durable != durableName ||
			v2.DeliverSubject != "" ||
			v2.FilterSubject != shardSubject ||
			v2.AckPolicy != nats.AckExplicitPolicy ||
			v2.MaxAckPending != 1 ||
			v2.AckWait != 30*time.Second ||
			v2.DeliverPolicy != nats.DeliverAllPolicy ||
			v2.ReplayPolicy != nats.ReplayInstantPolicy {
			return nil, fmt.Errorf("shard %d consumer has incompatible configuration", i)
		}
	}

	return s, nil
}

// Subscribe creates a pull subscription for a specific shard.
// Returns an error if the shard index is out of range.
func (s *Shards) Subscribe(shard int) (*nats.Subscription, error) {
	if shard < 0 || shard >= s.count {
		return nil, fmt.Errorf("shard %d out of range [0,%d)", shard, s.count)
	}

	// Reject second subscription on same shard
	if _, exists := s.subscriptions[shard]; exists {
		return nil, fmt.Errorf("shard %d already subscribed", shard)
	}

	durableName := "shard-" + strconv.Itoa(shard)
	streamID := strconv.Itoa(int(s.client.manifest.StreamID))
	sub, err := s.client.JS.PullSubscribe(
		"mtbtshard."+streamID+"."+strconv.Itoa(shard),
		durableName,
		nats.Bind(s.streamName, durableName),
	)
	if err != nil {
		return nil, fmt.Errorf("shard %d subscribe: %w", shard, err)
	}

	s.subscriptions[shard] = sub
	return sub, nil
}

// Publish routes an event to the appropriate shard based on its token and publishes to the shards stream.
// Copies existing Client envelope headers (Mtbt-Envelope-Version, Mtbt-Profile, Mtbt-Manifest, Mtbt-Received)
// and adds new sharding headers: Mtbt-Shard, Mtbt-Shard-Sequence, Mtbt-Source-Broker, Mtbt-Shards, Mtbt-Routing.
// The original raw bytes are published unchanged.
func (s *Shards) Publish(ctx context.Context, event sharding.Event) error {
	// Validate identity (sourceBroker positive <=MaxInt64, sequence positive <=MaxInt64)
	if event.SourceBroker == 0 || event.SourceBroker > math.MaxInt64 {
		return fmt.Errorf("invalid source broker: must be positive and <= MaxInt64")
	}
	if event.Sequence == 0 || event.Sequence > math.MaxInt64 {
		return fmt.Errorf("invalid sequence: must be positive and <= MaxInt64")
	}
	if event.Received.IsZero() || event.Received.After(time.Now().Add(time.Minute)) {
		return fmt.Errorf("invalid received time")
	}
	p, err := feed.Decode(event.Raw)
	if err != nil {
		return err
	}
	if p.Type == feed.Heartbeat || p.Stream != s.client.manifest.StreamID || !s.client.manifest.TokenAllowed(p.Token) {
		return fmt.Errorf("invalid shard packet scope")
	}

	// Calculate the pinned instrument shard from the fully validated packet.
	shard, err := sharding.ForToken(p.Token, s.count)
	if err != nil {
		return fmt.Errorf("calculate shard: %w", err)
	}

	// Verify shard matches event
	if shard != event.Shard {
		return fmt.Errorf("event shard %d does not match token-calculated shard %d", event.Shard, shard)
	}

	// Build subject: mtbtshard.<streamid>.<shard>
	streamID := strconv.Itoa(int(s.client.manifest.StreamID))
	subject := "mtbtshard." + streamID + "." + strconv.Itoa(shard)

	// Create message with original raw bytes
	msg := nats.NewMsg(subject)
	msg.Data = event.Raw

	// Copy existing envelope headers from Client
	msg.Header.Set(ProfileHeader, feed.Profile)
	msg.Header.Set(ManifestHeader, s.client.digest)
	msg.Header.Set(VersionHeader, "2")
	msg.Header.Set(ReceivedHeader, strconv.FormatInt(event.Received.UTC().UnixNano(), 10))

	// Add sharding-specific headers
	msg.Header.Set("Mtbt-Shard", strconv.Itoa(shard))
	msg.Header.Set("Mtbt-Shard-Sequence", strconv.FormatUint(event.Sequence, 10))
	msg.Header.Set("Mtbt-Source-Broker", strconv.FormatUint(event.SourceBroker, 10))
	msg.Header.Set("Mtbt-Shards", strconv.Itoa(s.count))
	msg.Header.Set("Mtbt-Routing", sharding.Version)

	// Publish to shards stream
	ack, err := s.client.JS.PublishMsg(msg, nats.Context(ctx))
	if err != nil {
		return fmt.Errorf("publish to shard %d: %w", shard, err)
	}

	if ack.Stream != s.streamName {
		return fmt.Errorf("unexpected publication stream, expected %s got %s", s.streamName, ack.Stream)
	}

	return nil
}

// Validate extracts and validates a sharding.Event from a NATS message published to the shards stream.
// Validates all header ranges and content consistency:
// - Shard must be in [0, Mtbt-Shards)
// - Mtbt-Shard-Sequence must be positive
// - Mtbt-Source-Broker must be positive and fit PostgreSQL bigint
// - Run/Profile/Manifest/Received headers must match the original stream
// - Received timestamp must not be in the future
func (s *Shards) Validate(msg *nats.Msg) (sharding.Event, error) {
	var event sharding.Event

	// Strict v2: reject legacy Mtbt-Run header if present
	if hasLegacyRunHeader(msg.Header) {
		return event, fmt.Errorf("legacy Mtbt-Run header not supported, envelope v1 rejected")
	}

	// Validate envelope headers against original stream
	if msg.Header.Get(ProfileHeader) != feed.Profile {
		return event, fmt.Errorf("profile mismatch")
	}
	if msg.Header.Get(ManifestHeader) != s.client.digest {
		return event, fmt.Errorf("manifest mismatch")
	}
	if msg.Header.Get(VersionHeader) != "2" {
		return event, fmt.Errorf("envelope version mismatch")
	}

	// Validate received timestamp
	ns, err := strconv.ParseInt(msg.Header.Get(ReceivedHeader), 10, 64)
	if err != nil || ns <= 0 {
		return event, fmt.Errorf("invalid receive timestamp")
	}
	received := time.Unix(0, ns).UTC()
	if received.After(time.Now().Add(time.Minute)) {
		return event, fmt.Errorf("receive timestamp is in the future")
	}
	event.Received = received

	// Validate shard
	shardStr := msg.Header.Get("Mtbt-Shard")
	shard, err := strconv.Atoi(shardStr)
	if err != nil || shard < 0 {
		return event, fmt.Errorf("invalid shard")
	}

	// Validate Mtbt-Shards (total count)
	shardsStr := msg.Header.Get("Mtbt-Shards")
	shards, err := strconv.Atoi(shardsStr)
	if err != nil || shards <= 0 || shards > sharding.MaxShards {
		return event, fmt.Errorf("invalid shards count")
	}
	if shard >= shards {
		return event, fmt.Errorf("shard %d out of range [0,%d)", shard, shards)
	}
	if shards != s.count {
		return event, fmt.Errorf("shard count mismatch: expected %d, got %d", s.count, shards)
	}
	event.Shard = shard
	streamID := strconv.Itoa(int(s.client.manifest.StreamID))
	if msg.Subject != "mtbtshard."+streamID+"."+strconv.Itoa(shard) {
		return event, fmt.Errorf("shard subject does not match stream and shard identity")
	}

	// Validate sequence (positive <=MaxInt64)
	seqStr := msg.Header.Get("Mtbt-Shard-Sequence")
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil || seq == 0 || seq > math.MaxInt64 {
		return event, fmt.Errorf("invalid shard sequence: must be positive and <= MaxInt64")
	}
	event.Sequence = seq

	// Validate source broker (positive <=MaxInt64)
	sourceBrokerStr := msg.Header.Get("Mtbt-Source-Broker")
	sourceBroker, err := strconv.ParseUint(sourceBrokerStr, 10, 64)
	if err != nil || sourceBroker == 0 || sourceBroker > math.MaxInt64 {
		return event, fmt.Errorf("invalid source broker: must be positive and <= MaxInt64")
	}
	event.SourceBroker = sourceBroker

	// Validate routing version
	routing := msg.Header.Get("Mtbt-Routing")
	if routing != sharding.Version {
		return event, fmt.Errorf("routing version mismatch: expected %s, got %s", sharding.Version, routing)
	}

	// Store raw data and decode/validate feed packet
	event.Raw = msg.Data

	// Decode packet to verify it's not heartbeat, has valid stream, and known allowed token
	packet, err := feed.Decode(event.Raw)
	if err != nil {
		return event, fmt.Errorf("decode feed packet: %w", err)
	}

	// Reject heartbeat packets
	if packet.Type == feed.Heartbeat {
		return event, fmt.Errorf("heartbeat packets cannot be routed")
	}

	if packet.Stream != s.client.manifest.StreamID || !s.client.manifest.TokenAllowed(packet.Token) {
		return event, fmt.Errorf("packet stream or instrument differs from manifest")
	}
	expectedShard, err := sharding.ForToken(packet.Token, s.count)
	if err != nil || expectedShard != shard {
		return event, fmt.Errorf("packet routed to wrong shard")
	}

	return event, nil
}

// Close closes all active subscriptions.
func (s *Shards) Close() error {
	for _, sub := range s.subscriptions {
		sub.Unsubscribe()
	}
	return nil
}

// decodePacketForToken extracts the token from a raw feed packet.
// Uses feed.Decode to perform full packet validation.
func decodePacketForToken(raw []byte) (struct{ Token int32 }, error) {
	var result struct{ Token int32 }

	// Use full feed.Decode to validate packet structure
	packet, err := feed.Decode(raw)
	if err != nil {
		return result, err
	}

	result.Token = packet.Token
	return result, nil
}
