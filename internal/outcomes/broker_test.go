package outcomes

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"nse-mtbt-handler/internal/feed"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxBytes != 64*1024*1024 {
		t.Errorf("MaxBytes: want 64 MiB, got %d", cfg.MaxBytes)
	}
	if cfg.MaxAge != 24*time.Hour {
		t.Errorf("MaxAge: want 24h, got %v", cfg.MaxAge)
	}
	if cfg.DuplicateWindow != 10*time.Minute {
		t.Errorf("DuplicateWindow: want 10m, got %v", cfg.DuplicateWindow)
	}
	if cfg.Replicas != 1 {
		t.Errorf("Replicas: want 1, got %d", cfg.Replicas)
	}
}

func TestNames_StreamID1(t *testing.T) {
	streamName, subject := Names(1)
	if streamName != "MTBT_OUTCOMES_1" {
		t.Errorf("streamName: want MTBT_OUTCOMES_1, got %s", streamName)
	}
	if subject != "mtbt.outcomes.1" {
		t.Errorf("subject: want mtbt.outcomes.1, got %s", subject)
	}
}

func TestNames_LargeStreamID(t *testing.T) {
	streamName, subject := Names(32767)
	if streamName != "MTBT_OUTCOMES_32767" {
		t.Errorf("streamName: want MTBT_OUTCOMES_32767, got %s", streamName)
	}
	if subject != "mtbt.outcomes.32767" {
		t.Errorf("subject: want mtbt.outcomes.32767, got %s", subject)
	}
}

func TestConnect_InvalidStream(t *testing.T) {
	tests := []struct {
		stream int16
		name   string
	}{
		{0, "zero"},
		{-1, "negative"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Connect("nats://localhost:4222", tt.stream, DefaultConfig())
			if err == nil {
				t.Fatalf("Connect() should fail for stream %d", tt.stream)
			}
		})
	}
}

func TestPublisher_PublishWithoutConnect(t *testing.T) {
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

	e, _ := New(1, 1, 1, feed.New, "NEW", order, time.Now())
	payload, _ := Encode(e)

	// Try to publish with nil publisher
	var p *Publisher
	_, err := p.Publish(context.Background(), e, payload)
	if err == nil {
		t.Fatalf("Publish() should fail on nil publisher")
	}
}

func TestPublisher_PublishWrongStream(t *testing.T) {
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

	// Create event for stream 2
	e, _ := New(2, 1, 1, feed.New, "NEW", order, time.Now())
	payload, _ := Encode(e)

	// Create a mock publisher struct with stream 1
	p := &Publisher{
		conn:       nil,
		streamID:   1, // different from event
		streamName: "MTBT_OUTCOMES_1",
		subject:    "mtbt.outcomes.1",
	}

	_, err := p.Publish(context.Background(), e, payload)
	if err == nil {
		t.Fatalf("Publish() should fail for stream mismatch")
	}
}

func TestPublisher_Close_Nil(t *testing.T) {
	var p *Publisher
	p.Close() // Should not panic
}

func TestPublisher_Close_Safe(t *testing.T) {
	p := &Publisher{
		conn: nil,
	}
	p.Close() // Should not panic
}

// Integration tests with real NATS (optional, requires MTBT_TEST_NATS_URL environment variable)

func TestConnect_RealNATS(t *testing.T) {
	natsURL := os.Getenv("MTBT_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("MTBT_TEST_NATS_URL not set; skipping real NATS test")
	}

	cfg := DefaultConfig()
	p, err := Connect(natsURL, 10001, cfg)
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	defer p.Close()

	if p.streamID != 10001 {
		t.Errorf("streamID: want 10001, got %d", p.streamID)
	}
	if p.streamName != "MTBT_OUTCOMES_10001" {
		t.Errorf("streamName: want MTBT_OUTCOMES_10001, got %s", p.streamName)
	}
}

func TestPublish_RealNATS(t *testing.T) {
	natsURL := os.Getenv("MTBT_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("MTBT_TEST_NATS_URL not set; skipping real NATS test")
	}

	cfg := DefaultConfig()
	streamID := int16(10000 + (time.Now().UnixNano() % 20000))

	p, err := Connect(natsURL, streamID, cfg)
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	defer p.Close()

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

	e, err := New(streamID, 1001, 1001, feed.New, "NEW", order, time.Now())
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	payload, err := Encode(e)
	if err != nil {
		t.Fatalf("Encode() failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ack, err := p.Publish(ctx, e, payload)
	if err != nil {
		t.Fatalf("Publish() failed: %v", err)
	}

	if ack == nil {
		t.Fatalf("Publish() returned nil ack")
	}

	if ack.Stream != p.streamName {
		t.Errorf("ack.Stream: want %s, got %s", p.streamName, ack.Stream)
	}

	if ack.Sequence == 0 {
		t.Errorf("ack.Sequence should be non-zero")
	}
}

func TestPublish_Duplicate_RealNATS(t *testing.T) {
	natsURL := os.Getenv("MTBT_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("MTBT_TEST_NATS_URL not set; skipping real NATS test")
	}

	cfg := DefaultConfig()
	streamID := int16(10000 + (time.Now().UnixNano() % 20000))

	p, err := Connect(natsURL, streamID, cfg)
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	defer p.Close()

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

	e, _ := New(streamID, 2001, 2001, feed.New, "NEW", order, time.Now())
	payload, _ := Encode(e)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish once
	ack1, err := p.Publish(ctx, e, payload)
	if err != nil {
		t.Fatalf("First Publish() failed: %v", err)
	}

	// Publish same event again (should be deduplicated by NATS via Nats-Msg-Id)
	ack2, err := p.Publish(ctx, e, payload)
	if err != nil {
		t.Fatalf("Second Publish() failed: %v", err)
	}

	// Both acks should succeed and report same sequence (duplicate)
	if ack1.Sequence != ack2.Sequence {
		t.Errorf("Duplicate publish: sequences differ: %d vs %d", ack1.Sequence, ack2.Sequence)
	}
}

func TestPublish_MultipleEvents_RealNATS(t *testing.T) {
	natsURL := os.Getenv("MTBT_TEST_NATS_URL")
	if natsURL == "" {
		t.Skip("MTBT_TEST_NATS_URL not set; skipping real NATS test")
	}

	cfg := DefaultConfig()
	streamID := int16(10000 + (time.Now().UnixNano() % 20000))

	p, err := Connect(natsURL, streamID, cfg)
	if err != nil {
		t.Fatalf("Connect() failed: %v", err)
	}
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish multiple distinct events
	for i := 0; i < 5; i++ {
		order := OrderSnapshot{
			OrderIDHex:   "000000000000f03f",
			Token:        1,
			Side:         "B",
			PricePaise:   10000,
			Status:       "PARTIALLY_FILLED",
			AcceptedQty:  "100",
			FilledQty:    strconv.Itoa(i),
			CancelledQty: "0",
			RemainingQty: 100 - int32(i),
		}

		if i == 0 {
			order.Status = "OPEN"
		}
		e, _ := New(streamID, int32(3000+i), uint64(3000+i), feed.New, "NEW", order, time.Now())
		payload, _ := Encode(e)

		ack, err := p.Publish(ctx, e, payload)
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}

		if ack.Sequence == 0 {
			t.Errorf("Event %d: sequence should be non-zero", i)
		}
	}
}
