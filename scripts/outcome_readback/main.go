// Host-only, post-run readback. Never calls broker.Connect or modifies the matching consumer.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"nse-mtbt-handler/internal/runconfig"
)

type rawRow struct {
	Broker          uint64 `json:"broker_sequence"`
	Source          int32  `json:"source_sequence"`
	Type            string `json:"type"`
	SourceTimestamp int64  `json:"source_timestamp_ns"`
	Received        int64  `json:"received_unix_ns"`
	Stored          int64  `json:"stored_unix_ns"`
}
type outcomeRow struct {
	Broker uint64         `json:"broker_sequence"`
	Stored int64          `json:"stored_unix_ns"`
	Event  outcomes.Event `json:"event"`
}

func parseWindows(text string) ([]int32, error) {
	var result []int32
	for _, s := range strings.Split(text, ",") {
		n, e := strconv.ParseInt(s, 10, 32)
		if e != nil || n < 1 || n > 99999 || len(result) > 0 && n <= int64(result[len(result)-1]) {
			return nil, fmt.Errorf("windows must be strictly increasing integers1..99999")
		}
		result = append(result, int32(n))
	}
	return result, nil
}
func rawLayout(windows []int32) []int32 {
	result := make([]int32, 0, int(windows[len(windows)-1])+len(windows))
	first := int32(1)
	for _, end := range windows {
		for n := first; n <= end; n++ {
			result = append(result, n)
		}
		result = append(result, 0)
		first = end + 1
	}
	return result
}
func pattern(n int32) (feed.Side, int32) {
	if n%3 == 1 {
		return feed.Buy, 100
	}
	if n%3 == 2 {
		return feed.Sell, 30
	}
	return feed.Sell, 70
}
func wireID(n int32) string {
	id, _ := feed.IDFromFloat64(float64(n))
	return hex.EncodeToString(id.Bytes())
}
func snapshots(n, token int32) []outcomes.OrderSnapshot {
	side, q := pattern(n)
	remaining := int32(0)
	status := "FILLED"
	if n%3 == 1 {
		remaining = 100
		status = "OPEN"
	}
	incoming := outcomes.OrderSnapshot{OrderIDHex: wireID(n), Token: token, Side: string(side), PricePaise: 10000, Status: status, AcceptedQty: strconv.Itoa(int(q)), FilledQty: strconv.Itoa(int(q - remaining)), CancelledQty: "0", RemainingQty: remaining}
	result := []outcomes.OrderSnapshot{incoming}
	if n%3 != 1 {
		prior := n - 1
		left, filled, oldstatus := int32(70), "30", "PARTIALLY_FILLED"
		if n%3 == 0 {
			prior = n - 2
			left = 0
			filled = "100"
			oldstatus = "FILLED"
		}
		result = append(result, outcomes.OrderSnapshot{OrderIDHex: wireID(prior), Token: token, Side: "B", PricePaise: 10000, Status: oldstatus, AcceptedQty: "100", FilledQty: filled, CancelledQty: "0", RemainingQty: left})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].OrderIDHex < result[j].OrderIDHex })
	return result
}
func expectedCount(total int32) uint64 {
	n := uint64(total/3) * 5
	switch total % 3 {
	case 1:
		n++
	case 2:
		n += 3
	}
	return n
}
func checkEvent(e outcomes.Event, stream int16, n int32, position uint64, snapshot outcomes.OrderSnapshot) error {
	cause := "MATCH"
	if snapshot.OrderIDHex == wireID(n) {
		cause = "NEW"
	}
	if e.StreamID != stream || e.SourceSequence != n || e.OrderVersion != n || e.InputType != "N" || e.Cause != cause || e.InputBrokerSequence != strconv.FormatUint(position, 10) || e.Order != snapshot {
		return fmt.Errorf("outcome snapshot/provenance mismatch at native%d", n)
	}
	return nil // Decode already validates schema, ID, synthetic marker and recorded_at.
}
func exclusive(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
}
func streamInfo(ctx context.Context, js nats.JetStreamContext, name, subject string, count uint64) (*nats.StreamInfo, error) {
	info, e := js.StreamInfo(name, nats.Context(ctx))
	if e != nil {
		return nil, e
	}
	v, s := info.Config, info.State
	if s.Msgs != count || s.FirstSeq != 1 || s.LastSeq != count || s.NumDeleted != 0 {
		return nil, fmt.Errorf("incomplete or deleted retained stream%s: %+v", name, s)
	}
	if len(v.Subjects) != 1 || v.Subjects[0] != subject || v.Storage != nats.FileStorage || v.Retention != nats.LimitsPolicy || v.Discard != nats.DiscardNew || v.Replicas != 1 {
		return nil, fmt.Errorf("incompatible retained stream%s", name)
	}
	return info, nil
}
func each(ctx context.Context, js nats.JetStreamContext, stream, subject string, count uint64, fn func(*nats.Msg, *nats.MsgMetadata) error) (err error) {
	name := "BENCH_READBACK_" + nuid.Next()
	if _, err = js.AddConsumer(stream, &nats.ConsumerConfig{Durable: name, FilterSubject: subject, AckPolicy: nats.AckNonePolicy, DeliverPolicy: nats.DeliverAllPolicy}, nats.Context(ctx)); err != nil {
		return err
	}
	defer func() {
		if e := js.DeleteConsumer(stream, name); err == nil {
			err = e
		}
	}()
	sub, e := js.PullSubscribe(subject, name, nats.Bind(stream, name))
	if e != nil {
		return e
	}
	defer sub.Unsubscribe()
	for seen := uint64(0); seen < count; {
		batch := 512
		if count-seen < uint64(batch) {
			batch = int(count - seen)
		}
		op, cancel := context.WithTimeout(ctx, 30*time.Second)
		messages, e := sub.Fetch(batch, nats.Context(op))
		cancel()
		if e != nil {
			return fmt.Errorf("read%s after%d: %w", stream, seen, e)
		}
		for _, msg := range messages {
			seen++
			meta, e := msg.Metadata()
			if e != nil {
				return e
			}
			if meta.Sequence.Stream != seen || meta.NumDelivered != 1 || msg.Subject != subject || meta.Timestamp.IsZero() {
				return fmt.Errorf("noncontiguous or redelivered observer stream%s at%d", stream, seen)
			}
			if e = fn(msg, meta); e != nil {
				return e
			}
		}
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() (err error) {
	manifest := flag.String("manifest", "", "native source manifest")
	windowsText := flag.String("windows", "", "increasing native endpoints; one heartbeat after each")
	out := flag.String("out", "", "existing output directory (files must not exist)")
	enabled := flag.Bool("outcomes", false, "require exact enabled outcome history")
	flag.Parse()
	windows, e := parseWindows(*windowsText)
	if e != nil {
		return e
	}
	total := windows[len(windows)-1]
	m, e := runconfig.Load(*manifest)
	if e != nil {
		return e
	}
	if len(m.Instruments) != 1 {
		return fmt.Errorf("matching workload requires one instrument")
	}
	digest, e := m.Digest()
	if e != nil {
		return e
	}
	info, e := os.Stat(*out)
	if e != nil || !info.IsDir() {
		return fmt.Errorf("output directory must exist")
	}
	url := os.Getenv("NATS_URL")
	if url == "" {
		return fmt.Errorf("NATS_URL required")
	}
	nc, e := nats.Connect(url, nats.Name("mtbt-postrun-benchmark-readback"), nats.NoReconnect(), nats.Timeout(3*time.Second))
	if e != nil {
		return fmt.Errorf("readback broker connection failed")
	}
	defer nc.Close()
	js, e := nc.JetStream(nats.MaxWait(5 * time.Second))
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	rawName, rawSubject := fmt.Sprintf("MTBT_%d", m.StreamID), fmt.Sprintf("mtbt.%d", m.StreamID)
	layout := rawLayout(windows)
	si, e := streamInfo(ctx, js, rawName, rawSubject, uint64(len(layout)))
	if e != nil {
		return e
	}
	if si.Config.Description != "mtbt-manifest-v2:"+digest {
		return fmt.Errorf("raw manifest digest mismatch")
	}
	file, e := exclusive(filepath.Join(*out, "raw-readback.jsonl"))
	if e != nil {
		return e
	}
	defer func() {
		if e := file.Close(); err == nil {
			err = e
		}
	}()
	encoder := json.NewEncoder(file)
	positions := make([]uint64, total+1)
	var hb int
	e = each(ctx, js, rawName, rawSubject, uint64(len(layout)), func(msg *nats.Msg, meta *nats.MsgMetadata) error {
		p, e := feed.Decode(msg.Data)
		if e != nil {
			return e
		}
		n := layout[meta.Sequence.Stream-1]
		h := msg.Header
		for key := range h {
			if strings.EqualFold(key, "Mtbt-Run") {
				return fmt.Errorf("legacy run header")
			}
		}
		received, e := strconv.ParseInt(h.Get(broker.ReceivedHeader), 10, 64)
		if e != nil || received <= 0 || h.Get(broker.ProfileHeader) != feed.Profile || h.Get(broker.ManifestHeader) != digest || h.Get(broker.VersionHeader) != "2" || p.Stream != m.StreamID {
			return fmt.Errorf("invalid raw envelope at%d", meta.Sequence.Stream)
		}
		if n == 0 {
			if p.Type != feed.Heartbeat || p.Sequence != 0 || hb >= len(windows) || p.LastSequence != windows[hb] {
				return fmt.Errorf("invalid heartbeat")
			}
			hb++
		} else {
			side, q := pattern(n)
			id, _ := feed.IDFromFloat64(float64(n))
			if p.Type != feed.New || p.Sequence != n || p.OrderID != id || p.Token != m.Instruments[0].Token || p.Side != side || p.Quantity != q || p.Price != 10000 || p.Timestamp <= 0 {
				return fmt.Errorf("unexpected raw packet native%d", n)
			}
			positions[n] = meta.Sequence.Stream
		}
		return encoder.Encode(rawRow{meta.Sequence.Stream, n, string(p.Type), p.Timestamp, received, meta.Timestamp.UnixNano()})
	})
	if e != nil {
		return e
	}
	result := map[string]any{"complete": true, "native_inputs": total, "raw_messages": len(layout), "heartbeats": hb, "exact_raw_payloads_and_envelopes": true, "outcomes_enabled": *enabled, "observer_policy": "separate temporary AckNone consumers; existing matching consumer unchanged; read after traffic"}
	outcomeName, outcomeSubject := fmt.Sprintf("MTBT_OUTCOMES_%d", m.StreamID), fmt.Sprintf("mtbt.outcomes.%d", m.StreamID)
	if !*enabled {
		if _, e := js.StreamInfo(outcomeName, nats.Context(ctx)); !errors.Is(e, nats.ErrStreamNotFound) {
			return fmt.Errorf("disabled arm unexpectedly has outcome stream or read failed")
		}
	} else {
		count := expectedCount(total)
		if _, e = streamInfo(ctx, js, outcomeName, outcomeSubject, count); e != nil {
			return e
		}
		f, e := exclusive(filepath.Join(*out, "outcome-readback.jsonl"))
		if e != nil {
			return e
		}
		defer func() {
			if e := f.Close(); err == nil {
				err = e
			}
		}()
		enc := json.NewEncoder(f)
		n, index := int32(1), 0
		want := snapshots(n, m.Instruments[0].Token)
		recorded := ""
		e = each(ctx, js, outcomeName, outcomeSubject, count, func(msg *nats.Msg, meta *nats.MsgMetadata) error {
			event, e := outcomes.Decode(msg.Data)
			if e != nil {
				return e
			}
			if e = checkEvent(event, m.StreamID, n, positions[n], want[index]); e != nil {
				return e
			}
			if msg.Header.Get(nats.MsgIdHdr) != event.EventID {
				return fmt.Errorf("outcome message id header mismatch")
			}
			if index == 0 {
				recorded = event.RecordedAt
			} else if recorded != event.RecordedAt {
				return fmt.Errorf("input snapshots differ in transaction timestamp")
			}
			if e = enc.Encode(outcomeRow{meta.Sequence.Stream, meta.Timestamp.UnixNano(), event}); e != nil {
				return e
			}
			index++
			if index == len(want) {
				n++
				index = 0
				if n <= total {
					want = snapshots(n, m.Instruments[0].Token)
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
		if n != total+1 || index != 0 {
			return fmt.Errorf("incomplete outcome fanout")
		}
		result["outcome_messages"] = count
		result["exact_all_outcome_fields_and_fanout"] = true
	}
	summary, e := exclusive(filepath.Join(*out, "readback-result.json"))
	if e != nil {
		return e
	}
	defer func() {
		if e := summary.Close(); err == nil {
			err = e
		}
	}()
	enc := json.NewEncoder(summary)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}
