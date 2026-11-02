// Host-only bounded retained-message readback; no broker writes/consumers.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/broker"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/tickerwire"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type wantEvent struct {
	order         outcomes.OrderSnapshot
	cause, broker string
}

func windows(text string) ([]int, error) {
	var a []int
	for _, s := range strings.Split(text, ",") {
		n, e := strconv.Atoi(s)
		if e != nil || n < 1 || n > 99999 || (len(a) > 0 && n <= a[len(a)-1]) {
			return nil, fmt.Errorf("strict increasing windows1..99999 required")
		}
		a = append(a, n)
	}
	return a, nil
}
func identity(n int) feed.OrderID { id, _ := feed.IDFromFloat64(float64(n)); return id }
func idhex(n int) string          { return hex.EncodeToString(identity(n).Bytes()) }
func pattern(n, tokens int) (int, int32, feed.Side) {
	local := (n-1)/tokens + 1
	if local%3 == 1 {
		return local, 100, feed.Buy
	}
	if local%3 == 2 {
		return local, 30, feed.Sell
	}
	return local, 70, feed.Sell
}
func rawPosition(n int, ends []int) uint64 {
	pos := n
	for _, end := range ends {
		if n > end {
			pos++
		}
	}
	return uint64(pos)
}
func expected(m runconfig.Manifest, ends []int) map[string]wantEvent {
	out := map[string]wantEvent{}
	tokens := len(m.Instruments)
	for n := 1; n <= ends[len(ends)-1]; n++ {
		local, q, side := pattern(n, tokens)
		inst := m.Instruments[(n-1)%tokens]
		remaining, filled, status := q, int32(0), "OPEN"
		if side == feed.Sell {
			remaining, filled, status = 0, q, "FILLED"
		}
		key := outcomes.EventID(m.StreamID, int32(n), identity(n))
		out[key] = wantEvent{outcomes.OrderSnapshot{OrderIDHex: idhex(n), Token: inst.Token, Side: string(side), PricePaise: inst.BasePrice, Status: status, AcceptedQty: strconv.Itoa(int(q)), FilledQty: strconv.Itoa(int(filled)), CancelledQty: "0", RemainingQty: remaining}, "NEW", strconv.FormatUint(rawPosition(n, ends), 10)}
		if side == feed.Sell {
			buy := n - tokens
			f, r, st := 30, int32(70), "PARTIALLY_FILLED"
			if local%3 == 0 {
				buy = n - 2*tokens
				f, r, st = 100, 0, "FILLED"
			}
			key = outcomes.EventID(m.StreamID, int32(n), identity(buy))
			out[key] = wantEvent{outcomes.OrderSnapshot{OrderIDHex: idhex(buy), Token: inst.Token, Side: "B", PricePaise: inst.BasePrice, Status: st, AcceptedQty: "100", FilledQty: strconv.Itoa(f), CancelledQty: "0", RemainingQty: r}, "MATCH", strconv.FormatUint(rawPosition(n, ends), 10)}
		}
	}
	return out
}
func read(ctx context.Context, js nats.JetStreamContext, stream string, count uint64) ([]*nats.RawStreamMsg, error) {
	info, e := js.StreamInfo(stream, nats.Context(ctx))
	if e != nil {
		return nil, e
	}
	if info.State.Msgs != count || info.State.FirstSeq != 1 || info.State.LastSeq != count || info.State.NumDeleted != 0 {
		return nil, fmt.Errorf("unexpected retained history %s: count=%d want=%d first=%d last=%d deleted=%d", stream, info.State.Msgs, count, info.State.FirstSeq, info.State.LastSeq, info.State.NumDeleted)
	}
	rows := make([]*nats.RawStreamMsg, 0, count)
	for n := uint64(1); n <= count; n++ {
		r, e := js.GetMsg(stream, n, nats.Context(ctx))
		if e != nil {
			return nil, e
		}
		if r.Sequence != n || r.Time.IsZero() {
			return nil, fmt.Errorf("invalid stored metadata")
		}
		rows = append(rows, r)
	}
	return rows, nil
}
func validateRaw(m runconfig.Manifest, digest string, r *nats.RawStreamMsg, n, hbend int) (time.Time, error) {
	if r.Subject != fmt.Sprintf("mtbt.%d", m.StreamID) || r.Header.Get(broker.ProfileHeader) != feed.Profile || r.Header.Get(broker.ManifestHeader) != digest || r.Header.Get(broker.VersionHeader) != "2" {
		return time.Time{}, fmt.Errorf("raw envelope mismatch")
	}
	for k := range r.Header {
		if strings.EqualFold(k, "Mtbt-Run") {
			return time.Time{}, fmt.Errorf("legacy identity")
		}
	}
	ns, e := strconv.ParseInt(r.Header.Get(broker.ReceivedHeader), 10, 64)
	if e != nil || ns <= 0 || ns > r.Time.UnixNano() {
		return time.Time{}, fmt.Errorf("invalid received/storage chronology")
	}
	p, e := feed.Decode(r.Data)
	if e != nil {
		return time.Time{}, e
	}
	if p.Stream != m.StreamID {
		return time.Time{}, fmt.Errorf("wrong native stream")
	}
	if n == 0 {
		if p.Type != feed.Heartbeat || p.LastSequence != int32(hbend) {
			return time.Time{}, fmt.Errorf("heartbeat position mismatch")
		}
	} else {
		_, q, side := pattern(n, len(m.Instruments))
		i := m.Instruments[(n-1)%len(m.Instruments)]
		if p.Type != feed.New || p.Sequence != int32(n) || p.OrderID != identity(n) || p.Token != i.Token || p.Side != side || p.Price != i.BasePrice || p.Quantity != q || p.Timestamp <= 0 || p.Timestamp > ns {
			return time.Time{}, fmt.Errorf("exact raw packet mismatch native%d", n)
		}
	}
	return time.Unix(0, ns).UTC(), nil
}
func validateEvent(m runconfig.Manifest, r *nats.RawStreamMsg, wants map[string]wantEvent, seen map[string]bool, versions map[string]int32) (outcomes.Event, error) {
	e, x := outcomes.Decode(r.Data)
	if x != nil {
		return e, x
	}
	w, ok := wants[e.EventID]
	if !ok || seen[e.EventID] || !reflect.DeepEqual(w.order, e.Order) || e.Cause != w.cause || e.InputBrokerSequence != w.broker || e.StreamID != m.StreamID || e.InputType != "N" || e.OrderVersion != e.SourceSequence || r.Header.Get(nats.MsgIdHdr) != e.EventID || r.Subject != fmt.Sprintf("mtbt.outcomes.%d", m.StreamID) {
		return e, fmt.Errorf("exact outcome mismatch %s", e.EventID)
	}
	at, x := time.Parse(time.RFC3339Nano, e.RecordedAt)
	if x != nil || at.IsZero() || at.After(r.Time) {
		return e, fmt.Errorf("invalid outcome chronology")
	}
	if versions[e.Order.OrderIDHex] >= e.OrderVersion {
		return e, fmt.Errorf("per-order version regression")
	}
	seen[e.EventID] = true
	versions[e.Order.OrderIDHex] = e.OrderVersion
	return e, nil
}
func write(out, name string, rows any) error {
	f, e := os.OpenFile(filepath.Join(out, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(rows)
}
func jsonl(out, name string, rows []map[string]any) error {
	f, e := os.OpenFile(filepath.Join(out, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	for _, r := range rows {
		if e = enc.Encode(r); e != nil {
			return e
		}
	}
	return nil
}
func run() error {
	path := flag.String("manifest", "", "required manifest")
	text := flag.String("windows", "", "native endpoints with heartbeat after each")
	topology := flag.String("topology", "combined", "combined or tickers")
	out := flag.String("out", "", "existing output directory")
	flag.Parse()
	if *topology != "combined" && *topology != "tickers" {
		return fmt.Errorf("unknown topology")
	}
	ends, e := windows(*text)
	if e != nil {
		return e
	}
	m, e := runconfig.Load(*path)
	if e != nil {
		return e
	}
	if len(m.Instruments) < 1 || len(m.Instruments) > 16 {
		return fmt.Errorf("bounded catalog required")
	}
	digest, e := m.Digest()
	if e != nil {
		return e
	}
	if s, e := os.Stat(*out); e != nil || !s.IsDir() {
		return fmt.Errorf("existing evidence directory required")
	}
	nc, e := nats.Connect(os.Getenv("NATS_URL"), nats.NoReconnect(), nats.Timeout(3*time.Second))
	if e != nil {
		return fmt.Errorf("NATS connection failed")
	}
	defer nc.Close()
	js, e := nc.JetStream()
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	total := ends[len(ends)-1]
	raw, e := read(ctx, js, fmt.Sprintf("MTBT_%d", m.StreamID), uint64(total+len(ends)))
	if e != nil {
		return e
	}
	original := map[uint64]*nats.RawStreamMsg{}
	received := map[uint64]time.Time{}
	routedWant := map[string]bool{}
	rawRows := []map[string]any{}
	n, stage := 1, 0
	for _, r := range raw {
		native, hbend := n, 0
		if n == ends[stage]+1 {
			native = 0
			hbend = ends[stage]
		}
		at, e := validateRaw(m, digest, r, native, hbend)
		if e != nil {
			return e
		}
		original[r.Sequence] = r
		received[r.Sequence] = at
		if native == 0 {
			for _, i := range m.Instruments {
				routedWant[fmt.Sprintf("%d:%d", r.Sequence, i.Token)] = true
			}
			stage++
		} else {
			routedWant[fmt.Sprintf("%d:%d", r.Sequence, m.Instruments[(n-1)%len(m.Instruments)].Token)] = true
			n++
		}
		rawRows = append(rawRows, map[string]any{"stored_unix_ns": r.Time.UnixNano(), "broker_sequence": r.Sequence, "source_sequence": native, "type": string(r.Data[8:9]), "received_unix_ns": at.UnixNano()})
	}
	if e = jsonl(*out, "raw-readback.jsonl", rawRows); e != nil {
		return e
	}
	routedCount := 0
	if *topology == "tickers" {
		routed, e := read(ctx, js, tickerwire.StreamName(m.StreamID), uint64(len(routedWant)))
		if e != nil {
			return e
		}
		local := map[int32]uint64{}
		previous := map[int32]string{}
		rows := []map[string]any{}
		for _, r := range routed {
			c, e := tickerwire.Decode(r.Data)
			if e != nil {
				return e
			}
			if e = tickerwire.Validate(m, c); e != nil {
				return e
			}
			key := fmt.Sprintf("%d:%d", c.SourceBroker, c.Token)
			original, ok := original[c.SourceBroker]
			if !ok || !routedWant[key] || !reflect.DeepEqual(c.Raw, original.Data) || !c.Received.Equal(received[c.SourceBroker]) || c.Sequence != local[c.Token]+1 || c.PreviousDigest != previous[c.Token] || r.Subject != tickerwire.Subject(m.StreamID, c.Token) || r.Header.Get(nats.MsgIdHdr) != tickerwire.ID(c) {
				return fmt.Errorf("routed provenance/digest mismatch")
			}
			delete(routedWant, key)
			local[c.Token] = c.Sequence
			previous[c.Token] = c.Digest
			rows = append(rows, map[string]any{"stored_unix_ns": r.Time.UnixNano(), "broker_sequence": r.Sequence, "source_broker": c.SourceBroker, "token": c.Token, "local_sequence": c.Sequence, "digest": c.Digest})
		}
		if len(routedWant) != 0 {
			return fmt.Errorf("missing routed fanout")
		}
		routedCount = len(routed)
		if e = jsonl(*out, "routed-readback.jsonl", rows); e != nil {
			return e
		}
	}
	wants := expected(m, ends)
	events, e := read(ctx, js, fmt.Sprintf("MTBT_OUTCOMES_%d", m.StreamID), uint64(len(wants)))
	if e != nil {
		return e
	}
	seen := map[string]bool{}
	versions := map[string]int32{}
	eventRows := []map[string]any{}
	for _, r := range events {
		ev, e := validateEvent(m, r, wants, seen, versions)
		if e != nil {
			return e
		}
		eventRows = append(eventRows, map[string]any{"stored_unix_ns": r.Time.UnixNano(), "broker_sequence": r.Sequence, "event": ev})
	}
	if len(seen) != len(wants) {
		return fmt.Errorf("missing outcomes")
	}
	if e = jsonl(*out, "outcome-readback.jsonl", eventRows); e != nil {
		return e
	}
	result := map[string]any{"complete": true, "topology": *topology, "exact_raw_payloads_and_envelopes": true, "exact_all_outcome_fields_and_fanout": true, "exact_routing_and_digest_chains": *topology == "tickers", "raw_count": len(raw), "routed_count": routedCount, "outcome_count": len(events), "no_application_consumer_modified": true}
	if e = write(*out, "readback-result.json", result); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
