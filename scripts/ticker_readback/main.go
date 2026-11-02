// Host-only bounded post-run verification; never changes application consumers.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/outcomes"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/tickerwire"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("manifest", "", "required manifest")
	out := flag.String("out", "", "existing output directory")
	flag.Parse()
	m, e := runconfig.Load(*path)
	if e != nil {
		return e
	}
	if len(m.Instruments) != 2 || m.Instruments[0].Token != 1 || m.Instruments[1].Token != 2 {
		return fmt.Errorf("exact two-token fixture required")
	}
	if info, e := os.Stat(*out); e != nil || !info.IsDir() {
		return fmt.Errorf("existing evidence directory required")
	}
	nc, e := nats.Connect(os.Getenv("NATS_URL"), nats.NoReconnect(), nats.Timeout(3*time.Second))
	if e != nil {
		return fmt.Errorf("NATS connect failed")
	}
	defer nc.Close()
	js, e := nc.JetStream()
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Read-only direct retained-message API, no new consumer or delivery ACK.
	read := func(stream string, count uint64) ([]*nats.RawStreamMsg, error) {
		info, e := js.StreamInfo(stream, nats.Context(ctx))
		if e != nil {
			return nil, e
		}
		if info.State.Msgs != count || info.State.FirstSeq != 1 || info.State.LastSeq != count || info.State.NumDeleted != 0 {
			return nil, fmt.Errorf("unexpected retained history %s", stream)
		}
		var rows []*nats.RawStreamMsg
		for n := uint64(1); n <= count; n++ {
			r, e := js.GetMsg(stream, n, nats.Context(ctx))
			if e != nil {
				return nil, e
			}
			rows = append(rows, r)
		}
		return rows, nil
	}
	raw, e := read("MTBT_1", 14)
	if e != nil {
		return e
	}
	routed, e := read("MATCH_1", 16)
	if e != nil {
		return e
	}
	events, e := read("MTBT_OUTCOMES_1", 20)
	if e != nil {
		return e
	}
	original := map[uint64][]byte{}
	for _, r := range raw {
		p, e := feed.Decode(r.Data)
		if e != nil {
			return e
		}
		if r.Subject != "mtbt.1" {
			return fmt.Errorf("wrong raw subject")
		}
		original[r.Sequence] = r.Data
		if p.Type == feed.Heartbeat {
			if r.Sequence != 5 && r.Sequence != 14 {
				return fmt.Errorf("unexpected control position")
			}
		} else {
			expected := int32(r.Sequence)
			if r.Sequence > 5 {
				expected--
			}
			if p.Sequence != expected || p.Token != (p.Sequence-1)%2+1 {
				return fmt.Errorf("source fixture identity mismatch")
			}
		}
	}
	previous := map[int32]string{}
	local := map[int32]uint64{}
	for _, r := range routed {
		c, e := tickerwire.Decode(r.Data)
		if e != nil {
			return e
		}
		if e = tickerwire.Validate(m, c); e != nil {
			return e
		}
		if !reflect.DeepEqual(c.Raw, original[c.SourceBroker]) || c.PreviousDigest != previous[c.Token] || c.Sequence != local[c.Token]+1 || r.Subject != tickerwire.Subject(c.StreamID, c.Token) || r.Header.Get(nats.MsgIdHdr) != tickerwire.ID(c) {
			return fmt.Errorf("routed provenance/ordering mismatch")
		}
		previous[c.Token] = c.Digest
		local[c.Token] = c.Sequence
	}
	if local[1] != 8 || local[2] != 8 {
		return fmt.Errorf("control fanout mismatch")
	}
	expected := map[string]outcomes.OrderSnapshot{}
	causes := map[string]string{}
	brokers := map[string]string{}
	for n := 1; n <= 12; n++ {
		token := int32((n-1)%2 + 1)
		i := (n-1)/2 + 1
		mod := i % 3
		q := int32(100)
		side := "B"
		remaining := int32(100)
		filled := int64(0)
		status := "OPEN"
		if mod != 1 {
			side = "S"
			remaining = 0
			status = "FILLED"
			if mod == 2 {
				q = 30
			} else {
				q = 70
			}
			filled = int64(q)
		}
		id, _ := feed.IDFromFloat64(float64(n))
		snapshot := outcomes.OrderSnapshot{OrderIDHex: hex.EncodeToString(id.Bytes()), Token: token, Side: side, PricePaise: 10000, Status: status, AcceptedQty: strconv.Itoa(int(q)), FilledQty: strconv.FormatInt(filled, 10), CancelledQty: "0", RemainingQty: remaining}
		key := outcomes.EventID(m.StreamID, int32(n), id)
		expected[key] = snapshot
		causes[key] = "NEW"
		pos := n
		if n > 4 {
			pos++
		}
		brokers[key] = strconv.Itoa(pos)
		if mod != 1 {
			buy := n - 2
			if mod == 0 {
				buy = n - 4
			}
			bid, _ := feed.IDFromFloat64(float64(buy))
			f := int64(30)
			r := int32(70)
			st := "PARTIALLY_FILLED"
			if mod == 0 {
				f = 100
				r = 0
				st = "FILLED"
			}
			key = outcomes.EventID(m.StreamID, int32(n), bid)
			expected[key] = outcomes.OrderSnapshot{OrderIDHex: hex.EncodeToString(bid.Bytes()), Token: token, Side: "B", PricePaise: 10000, Status: st, AcceptedQty: "100", FilledQty: strconv.FormatInt(f, 10), CancelledQty: "0", RemainingQty: r}
			causes[key] = "MATCH"
			brokers[key] = strconv.Itoa(pos)
		}
	}
	seen := map[string]bool{}
	versions := map[string]int32{}
	for _, r := range events {
		ev, e := outcomes.Decode(r.Data)
		if e != nil {
			return e
		}
		want, ok := expected[ev.EventID]
		if !ok || seen[ev.EventID] || !reflect.DeepEqual(want, ev.Order) || ev.Cause != causes[ev.EventID] || ev.InputBrokerSequence != brokers[ev.EventID] || ev.StreamID != m.StreamID || ev.InputType != "N" || ev.OrderVersion != ev.SourceSequence || r.Header.Get(nats.MsgIdHdr) != ev.EventID {
			return fmt.Errorf("exact outcome snapshot/provenance mismatch %s", ev.EventID)
		}
		if versions[ev.Order.OrderIDHex] >= ev.OrderVersion {
			return fmt.Errorf("per-order version moved backwards")
		}
		versions[ev.Order.OrderIDHex] = ev.OrderVersion
		seen[ev.EventID] = true
	}
	if len(seen) != len(expected) {
		return fmt.Errorf("missing outcome")
	}
	for name, rows := range map[string][]*nats.RawStreamMsg{"raw": raw, "routed": routed, "outcomes": events} {
		f, e := os.OpenFile(filepath.Join(*out, name+"-readback.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		enc := json.NewEncoder(f)
		for _, r := range rows {
			if e = enc.Encode(r); e != nil {
				f.Close()
				return e
			}
		}
		if e = f.Close(); e != nil {
			return e
		}
	}
	result := map[string]any{"exact_raw_packets": 14, "exact_routed_commands": 16, "exact_outcome_payloads": 20, "digest_chains_and_provenance": true, "per_order_versions_ordered": true, "no_application_consumer_modified": true}
	f, e := os.OpenFile(filepath.Join(*out, "readback-result.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if e = json.NewEncoder(f).Encode(result); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
