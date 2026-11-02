// Package ingestdiagnostics observes UDP receipt and confirmed broker admission.
// The caller serializes Receive/Publication; this recorder does not own buffers,
// change socket sizes or choose synchronous versus asynchronous publication.
package ingestdiagnostics

import (
	"encoding/json"
	"fmt"
	"net"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/loadmetrics"
	"os"
	"strconv"
	"strings"
	"time"
)

type Socket struct {
	Found        bool   `json:"found"`
	RXQueueBytes uint64 `json:"rx_queue_bytes"`
	Drops        uint64 `json:"drops"`
	Inode        string `json:"inode"`
}

// ParseUDP selects the exact local port; multiple bindings are rejected, not summed.
func ParseUDP(text string, port int) (Socket, error) {
	var result Socket
	for _, line := range strings.Split(text, "\n") {
		f := strings.Fields(line)
		if len(f) < 13 || !strings.Contains(f[1], ":") {
			continue
		}
		parts := strings.Split(f[1], ":")
		p, e := strconv.ParseUint(parts[len(parts)-1], 16, 16)
		if e != nil || int(p) != port {
			continue
		}
		if result.Found {
			return result, fmt.Errorf("multiple UDP sockets on port")
		}
		queues := strings.Split(f[4], ":")
		if len(queues) != 2 {
			return result, fmt.Errorf("invalid queues")
		}
		rx, e := strconv.ParseUint(queues[1], 16, 64)
		if e != nil {
			return result, e
		}
		drops, e := strconv.ParseUint(f[len(f)-1], 10, 64)
		if e != nil {
			return result, e
		}
		result = Socket{true, rx, drops, f[9]}
	}
	return result, nil
}
func ParseSNMP(text string) map[string]uint64 {
	out := map[string]uint64{}
	lines := strings.Split(text, "\n")
	for i := 0; i+1 < len(lines); i++ {
		head, values := strings.Fields(lines[i]), strings.Fields(lines[i+1])
		if len(head) == len(values) && len(head) > 1 && head[0] == "Udp:" && values[0] == "Udp:" {
			for j := 1; j < len(head); j++ {
				v, e := strconv.ParseUint(values[j], 10, 64)
				if e == nil {
					out[head[j]] = v
				}
			}
			break
		}
	}
	return out
}
func parseKV(text string) map[string]uint64 {
	m := map[string]uint64{}
	for _, l := range strings.Split(text, "\n") {
		f := strings.Fields(l)
		if len(f) == 2 {
			v, e := strconv.ParseUint(f[1], 10, 64)
			if e == nil {
				m[f[0]] = v
			}
		}
	}
	return m
}

type OSState struct {
	Socket        Socket            `json:"socket"`
	UDP           map[string]uint64 `json:"udp"`
	CPU           map[string]uint64 `json:"cgroup_cpu"`
	CPUQuota      string            `json:"cpu_max"`
	MemoryCurrent string            `json:"memory_current"`
	Errors        []string          `json:"errors,omitempty"`
}

func osState(port int) OSState {
	var s OSState
	read := func(path string) string {
		b, e := os.ReadFile(path)
		if e != nil {
			s.Errors = append(s.Errors, path+": "+e.Error())
			return ""
		}
		return string(b)
	}
	var e error
	s.Socket, e = ParseUDP(read("/proc/net/udp"), port)
	if e != nil {
		s.Errors = append(s.Errors, e.Error())
	}
	s.UDP = ParseSNMP(read("/proc/net/snmp"))
	s.CPU = parseKV(read("/sys/fs/cgroup/cpu.stat"))
	s.CPUQuota = strings.TrimSpace(read("/sys/fs/cgroup/cpu.max"))
	s.MemoryCurrent = strings.TrimSpace(read("/sys/fs/cgroup/memory.current"))
	return s
}

type Gap struct{ Previous, Next int32 }
type Report struct {
	Kind                   string              `json:"kind"`
	AtUnixNS               int64               `json:"at_unix_ns"`
	ElapsedSeconds         float64             `json:"elapsed_seconds"`
	Received               uint64              `json:"received"`
	Published              uint64              `json:"published"`
	PublishErrors          uint64              `json:"publish_errors"`
	Malformed              uint64              `json:"malformed"`
	LastSequence           int32               `json:"last_sequence"`
	GapEvents              uint64              `json:"gap_events"`
	MissingSequenceNumbers uint64              `json:"missing_sequence_numbers"`
	Reorders               uint64              `json:"reorders"`
	FirstGaps              []Gap               `json:"first_gaps"`
	SocketReadBuffer       int                 `json:"socket_read_buffer_bytes"`
	Start                  OSState             `json:"start"`
	Current                OSState             `json:"current"`
	Publish                loadmetrics.Summary `json:"publish"`
	ReceiveWait            loadmetrics.Summary `json:"read_wait"`
	EmittedToReceive       loadmetrics.Summary `json:"emitted_to_receive"`
	IntervalPublished      uint64              `json:"interval_published"`
	IntervalSeconds        float64             `json:"interval_seconds"`
}
type Recorder struct {
	f              *os.File
	enc            *json.Encoder
	port           int
	started, last  time.Time
	state          Report
	pub, read, lag loadmetrics.Histogram
	lastCount      uint64
	closed         bool
}

func New(conn *net.UDPConn, path string) (*Recorder, error) {
	if path == "" {
		return nil, nil
	}
	size, e := readBuffer(conn)
	if e != nil {
		return nil, e
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	now := time.Now()
	r := &Recorder{f: f, enc: json.NewEncoder(f), port: port, started: now, last: now}
	r.state.SocketReadBuffer = size
	r.state.Start = osState(port)
	if e = r.flush("startup"); e != nil {
		f.Close()
		return nil, e
	}
	return r, nil
}
func (r *Recorder) Enabled() bool { return r != nil }

// Receive and Publication are used by an externally serialized async observer.
// This recorder itself still belongs to one serialized owner.
func (r *Recorder) Receive(raw []byte, readWait time.Duration, received time.Time) error {
	if r == nil {
		return nil
	}
	if r.closed {
		return fmt.Errorf("ingest recorder closed")
	}
	r.state.Received++
	r.read.Record(readWait.Nanoseconds())
	p, e := feed.Decode(raw)
	if e != nil {
		r.state.Malformed++
	} else if p.Type != feed.Heartbeat {
		if r.state.LastSequence > 0 {
			if p.Sequence > r.state.LastSequence+1 {
				r.state.GapEvents++
				r.state.MissingSequenceNumbers += uint64(p.Sequence - r.state.LastSequence - 1)
				if len(r.state.FirstGaps) < 8 {
					r.state.FirstGaps = append(r.state.FirstGaps, Gap{r.state.LastSequence, p.Sequence})
				}
			} else if p.Sequence <= r.state.LastSequence {
				r.state.Reorders++
			}
		}
		r.state.LastSequence = p.Sequence
		if p.Timestamp > 0 {
			r.lag.Record(received.UnixNano() - p.Timestamp)
		}
	}

	return nil
}
func (r *Recorder) Publication(duration time.Duration, publishErr error) error {
	if r == nil {
		return nil
	}
	if r.closed {
		return fmt.Errorf("ingest recorder closed")
	}
	r.pub.Record(duration.Nanoseconds())
	if publishErr != nil {
		r.state.PublishErrors++
	} else {
		r.state.Published++
	}
	return r.Tick()
}
func (r *Recorder) Record(raw []byte, readWait, publishDuration time.Duration, received time.Time, publishErr error) error {
	if e := r.Receive(raw, readWait, received); e != nil {
		return e
	}
	return r.Publication(publishDuration, publishErr)
}
func (r *Recorder) Tick() error {
	if r == nil {
		return nil
	}
	if time.Since(r.last) >= time.Second {
		return r.flush("interval")
	}
	return nil
}
func (r *Recorder) flush(kind string) error {
	now := time.Now()
	r.state.Kind = kind
	r.state.AtUnixNS = now.UnixNano()
	r.state.ElapsedSeconds = now.Sub(r.started).Seconds()
	r.state.Current = osState(r.port)
	r.state.Publish = r.pub.Snapshot()
	r.state.ReceiveWait = r.read.Snapshot()
	r.state.EmittedToReceive = r.lag.Snapshot()
	r.state.IntervalPublished = r.state.Published - r.lastCount
	r.state.IntervalSeconds = now.Sub(r.last).Seconds()
	if e := r.enc.Encode(r.state); e != nil {
		return e
	}
	r.last = now
	r.lastCount = r.state.Published
	return nil
}
func (r *Recorder) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	e := r.flush("final")
	closeErr := r.f.Close()
	if e != nil {
		return e
	}
	return closeErr
}
