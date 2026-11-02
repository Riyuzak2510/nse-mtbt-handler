package ingestdiagnostics

import (
	"bufio"
	"encoding/json"
	"net"
	"nse-mtbt-handler/internal/feed"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseUDP(t *testing.T) {
	text := "sl local_address rem_address st tx_queue:rx_queue tr:tm->when retrnsmt uid timeout inode ref pointer drops\n 5: 00000000:4A39 00000000:0000 07 00000000:00000200 00:00000000 00000000 10001 0 12345 2 0000000000000000 42\n"
	s, e := ParseUDP(text, 19001)
	if e != nil || !s.Found || s.Drops != 42 || s.RXQueueBytes != 512 || s.Inode != "12345" {
		t.Fatal(s, e)
	}
	if _, e = ParseUDP(text+text, 19001); e == nil {
		t.Fatal("ambiguous socket")
	}
	s, e = ParseUDP(text, 1234)
	if e != nil || s.Found {
		t.Fatal(s, e)
	}
}
func TestSNMP(t *testing.T) {
	m := ParseSNMP("Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors\nUdp: 100 0 5 200 5 0\n")
	if m["RcvbufErrors"] != 5 || m["InErrors"] != 5 {
		t.Fatal(m)
	}
}
func TestRecorderGapAndExclusive(t *testing.T) {
	c, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	path := filepath.Join(t.TempDir(), "m")
	r, e := New(c, path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = New(c, path); e == nil {
		t.Fatal("overwrite")
	}
	for _, n := range []int32{1, 2, 22, 23} {
		id, _ := feed.IDFromFloat64(float64(n))
		b, _ := feed.Encode(feed.Packet{Stream: 1, Sequence: n, Type: feed.New, Timestamp: time.Now().UnixNano() - 1000, OrderID: id, Token: 1, Side: feed.Buy, Price: 100, Quantity: 1})
		if e = r.Record(b, time.Microsecond, 2*time.Microsecond, time.Now(), nil); e != nil {
			t.Fatal(e)
		}
	}
	if e = r.Close(); e != nil {
		t.Fatal(e)
	}
	f, _ := os.Open(path)
	defer f.Close()
	sc := bufio.NewScanner(f)
	var last Report
	for sc.Scan() {
		if e = json.Unmarshal(sc.Bytes(), &last); e != nil {
			t.Fatal(e)
		}
	}
	if last.Published != 4 || last.GapEvents != 1 || last.MissingSequenceNumbers != 19 || last.FirstGaps[0].Next != 22 || last.Publish.MeanMS != .002 || last.Kind != "final" {
		t.Fatal(last)
	}
}
func TestNilRecorder(t *testing.T) {
	var r *Recorder
	if r.Enabled() {
		t.Fatal("enabled")
	}
	if e := r.Tick(); e != nil {
		t.Fatal(e)
	}
	if e := r.Close(); e != nil {
		t.Fatal(e)
	}
}
