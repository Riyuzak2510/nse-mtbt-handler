package broker

import (
	"github.com/nats-io/nats.go"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"strconv"
	"testing"
	"time"
)

func TestEnvelopeValidation(t *testing.T) {
	m := runconfig.Manifest{StreamID: 1}
	c := Client{Subject: "mtbt.1", manifest: m, digest: "hash"}
	msg := nats.NewMsg(c.Subject)
	for k, v := range map[string]string{ProfileHeader: feed.Profile, ManifestHeader: "hash", VersionHeader: "2", ReceivedHeader: strconv.FormatInt(time.Now().UnixNano(), 10)} {
		msg.Header.Set(k, v)
	}
	if _, e := c.Validate(msg); e != nil {
		t.Fatal(e)
	}
	// Test rejection of legacy Mtbt-Run header
	legacyMsg := nats.NewMsg(c.Subject)
	for k, v := range map[string]string{ProfileHeader: feed.Profile, ManifestHeader: "hash", VersionHeader: "2", ReceivedHeader: strconv.FormatInt(time.Now().UnixNano(), 10)} {
		legacyMsg.Header.Set(k, v)
	}
	legacyMsg.Header.Set("Mtbt-Run", "run")
	if _, e := c.Validate(legacyMsg); e == nil {
		t.Fatal("accepted legacy Mtbt-Run header")
	}
	for _, key := range []string{ProfileHeader, ManifestHeader, VersionHeader, ReceivedHeader} {
		copy := nats.NewMsg(c.Subject)
		for k, values := range msg.Header {
			copy.Header[k] = append([]string(nil), values...)
		}
		copy.Header.Set(key, "bad")
		if _, e := c.Validate(copy); e == nil {
			t.Fatal("accepted invalid header", key)
		}
	}
}
