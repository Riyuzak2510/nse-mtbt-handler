package broker

import "testing"

func TestStreamLimits(t *testing.T) {
	for _, s := range []string{"-1", "unlimited", "12", "1099511627777"} {
		t.Setenv("MTBT_STREAM_MAX_BYTES", s)
		if _, err := StreamMaxBytes(); err == nil {
			t.Fatal(s)
		}
	}
	for _, s := range []string{"", "67108864", "8589934592"} {
		t.Setenv("MTBT_STREAM_MAX_BYTES", s)
		if _, err := StreamMaxBytes(); err != nil {
			t.Fatal(err)
		}
	}
}
