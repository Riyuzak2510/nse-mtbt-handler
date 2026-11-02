package runconfig

import (
	"encoding/json"
	"nse-mtbt-handler/internal/feed"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func valid() Manifest {
	return Manifest{Version: 2, Profile: feed.Profile, StreamID: 1, Seed: 1, Baseline: "empty", CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Instruments: []Instrument{{Token: 1, Symbol: "SIM1", TickSize: 5, BasePrice: 100}}}
}
func TestValidate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Manifest)
	}{{"profile", func(m *Manifest) { m.Profile = "other" }}, {"legacy-version", func(m *Manifest) { m.Version = 1 }}, {"baseline", func(m *Manifest) { m.Baseline = "" }}, {"token", func(m *Manifest) { m.Instruments[0].Token = 0 }}, {"tick", func(m *Manifest) { m.Instruments[0].BasePrice = 101 }}, {"duplicate", func(m *Manifest) { m.Instruments = append(m.Instruments, m.Instruments[0]) }}, {"created", func(m *Manifest) { m.CreatedAt = time.Time{} }}} {
		t.Run(tc.name, func(t *testing.T) {
			m := valid()
			tc.change(&m)
			if m.Validate() == nil {
				t.Fatal("accepted invalid manifest")
			}
		})
	}
	m := valid()
	if e := m.Validate(); e != nil {
		t.Fatal(e)
	}
}
func TestLegacyRunIdentityRejected(t *testing.T) {
	m := valid()
	b, _ := json.Marshal(m)
	var fields map[string]any
	_ = json.Unmarshal(b, &fields)
	fields["run_id"] = "old-test"
	b, _ = json.Marshal(fields)
	path := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("accepted legacy identity in version2")
	}
	m.Version = 1
	if err := m.Validate(); err == nil {
		t.Fatal("accepted legacy version1")
	}
}

func TestDigestAndLoad(t *testing.T) {
	m := valid()
	a, e := m.Digest()
	if e != nil {
		t.Fatal(e)
	}
	p := filepath.Join(t.TempDir(), "manifest.json")
	b, _ := json.MarshalIndent(m, "", "  ")
	if e := os.WriteFile(p, b, 0600); e != nil {
		t.Fatal(e)
	}
	n, e := Load(p)
	if e != nil {
		t.Fatal(e)
	}
	c, _ := n.Digest()
	if a != c {
		t.Fatal("format changed digest")
	}
	n.CreatedAt = n.CreatedAt.Add(time.Hour)
	createdDigest, _ := n.Digest()
	if createdDigest != a {
		t.Fatal("creation timestamp became an implicit execution identity")
	}
	n.Seed++
	c, _ = n.Digest()
	if a == c {
		t.Fatal("seed not bound")
	}
	if !m.TokenAllowed(1) || m.TokenAllowed(2) {
		t.Fatal("token mapping")
	}
	if e := os.WriteFile(p, append(b, []byte(` {}`)...), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Load(p); e == nil {
		t.Fatal("trailing content accepted")
	}
}
