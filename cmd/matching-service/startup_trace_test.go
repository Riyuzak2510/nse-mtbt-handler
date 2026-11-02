package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDatabaseStartupFailureDoesNotReserveTracePath(t *testing.T) {
	dir := t.TempDir()
	manifest := filepath.Join(dir, "manifest.json")
	data, err := json.Marshal(testManifest())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manifest, data, 0600); err != nil {
		t.Fatal(err)
	}
	trace := filepath.Join(dir, "startup.csv")
	// A failed startup retry in the same Kubernetes pod must not poison an exclusive path.
	for i := 0; i < 2; i++ {
		err = runDurable(manifest, "nats://127.0.0.1:1", "127.0.0.1:0", false, 100, time.Second, trace, 10, "postgres://mtbt:test@127.0.0.1:1/mtbt?sslmode=disable&connect_timeout=1")
		if err == nil {
			t.Fatal("unreachable database accepted")
		}
		if _, err = os.Stat(trace); !os.IsNotExist(err) {
			t.Fatal("failed startup created trace", err)
		}
	}
}
