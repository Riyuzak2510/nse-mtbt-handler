package sqldiagnostics

import (
	"path/filepath"
	"testing"
	"time"
)

func TestWriteErrorStickyAndCloseVisible(t *testing.T) {
	r, e := NewRecorder(filepath.Join(t.TempDir(), "m"))
	if e != nil {
		t.Fatal(e)
	}
	r.file.Close()
	r.mu.Lock()
	r.lastFlush = time.Now().UnixNano() - 2e9
	r.mu.Unlock()
	if e = r.recordQuery("commit", 100, nil); e == nil {
		t.Fatal("lost encoding error")
	}
	if r.LastError() == nil {
		t.Fatal("lost sticky error")
	}
	if e = r.Close(); e == nil {
		t.Fatal("lost close error")
	}
}
