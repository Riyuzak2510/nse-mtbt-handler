package main

import (
	"net"
	"nse-mtbt-handler/internal/ingestdiagnostics"
	"nse-mtbt-handler/internal/publisherpipeline"
	"nse-mtbt-handler/internal/timingtrace"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestAsyncObserverSerializesAndPreservesTrace(t *testing.T) {
	c, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	path := filepath.Join(t.TempDir(), "m")
	d, e := ingestdiagnostics.New(c, path)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	trace, e := timingtrace.New(filepath.Join(t.TempDir(), "t.csv"), 100)
	if e != nil {
		t.Fatal(e)
	}
	defer trace.Close()
	o, e := newAsyncObserver(d, trace, path)
	if e != nil {
		t.Fatal(e)
	}
	defer o.Close()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				p := publisherpipeline.Datagram{Raw: []byte{1}, Received: time.Now()}
				if i == 0 {
					if e := o.Receive(p); e != nil {
						t.Error(e)
					}
				} else if e := o.Confirm(p, time.Now(), time.Now(), nil); e != nil {
					t.Error(e)
				}
			}
		}()
	}
	wg.Wait()
	if e = o.Status(publisherpipeline.Snapshot{Mode: "async"}); e != nil {
		t.Fatal(e)
	}
	if _, e = newAsyncObserver(nil, nil, path); e == nil {
		t.Fatal("overwritten")
	}
}
