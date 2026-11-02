package publisherpipeline

import (
	"context"
	"errors"
	"testing"
	"time"
)

type failingObserver struct {
	noop
	receive bool
}

func (o failingObserver) Receive(Datagram) error {
	if o.receive {
		return errors.New("observerwrite")
	}
	return nil
}
func (o failingObserver) Status(Snapshot) error { return errors.New("snapshotwrite") }
func TestDiagnosticWriteErrorsFailRun(t *testing.T) {
	for _, receive := range []bool{true, false} {
		r, w := sockets(t)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		done := make(chan error, 1)
		go func() {
			_, e := Run(ctx, r, &fakePublisher{auto: true}, config(), failingObserver{receive: receive})
			done <- e
		}()
		w.Write([]byte{1})
		if !receive {
			time.AfterFunc(20*time.Millisecond, cancel)
		}
		if e := <-done; e == nil {
			t.Fatal("lostdiagnosticerror")
		}
		cancel()
	}
}
func TestEmptyGracefulCancellation(t *testing.T) {
	r, _ := sockets(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, e := Run(ctx, r, &fakePublisher{}, config(), nil)
	if e != nil || s.Received != 0 || s.Confirmed != 0 {
		t.Fatal(s, e)
	}
}
