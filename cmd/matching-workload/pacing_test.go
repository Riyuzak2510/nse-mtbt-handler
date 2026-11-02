package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPacerUsesExactlyOneTickPerPacketWithoutAdditionalWait(t *testing.T) {
	ticks := make(chan time.Time, 4)
	future := time.Now().Add(time.Hour)
	for i := 0; i < 4; i++ {
		ticks <- future.Add(time.Duration(i) * time.Millisecond)
	}
	var seqs []int32
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- sendPaced(ctx, ticks, 10, 12, func(n int32, scheduled time.Time) error {
			if scheduled != future.Add(time.Duration(n-10)*time.Millisecond) {
				return errors.New("schedule changed")
			}
			seqs = append(seqs, n)
			return nil
		})
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal("pacer added a wall-clock wait after a scheduling tick")
	}
	if len(seqs) != 3 || seqs[0] != 10 || seqs[2] != 12 || len(ticks) != 1 {
		t.Fatal(seqs, len(ticks))
	}
}
func TestPacerCancellationAndFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(sendPaced(ctx, make(chan time.Time), 1, 2, func(int32, time.Time) error { t.Fatal("emitted after cancel"); return nil }), context.Canceled) {
		t.Fatal("cancel ignored")
	}
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	want := errors.New("send failed")
	if !errors.Is(sendPaced(context.Background(), ticks, 1, 2, func(int32, time.Time) error { return want }), want) {
		t.Fatal("send error lost")
	}
	close(ticks)
	if sendPaced(context.Background(), ticks, 1, 2, func(int32, time.Time) error { return nil }) == nil {
		t.Fatal("closed clock accepted")
	}
}
