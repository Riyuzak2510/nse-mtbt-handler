package timingtrace

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Parallel owns independent single-writer recorders, not a shared concurrent slice.
// The dispatcher/worker loops MUST stop and join before Close.
type Parallel struct {
	Dispatcher *Recorder
	Workers    []*Recorder
}

func NewParallel(path string, count, limit int) (*Parallel, error) {
	if path == "" {
		return nil, nil
	}
	if count < 1 || count > 16 {
		return nil, fmt.Errorf("parallel trace needs1..16shards")
	}
	if limit < 1 || limit > 2000000 || int64(count+1)*int64(limit) > 4000000 {
		return nil, fmt.Errorf("parallel trace capacity exceeds per-loop or total4million-row bound")
	}
	if err := os.Mkdir(path, 0700); err != nil {
		return nil, err
	}
	p := &Parallel{}
	var err error
	if p.Dispatcher, err = New(filepath.Join(path, "dispatcher.csv"), limit); err != nil {
		return nil, err
	}
	for i := 0; i < count; i++ {
		r, e := New(filepath.Join(path, fmt.Sprintf("shard-%d.csv", i)), limit)
		if e != nil {
			_ = p.Close()
			return nil, e
		}
		p.Workers = append(p.Workers, r)
	}
	return p, nil
}
func (p *Parallel) Close() error {
	if p == nil {
		return nil
	}
	errs := []error{p.Dispatcher.Close()}
	for _, r := range p.Workers {
		errs = append(errs, r.Close())
	}
	return errors.Join(errs...)
}
func (p *Parallel) DispatcherRecorder() *Recorder {
	if p == nil {
		return nil
	}
	return p.Dispatcher
}
func (p *Parallel) Worker(i int) *Recorder {
	if p == nil {
		return nil
	}
	return p.Workers[i]
}
