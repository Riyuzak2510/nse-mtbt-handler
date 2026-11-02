package runconfig

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

func Load(path string) (Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return Manifest{}, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 1<<20))
	d.DisallowUnknownFields()
	var m Manifest
	if err := d.Decode(&m); err != nil {
		return m, fmt.Errorf("decode manifest: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return m, fmt.Errorf("manifest contains trailing data")
	}
	if err := m.Validate(); err != nil {
		return m, err
	}
	return m, nil
}
