package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteNewRefusesOverwrite(t *testing.T) {
	p := filepath.Join(t.TempDir(), "nested", "manifest.json")
	if e := writeNew(p, map[string]int{"one": 1}); e != nil {
		t.Fatal(e)
	}
	before, e := os.ReadFile(p)
	if e != nil {
		t.Fatal(e)
	}
	if e := writeNew(p, map[string]int{"two": 2}); e == nil {
		t.Fatal("overwrote source configuration")
	}
	after, _ := os.ReadFile(p)
	if string(before) != string(after) {
		t.Fatal("changed original manifest")
	}
}
