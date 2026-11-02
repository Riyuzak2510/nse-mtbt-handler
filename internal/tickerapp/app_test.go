package tickerapp

import (
	"flag"
	"testing"
)

func TestTimingsFlags(t *testing.T) {
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	timings := f.String("timings", "", "opt-in benchmark trace CSV path")
	timingsLimit := f.Int("timings-limit", 50000, "max samples per trace")

	if e := f.Parse([]string{"-timings", "/tmp/test.csv", "-timings-limit", "1000"}); e != nil {
		t.Fatal(e)
	}

	if *timings != "/tmp/test.csv" {
		t.Fatalf("expected /tmp/test.csv, got %s", *timings)
	}
	if *timingsLimit != 1000 {
		t.Fatalf("expected 1000, got %d", *timingsLimit)
	}
}

func TestTimingsDisabledByDefault(t *testing.T) {
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	timings := f.String("timings", "", "opt-in benchmark trace CSV path")
	timingsLimit := f.Int("timings-limit", 50000, "max samples per trace")

	if e := f.Parse([]string{}); e != nil {
		t.Fatal(e)
	}

	if *timings != "" {
		t.Fatalf("expected empty, got %s", *timings)
	}
	if *timingsLimit != 50000 {
		t.Fatalf("expected 50000, got %d", *timingsLimit)
	}
}
