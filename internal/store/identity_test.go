package store

import (
	"context"
	"os"
	"strings"
	"testing"
)

func TestLegacyIdentityMigrationRefusesWithoutDataLoss(t *testing.T) {
	dsn := os.Getenv("MTBT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set isolated PostgreSQL integration DSN")
	}
	ctx := context.Background()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	// Own only this test table, never drop or truncate application tables.
	if _, err = s.pool.Exec(ctx, `CREATE TABLE mtbt.legacy_identity_probe(run_id text NOT NULL); INSERT INTO mtbt.legacy_identity_probe VALUES('preserved')`); err != nil {
		t.Fatal(err)
	}
	defer s.pool.Exec(context.Background(), `DROP TABLE mtbt.legacy_identity_probe`)
	for _, populated := range []bool{true, false} {
		if !populated {
			if _, err = s.pool.Exec(ctx, `DELETE FROM mtbt.legacy_identity_probe`); err != nil {
				t.Fatal(err)
			}
		}
		err = s.Migrate(ctx)
		if err == nil || !strings.Contains(err.Error(), "Legacy run_id schema") {
			t.Fatalf("populated=%v guard=%v", populated, err)
		}
		var count int
		if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM mtbt.legacy_identity_probe`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if populated && count != 1 || !populated && count != 0 {
			t.Fatal("migration changed legacy rows")
		}
	}
}

func TestNativeStreamIsolationAndManifestPin(t *testing.T) {
	s, m := setup(t)
	ctx := context.Background()
	other := m
	other.StreamID = 2
	if err := s.Init(ctx, other); err != nil {
		t.Fatal(err)
	}
	defer s.pool.Exec(context.Background(), `DELETE FROM mtbt.feed_state WHERE stream_id=$1`, other.StreamID)
	if err := process(t, s, m, order(1, 1, 'B'), 1); err != nil {
		t.Fatal(err)
	}
	a, err := s.Snapshot(ctx, m, 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Snapshot(ctx, other, 1, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Bids) != 1 || len(b.Bids) != 0 {
		t.Fatal("native streams mixed", a, b)
	}
	incompatible := m
	incompatible.Seed++
	if err := s.Init(ctx, incompatible); err == nil {
		t.Fatal("accepted replacement configuration against retained stream")
	}
	if err := s.Init(ctx, m); err != nil {
		t.Fatal("same configuration did not resume", err)
	}
	var columns int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='mtbt' AND column_name='run_id'`).Scan(&columns); err != nil || columns != 0 {
		t.Fatal(columns, err)
	}
}
