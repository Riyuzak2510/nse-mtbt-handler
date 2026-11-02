package durable

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"math"
	"nse-mtbt-handler/internal/feed"
	"nse-mtbt-handler/internal/runconfig"
	"nse-mtbt-handler/internal/store"
	"nse-mtbt-handler/internal/tickerwire"
	"sync"
	"time"
)

const MaxRoutingRows int64 = 100000
const MaxRoutingBytes int64 = 128 << 20

func RoutingChannel(stream int16) string { return fmt.Sprintf("mtbt_match_ready_%d", stream) }

// Intake owns native admission only; matching owns subsequent business changes.
type Intake struct {
	mu           sync.Mutex
	db           *store.Store
	conn         *pgxpool.Conn
	m            runconfig.Manifest
	failed       error
	beforeCommit func(pgx.Tx) error
}

func OpenIntake(ctx context.Context, dsn string, m runconfig.Manifest) (s *Intake, err error) {
	if e := m.Validate(); e != nil {
		return nil, e
	}
	if len(m.Instruments) > 16 {
		return nil, fmt.Errorf("ticker topology supports at most16 configured instruments in local v1")
	}
	db, e := store.OpenWithPool(ctx, dsn, 3)
	if e != nil {
		return nil, e
	}
	s = &Intake{db: db, m: m}
	ok := false
	owned := s
	defer func() {
		if !ok {
			owned.Close()
		}
	}()
	if e = db.Migrate(ctx); e != nil {
		return nil, e
	}
	if e = db.InitMode(ctx, m, "matching"); e != nil {
		return nil, e
	}
	s.conn, e = db.Pool().Acquire(ctx)
	if e != nil {
		return nil, e
	}
	if e = lease(ctx, s.conn, "mtbt-stream:", m.StreamID, 0); e != nil {
		return nil, e
	}
	tx, e := s.conn.Begin(ctx)
	if e != nil {
		return nil, e
	}
	defer tx.Rollback(ctx)
	var exists bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM mtbt.ticker_topology WHERE stream_id=$1)", m.StreamID).Scan(&exists); e != nil {
		return nil, e
	}
	if !exists {
		var source, broker, count, other int64
		var enabled *int32
		if e = tx.QueryRow(ctx, `SELECT source_sequence,broker_sequence,outcomes_start_sequence FROM mtbt.feed_state WHERE stream_id=$1 FOR UPDATE`, m.StreamID).Scan(&source, &broker, &enabled); e != nil {
			return nil, e
		}
		if e = tx.QueryRow(ctx, "SELECT count(*) FROM mtbt.orders WHERE stream_id=$1", m.StreamID).Scan(&count); e != nil {
			return nil, e
		}
		if e = tx.QueryRow(ctx, "SELECT count(*) FROM mtbt.subscriber_topology WHERE stream_id=$1", m.StreamID).Scan(&other); e != nil {
			return nil, e
		}
		if source != 0 || broker != 0 || count != 0 || enabled != nil || other != 0 {
			return nil, fmt.Errorf("ticker mode requires pristine stream; no automatic conversion")
		}
		// Old orders constraints must not be silently altered on retained schemas.
		var pending bool
		if e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='mtbt.orders'::regclass AND pg_get_constraintdef(oid) LIKE '%PENDING%')`).Scan(&pending); e != nil {
			return nil, e
		}
		if !pending {
			return nil, fmt.Errorf("fresh PENDING-capable schema required")
		}
		if _, e = tx.Exec(ctx, `INSERT INTO mtbt.ticker_topology VALUES($1,'ticker-v1',$2,$3)`, m.StreamID, MaxRoutingRows, MaxRoutingBytes); e != nil {
			return nil, e
		}
		if e = writer(ctx, tx, "intake", 0); e != nil {
			return nil, e
		}
		if _, e = tx.Exec(ctx, `UPDATE mtbt.feed_state SET outcomes_start_sequence=1,outcomes_max_rows=100000,outcomes_max_bytes=134217728 WHERE stream_id=$1`, m.StreamID); e != nil {
			return nil, e
		}
		for _, i := range m.Instruments {
			if _, e = tx.Exec(ctx, "INSERT INTO mtbt.ticker_progress(stream_id,token) VALUES($1,$2)", m.StreamID, i.Token); e != nil {
				return nil, e
			}
			if _, e = tx.Exec(ctx, "INSERT INTO mtbt.ticker_applied(stream_id,token) VALUES($1,$2)", m.StreamID, i.Token); e != nil {
				return nil, e
			}
		}
	}
	var count int
	if e = tx.QueryRow(ctx, "SELECT count(*) FROM mtbt.ticker_progress WHERE stream_id=$1", m.StreamID).Scan(&count); e != nil {
		return nil, e
	}
	if count != len(m.Instruments) {
		return nil, fmt.Errorf("ticker topology differs")
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	ok = true
	return s, nil
}
func writer(ctx context.Context, tx pgx.Tx, role string, token int32) error {
	_, e := tx.Exec(ctx, `SELECT set_config('mtbt.outcome_writer','v1',true),set_config('mtbt.ticker_writer',$1,true),set_config('mtbt.ticker_token',$2,true)`, role, fmt.Sprint(token))
	return e
}
func lease(ctx context.Context, c *pgxpool.Conn, prefix string, stream int16, token int32) error {
	key := fmt.Sprint(prefix, stream)
	if token != 0 {
		key = fmt.Sprintf("%s%d:%d", prefix, stream, token)
	}
	h := sha256.Sum256([]byte(key))
	var n int64
	for i := 7; i >= 0; i-- {
		n = n<<8 | int64(h[i])
	}
	var ok bool
	if e := c.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", n).Scan(&ok); e != nil {
		return e
	}
	if !ok {
		return fmt.Errorf("another owner holds %s", key)
	}
	return nil
}
func (s *Intake) Close() {
	if s == nil {
		return
	}
	if s.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = s.conn.Conn().Close(ctx)
		cancel()
		s.conn.Release()
		s.conn = nil
	}
	if s.db != nil {
		s.db.Close()
	}
}
func (s *Intake) Position(ctx context.Context) (uint64, error) {
	var n int64
	e := s.db.Pool().QueryRow(ctx, "SELECT broker_sequence FROM mtbt.feed_state WHERE stream_id=$1", s.m.StreamID).Scan(&n)
	return uint64(n), e
}
func (s *Intake) Process(ctx context.Context, raw []byte, position uint64, received time.Time) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return fmt.Errorf("intake restart required: %w", s.failed)
	}
	defer func() {
		if err != nil {
			s.failed = err
		}
	}()
	if position == 0 || position > math.MaxInt64 || received.IsZero() || received.After(time.Now().Add(time.Minute)) {
		return fmt.Errorf("invalid admission metadata")
	}
	tx, e := s.conn.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	if e = writer(ctx, tx, "intake", 0); e != nil {
		return e
	}
	var source int32
	var broker int64
	var sh, bh []byte
	var state string
	if e = tx.QueryRow(ctx, `SELECT source_sequence,broker_sequence,source_hash,broker_hash,status FROM mtbt.feed_state WHERE stream_id=$1 FOR NO KEY UPDATE`, s.m.StreamID).Scan(&source, &broker, &sh, &bh, &state); e != nil {
		return e
	}
	if state == "INVALID" {
		return fmt.Errorf("native stream invalid")
	}
	h := sha256.Sum256(raw)
	reject := func(why string) error {
		if _, x := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1,$2)", int32(s.m.StreamID)+300000, int32(0)); x != nil {
			return x
		}
		_, x := tx.Exec(ctx, "UPDATE mtbt.feed_state SET status='INVALID',reason=$2 WHERE stream_id=$1", s.m.StreamID, why)
		if x == nil {
			x = tx.Commit(ctx)
		}
		if x != nil {
			return x
		}
		return fmt.Errorf("invalid admission: %s", why)
	}
	if position <= uint64(broker) {
		if position == uint64(broker) && bytes.Equal(h[:], bh) {
			return nil
		}
		return reject("conflicting or historical raw redelivery")
	}
	if position != uint64(broker)+1 {
		return reject("raw broker gap")
	}
	p, e := feed.Decode(raw)
	if e != nil {
		return reject(e.Error())
	}
	if p.Stream != s.m.StreamID {
		return reject("native stream mismatch")
	}
	apply := true
	if p.Type == feed.Heartbeat {
		if p.LastSequence != source {
			return reject("heartbeat continuity mismatch")
		}
	} else {
		if !s.m.TokenAllowed(p.Token) || p.Type != feed.Cancel && !onTick(s.m, p.Token, p.Price) {
			return reject("unknown instrument/off tick")
		}
		if p.Sequence == source && source > 0 {
			if !bytes.Equal(h[:], sh) {
				return reject("conflicting source duplicate")
			}
			apply = false
		} else if int64(p.Sequence) != int64(source)+1 {
			return reject("native source gap/reset")
		} else {
			source = p.Sequence
			sh = h[:]
		}
	}
	if apply {
		if p.Type == feed.New {
			if _, e = tx.Exec(ctx, `INSERT INTO mtbt.orders(stream_id,order_id,token,side,price,accepted_quantity,remaining_quantity,status) VALUES($1,$2,$3,$4,$5,$6::bigint,$7::integer,'PENDING')`, s.m.StreamID, p.OrderID.Bytes(), p.Token, string(p.Side), p.Price, int64(p.Quantity), p.Quantity); e != nil {
				return e
			}
		}
		tokens := []int32{p.Token}
		if p.Type == feed.Heartbeat {
			tokens = nil
			for _, i := range s.m.Instruments {
				tokens = append(tokens, i.Token)
			}
		}
		var rows, total int64
		if e = tx.QueryRow(ctx, "SELECT count(*),COALESCE(sum(octet_length(payload)),0)::bigint FROM mtbt.match_work WHERE stream_id=$1", s.m.StreamID).Scan(&rows, &total); e != nil {
			return e
		}
		if rows+int64(len(tokens)) > MaxRoutingRows {
			return fmt.Errorf("pending routing row budget exhausted")
		}
		for _, token := range tokens {
			var seq int64
			var previous string
			if e = tx.QueryRow(ctx, "SELECT admitted,admitted_digest FROM mtbt.ticker_progress WHERE stream_id=$1 AND token=$2 FOR UPDATE", s.m.StreamID, token).Scan(&seq, &previous); e != nil {
				return e
			}
			c, x := tickerwire.New(s.m, token, uint64(seq+1), position, received, raw, previous)
			if x != nil {
				return x
			}
			payload, x := tickerwire.Encode(c)
			if x != nil {
				return x
			}
			total += int64(len(payload))
			if total > MaxRoutingBytes {
				return fmt.Errorf("pending routing byte budget exhausted")
			}
			if _, e = tx.Exec(ctx, "INSERT INTO mtbt.match_work VALUES($1,$2,$3,$4,$5,$6)", s.m.StreamID, token, seq+1, int64(position), c.Digest, payload); e != nil {
				return e
			}
			if _, e = tx.Exec(ctx, "UPDATE mtbt.ticker_progress SET admitted=$3,admitted_digest=$4 WHERE stream_id=$1 AND token=$2", s.m.StreamID, token, seq+1, c.Digest); e != nil {
				return e
			}
		}
		if _, e = tx.Exec(ctx, "SELECT pg_notify($1,'')", RoutingChannel(s.m.StreamID)); e != nil {
			return e
		}
	}
	state = "UNINITIALIZED"
	if source > 0 {
		state = "VALID"
	}
	if _, e = tx.Exec(ctx, `UPDATE mtbt.feed_state SET source_sequence=$2,source_hash=$3,broker_sequence=$4,broker_hash=$5,status=$6,last_received=GREATEST(last_received,$7) WHERE stream_id=$1`, s.m.StreamID, source, sh, int64(position), h[:], state, received.UTC()); e != nil {
		return e
	}
	if s.beforeCommit != nil {
		if e = s.beforeCommit(tx); e != nil {
			return e
		}
	}
	e = tx.Commit(ctx)
	return e
}
