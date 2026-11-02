-- Manifest v2: native stream identity only. Never erase or merge retained legacy data.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema='mtbt' AND column_name='run_id'
  ) THEN
    RAISE EXCEPTION 'Legacy run_id schema detected. Use a fresh database or an explicit operator-planned migration; no data was changed.';
  END IF;
END $$;

CREATE SCHEMA IF NOT EXISTS mtbt;
CREATE TABLE IF NOT EXISTS mtbt.feed_state (
 stream_id smallint NOT NULL CHECK (stream_id > 0),
 manifest_hash text NOT NULL,
 status text NOT NULL DEFAULT 'UNINITIALIZED' CHECK (status IN ('UNINITIALIZED','VALID','INVALID')),
 reason text NOT NULL DEFAULT '',
 source_sequence integer NOT NULL DEFAULT 0 CHECK (source_sequence >= 0),
 source_hash bytea,
 broker_sequence bigint NOT NULL DEFAULT 0 CHECK (broker_sequence >= 0),
 broker_hash bytea,
 last_received timestamptz,
 PRIMARY KEY(stream_id)
);
CREATE TABLE IF NOT EXISTS mtbt.active_orders (
 stream_id smallint NOT NULL,
 order_id bytea NOT NULL CHECK (octet_length(order_id)=8),
 token integer NOT NULL CHECK(token > 0),
 side text NOT NULL CHECK(side IN ('B','S')),
 price integer NOT NULL CHECK(price > 0),
 quantity integer NOT NULL CHECK(quantity > 0),
 PRIMARY KEY(stream_id,order_id),
 FOREIGN KEY(stream_id) REFERENCES mtbt.feed_state(stream_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS active_orders_depth ON mtbt.active_orders(stream_id,token,side,price);
