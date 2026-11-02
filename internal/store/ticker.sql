-- Explicit fresh-history opt-in only. No automatic conversion of existing rows.
CREATE TABLE IF NOT EXISTS mtbt.ticker_topology (
 stream_id smallint PRIMARY KEY REFERENCES mtbt.feed_state(stream_id),
 protocol text NOT NULL CHECK(protocol='ticker-v1'),
 max_pending_rows bigint NOT NULL CHECK(max_pending_rows BETWEEN 1 AND 100000),
 max_pending_bytes bigint NOT NULL CHECK(max_pending_bytes BETWEEN 16384 AND 1073741824)
);
CREATE TABLE IF NOT EXISTS mtbt.ticker_progress (
 stream_id smallint NOT NULL REFERENCES mtbt.ticker_topology(stream_id),
 token integer NOT NULL CHECK(token>0),
 admitted bigint NOT NULL DEFAULT 0 CHECK(admitted>=0),
 admitted_digest text NOT NULL DEFAULT '',
 published bigint NOT NULL DEFAULT 0 CHECK(published>=0),
 PRIMARY KEY(stream_id,token),
 CHECK(published<=admitted)
);
-- Matching locks its own applied row, never the intake/dispatch progress row.
CREATE TABLE IF NOT EXISTS mtbt.ticker_applied (
 stream_id smallint NOT NULL,
 token integer NOT NULL,
 applied bigint NOT NULL DEFAULT 0 CHECK(applied>=0),
 applied_digest text NOT NULL DEFAULT '',
 source_sequence integer NOT NULL DEFAULT 0,
 source_broker bigint NOT NULL DEFAULT 0,
 last_received timestamptz,
 PRIMARY KEY(stream_id,token),
 FOREIGN KEY(stream_id,token) REFERENCES mtbt.ticker_progress(stream_id,token)
);
CREATE TABLE IF NOT EXISTS mtbt.match_work (
 stream_id smallint NOT NULL,
 token integer NOT NULL,
 sequence bigint NOT NULL CHECK(sequence>0),
 source_broker bigint NOT NULL CHECK(source_broker>0),
 digest text NOT NULL CHECK(length(digest)=64),
 payload bytea NOT NULL CHECK(octet_length(payload) BETWEEN 1 AND 16384),
 PRIMARY KEY(stream_id,token,sequence),
 FOREIGN KEY(stream_id,token) REFERENCES mtbt.ticker_progress(stream_id,token)
);
CREATE INDEX IF NOT EXISTS match_work_dispatch ON mtbt.match_work(stream_id,source_broker,token);
-- Compatibility fence, not a privileged-SQL security boundary. Old combined
-- binaries may not write split-owned rows or checkpoint after opt-in.
CREATE OR REPLACE FUNCTION mtbt.require_ticker_writer() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE sid smallint; role text;
BEGIN
 IF TG_OP='DELETE' THEN sid:=OLD.stream_id; ELSE sid:=NEW.stream_id; END IF;
 IF EXISTS(SELECT 1 FROM mtbt.ticker_topology WHERE stream_id=sid) THEN
  role:=current_setting('mtbt.ticker_writer',true);
  IF TG_TABLE_NAME='feed_state' THEN
   IF role IS DISTINCT FROM 'intake' THEN RAISE EXCEPTION 'ticker stream requires intake checkpoint writer'; END IF;
  ELSE
   IF TG_OP='INSERT' THEN
    IF role IS DISTINCT FROM 'intake' OR NEW.status<>'PENDING' THEN RAISE EXCEPTION 'ticker order requires pending admission'; END IF;
   ELSIF TG_OP='UPDATE' THEN
    IF role IS DISTINCT FROM 'matcher' OR current_setting('mtbt.ticker_token',true) IS DISTINCT FROM NEW.token::text
      OR OLD.token<>NEW.token OR OLD.order_id<>NEW.order_id OR OLD.stream_id<>NEW.stream_id THEN
     RAISE EXCEPTION 'ticker order update requires owning matcher';
    END IF;
   ELSE RAISE EXCEPTION 'ticker order deletion forbidden'; END IF;
  END IF;
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
DROP TRIGGER IF EXISTS require_ticker_order_writer ON mtbt.orders;
CREATE TRIGGER require_ticker_order_writer BEFORE INSERT OR UPDATE OR DELETE ON mtbt.orders
 FOR EACH ROW EXECUTE FUNCTION mtbt.require_ticker_writer();
DROP TRIGGER IF EXISTS require_ticker_checkpoint_writer ON mtbt.feed_state;
CREATE TRIGGER require_ticker_checkpoint_writer BEFORE UPDATE OR DELETE ON mtbt.feed_state
 FOR EACH ROW EXECUTE FUNCTION mtbt.require_ticker_writer();
