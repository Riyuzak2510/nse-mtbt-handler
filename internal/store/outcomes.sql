-- Optional outcomes are disabled for all pre-existing streams. Explicit fresh-only
-- enablement belongs to the matching lease; migration alone enables nothing.
ALTER TABLE mtbt.feed_state ADD COLUMN IF NOT EXISTS outcomes_start_sequence integer CHECK(outcomes_start_sequence>0);
ALTER TABLE mtbt.feed_state ADD COLUMN IF NOT EXISTS outcomes_max_rows bigint CHECK(outcomes_max_rows>0);
ALTER TABLE mtbt.feed_state ADD COLUMN IF NOT EXISTS outcomes_max_bytes bigint CHECK(outcomes_max_bytes>0);
CREATE TABLE IF NOT EXISTS mtbt.order_outbox (
 stream_id smallint NOT NULL REFERENCES mtbt.feed_state(stream_id) ON DELETE RESTRICT,
 source_sequence integer NOT NULL CHECK(source_sequence>0),
 order_id bytea NOT NULL CHECK(octet_length(order_id)=8),
 payload bytea NOT NULL CHECK(octet_length(payload) BETWEEN 1 AND 8192),
 recorded_at timestamptz NOT NULL,
 PRIMARY KEY(stream_id,source_sequence,order_id)
);
-- An old matcher does not know the enable flag. Refuse its writes using a local
-- transaction capability marker, set only by the outcome-aware transaction path.
-- Not a security barrier against arbitrary privileged SQL or migration admins.
CREATE OR REPLACE FUNCTION mtbt.require_outcome_writer() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE sid smallint;
BEGIN
 IF TG_OP='DELETE' THEN sid:=OLD.stream_id; ELSE sid:=NEW.stream_id; END IF;
 IF EXISTS(SELECT 1 FROM mtbt.feed_state WHERE stream_id=sid AND outcomes_start_sequence IS NOT NULL)
 AND current_setting('mtbt.outcome_writer',true) IS DISTINCT FROM 'v1' THEN
   RAISE EXCEPTION 'outcome-enabled stream requires outcome-aware writer';
 END IF;
 IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
END $$;
DROP TRIGGER IF EXISTS require_outcome_order_writer ON mtbt.orders;
CREATE TRIGGER require_outcome_order_writer BEFORE INSERT OR UPDATE OR DELETE ON mtbt.orders
 FOR EACH ROW EXECUTE FUNCTION mtbt.require_outcome_writer();
DROP TRIGGER IF EXISTS require_outcome_checkpoint_writer ON mtbt.feed_state;
CREATE TRIGGER require_outcome_checkpoint_writer BEFORE UPDATE OR DELETE ON mtbt.feed_state
 FOR EACH ROW EXECUTE FUNCTION mtbt.require_outcome_writer();
