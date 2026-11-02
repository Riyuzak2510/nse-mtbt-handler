-- Optional matching mode owns the SAME native-stream namespace exclusively.
-- Existing v2 reconstruction streams remain reconstruction mode; no data is converted.
ALTER TABLE mtbt.feed_state ADD COLUMN IF NOT EXISTS owner_mode text NOT NULL DEFAULT 'reconstruction'
 CHECK(owner_mode IN ('reconstruction','matching'));
CREATE TABLE IF NOT EXISTS mtbt.orders (
 stream_id smallint NOT NULL REFERENCES mtbt.feed_state(stream_id) ON DELETE CASCADE,
 order_id bytea NOT NULL CHECK(octet_length(order_id)=8),
 token integer NOT NULL CHECK(token>0),
 side text NOT NULL CHECK(side IN ('B','S')),
 price integer NOT NULL CHECK(price>0),
 accepted_quantity bigint NOT NULL CHECK(accepted_quantity>0),
 remaining_quantity integer NOT NULL CHECK(remaining_quantity>=0),
 filled_quantity bigint NOT NULL DEFAULT 0 CHECK(filled_quantity>=0),
 cancelled_quantity bigint NOT NULL DEFAULT 0 CHECK(cancelled_quantity>=0),
 status text NOT NULL CHECK(status IN ('PENDING','OPEN','PARTIALLY_FILLED','FILLED','CANCELLED')),
 PRIMARY KEY(stream_id,order_id),
 CHECK(accepted_quantity=remaining_quantity::bigint+filled_quantity+cancelled_quantity),
 CHECK((status='PENDING' AND remaining_quantity>0 AND filled_quantity=0 AND cancelled_quantity=0)
    OR (status='OPEN' AND remaining_quantity>0 AND filled_quantity=0)
    OR (status='PARTIALLY_FILLED' AND remaining_quantity>0 AND filled_quantity>0)
    OR (status='FILLED' AND remaining_quantity=0 AND filled_quantity>0)
    OR (status='CANCELLED' AND remaining_quantity=0))
);
CREATE INDEX IF NOT EXISTS orders_active_depth
 ON mtbt.orders(stream_id,token,side,price) WHERE remaining_quantity>0 AND status IN ('OPEN','PARTIALLY_FILLED');
