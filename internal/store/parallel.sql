-- Parallel extension for Manifest v2 (StreamID-based)
CREATE TABLE IF NOT EXISTS mtbt.subscriber_topology (
 stream_id smallint NOT NULL,
 shards integer NOT NULL CHECK(shards BETWEEN 1 AND 16),
 routing text NOT NULL,
 PRIMARY KEY(stream_id),
 FOREIGN KEY(stream_id) REFERENCES mtbt.feed_state(stream_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS mtbt.subscriber_shards (
 stream_id smallint NOT NULL,
 shard integer NOT NULL,
 dispatched bigint NOT NULL DEFAULT 0,
 applied bigint NOT NULL DEFAULT 0,
 applied_source integer NOT NULL DEFAULT 0,
 applied_source_broker bigint NOT NULL DEFAULT 0,
 applied_hash bytea,
 PRIMARY KEY(stream_id,shard),
 CHECK(shard BETWEEN 0 AND 15 AND applied >= 0 AND dispatched >= applied),
 FOREIGN KEY(stream_id) REFERENCES mtbt.subscriber_topology(stream_id) ON DELETE CASCADE
);
-- A single ordered dispatcher drains this outbox before accepting another raw message.
-- Delete only after confirmed JetStream publication. Stable shard sequence handles publish/commit ambiguity.
CREATE TABLE IF NOT EXISTS mtbt.subscriber_outbox (
 stream_id smallint NOT NULL,
 shard integer NOT NULL,
 sequence bigint NOT NULL CHECK(sequence > 0),
 source_broker bigint NOT NULL CHECK(source_broker > 0),
 raw bytea NOT NULL,
 received timestamptz NOT NULL,
 PRIMARY KEY(stream_id,shard,sequence),
 FOREIGN KEY(stream_id,shard) REFERENCES mtbt.subscriber_shards(stream_id,shard) ON DELETE CASCADE
);
-- Ownership tombstones prevent cross-instrument identity reuse/races after sharding.
CREATE TABLE IF NOT EXISTS mtbt.subscriber_identities (
 stream_id smallint NOT NULL,
 order_id bytea NOT NULL CHECK(octet_length(order_id)=8),
 token integer NOT NULL CHECK(token > 0),
 PRIMARY KEY(stream_id,order_id),
 FOREIGN KEY(stream_id) REFERENCES mtbt.subscriber_topology(stream_id) ON DELETE CASCADE
);
