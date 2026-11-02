-- SQL oracle for load-source's documented N-only multi-instrument pattern.
-- psql variables: count=absolute final sequence, instruments=catalog count.
-- Decode native little-endian DOUBLE ID without materializing rows in client.
WITH bits AS (
 SELECT *, (get_byte(order_id,0)::bigint | (get_byte(order_id,1)::bigint<<8) |
 (get_byte(order_id,2)::bigint<<16) | (get_byte(order_id,3)::bigint<<24) |
 (get_byte(order_id,4)::bigint<<32) | (get_byte(order_id,5)::bigint<<40) |
 (get_byte(order_id,6)::bigint<<48) | (get_byte(order_id,7)::bigint<<56)) AS b
 FROM mtbt.orders WHERE stream_id=1
), decoded AS (
 SELECT *, (1+(b & 4503599627370495)::double precision/4503599627370496.0)
 * power(2.0::double precision, ((b>>52)&2047)-1023) AS native_id FROM bits
), ids AS (
 SELECT *,native_id::bigint AS n FROM decoded
), expected AS (
 SELECT *, ((n-1)%:instruments+1)::integer AS expected_token,
 ((n-1)%(3*:instruments))/:instruments AS phase,
 CASE WHEN (n-1)%(3*:instruments)<:instruments THEN
   CASE WHEN n+2*:instruments<=:count THEN 0 WHEN n+:instruments<=:count THEN 70 ELSE 100 END ELSE 0 END AS expected_remaining
 FROM ids
)
SELECT count(*) AS rows,
 count(*) FILTER (WHERE n<1 OR n>:count OR native_id<>n::double precision OR b<0
 OR token<>expected_token OR price<>10000+5*expected_token
 OR side<>CASE WHEN phase=0 THEN 'B' ELSE 'S' END
 OR accepted_quantity<>CASE WHEN phase=0 THEN 100 WHEN phase=1 THEN 30 ELSE 70 END
 OR remaining_quantity<>expected_remaining
 OR filled_quantity<>accepted_quantity-expected_remaining OR cancelled_quantity<>0
 OR status<>CASE WHEN expected_remaining=100 THEN 'OPEN' WHEN expected_remaining=70 THEN 'PARTIALLY_FILLED' ELSE 'FILLED' END) AS mismatches,
 min(n),max(n),count(*) FILTER(WHERE remaining_quantity>0) AS active
FROM expected;
