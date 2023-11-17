CREATE EXTENSION brin_aops;

CREATE TABLE test (dataset int2[], padding text) with (fillfactor = 10);
select setseed(0);
INSERT INTO test
SELECT array(select distinct (random() * n/5)::int2 + n / 10 + (y % ((n + 16) / 17)) + 1
			 from generate_series(n, n + 100, 1) y
			 order by 1 limit 15
		   )::int2[] AS dataset,
	   repeat('a', 800) AS padding
FROM generate_series(1, 1000) n;

EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset > '{1}'::int2[];
SELECT count(*) FROM test WHERE dataset > '{1}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset && '{1}'::int2[];
SELECT count(*) FROM test WHERE dataset && '{1}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset && '{null}'::int2[];
SELECT count(*) FROM test WHERE dataset && '{null}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset @> '{}'::int2[];
SELECT count(*) FROM test WHERE dataset @> '{}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset <@ '{1,2,3,4,5,6,7,8,9,10}'::int2[];
SELECT count(*) FROM test WHERE dataset <@ '{1,2,3,4,5,6,7,8,9,10}'::int2[];

CREATE INDEX brin_minmax_int2
    ON test
        USING brin (dataset int2_minmax_aops)
		WITH (pages_per_range = 2);

SET enable_seqscan = off;

EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset > '{1}'::int2[];
SELECT count(*) FROM test WHERE dataset > '{1}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset && '{1}'::int2[];
SELECT count(*) FROM test WHERE dataset && '{1}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset && '{null}'::int2[];
SELECT count(*) FROM test WHERE dataset && '{null}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset @> '{}'::int2[];
SELECT count(*) FROM test WHERE dataset @> '{}'::int2[];
EXPLAIN (costs off, analyze, verbose off, buffers, timing off)
	SELECT count(*) FROM test WHERE dataset <@ '{1,2,3,4,5,6,7,8,9,10}'::int2[];
SELECT count(*) FROM test WHERE dataset <@ '{1,2,3,4,5,6,7,8,9,10}'::int2[];
