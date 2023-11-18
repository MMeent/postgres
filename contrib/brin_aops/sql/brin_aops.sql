CREATE TABLE tenk1 (
	unique1		int4,
	unique2		int4,
	two			int4,
	four		int4,
	ten			int4,
	twenty		int4,
	hundred		int4,
	thousand	int4,
	twothousand	int4,
	fivethous	int4,
	tenthous	int4,
	odd			int4,
	even		int4,
	stringu1	name,
	stringu2	name,
	string4		name
);
\getenv abs_srcdir PG_ABS_SRCDIR
\set filename :abs_srcdir '/../../src/test/regress/data/tenk.data'
COPY tenk1 FROM :'filename';
VACUUM ANALYZE tenk1;

CREATE EXTENSION brin_aops;

CREATE TABLE brintest (byteacol bytea[],
	charcol "char"[],
	namecol name[],
	int8col bigint[],
	int2col smallint[],
	int4col integer[],
	textcol text[],
	oidcol oid[],
	tidcol tid[],
	float4col real[],
	float8col double precision[],
	macaddrcol macaddr[],
	inetcol inet[],
	bpcharcol character[],
	datecol date[],
	timecol time without time zone[],
	timestampcol timestamp without time zone[],
	timestamptzcol timestamp with time zone[],
	intervalcol interval[],
	timetzcol time with time zone[],
	bitcol bit(10)[],
	varbitcol bit varying(16)[],
	numericcol numeric[],
	uuidcol uuid[],
	lsncol pg_lsn[]
) WITH (fillfactor=10, autovacuum_enabled=off);

INSERT INTO brintest SELECT
	ARRAY[repeat(stringu1, 8)::bytea],
	ARRAY[substr(stringu1, 1, 1)::"char"],
	ARRAY[stringu1::name],
	ARRAY[142857 * tenthous],
	ARRAY[thousand],
	ARRAY[twothousand],
	ARRAY[repeat(stringu1, 8)],
	ARRAY[unique1::oid],
	ARRAY[format('(%s,%s)', tenthous, twenty)::tid],
	ARRAY[(four + 1.0)/(hundred+1)],
	ARRAY[odd::float8 / (tenthous + 1)],
	ARRAY[format('%s:00:%s:00:%s:00', to_hex(odd), to_hex(even), to_hex(hundred))::macaddr],
	ARRAY[inet '10.2.3.4/24' + tenthous],
	ARRAY[substr(stringu1, 1, 1)::bpchar],
	ARRAY[date '1995-08-15' + tenthous],
	ARRAY[time '01:20:30' + thousand * interval '18.5 second'],
	ARRAY[timestamp '1942-07-23 03:05:09' + tenthous * interval '36.38 hours'],
	ARRAY[timestamptz '1972-10-10 03:00' + thousand * interval '1 hour'],
	ARRAY[justify_days(justify_hours(tenthous * interval '12 minutes'))],
	ARRAY[timetz '01:30:20+02' + hundred * interval '15 seconds'],
	ARRAY[thousand::bit(10)],
	ARRAY[tenthous::bit(16)::varbit],
	ARRAY[tenthous::numeric(36,30) * fivethous * even / (hundred + 1)],
	ARRAY[format('%s%s-%s-%s-%s-%s%s%s', to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'), to_char(tenthous, 'FM0000'))::uuid],
	ARRAY[format('%s/%s%s', odd, even, tenthous)::pg_lsn]
FROM tenk1 ORDER BY unique2 LIMIT 100;

-- throw in some NULL's and different values
INSERT INTO brintest (inetcol) SELECT
	ARRAY[inet 'fe80::6e40:8ff:fea9:8c46' + tenthous]
FROM tenk1 ORDER BY thousand, tenthous LIMIT 25;

CREATE INDEX brinidx ON brintest USING brin (
	byteacol,
	charcol,
	namecol,
	int8col,
	int2col,
	int4col,
	textcol,
	oidcol,
	tidcol,
	float4col,
	float8col,
	macaddrcol,
	inetcol,
	bpcharcol,
	datecol,
	timecol,
	timestampcol,
	timestamptzcol,
	intervalcol,
	timetzcol,
	bitcol,
	varbitcol,
	numericcol,
	uuidcol,
	lsncol
) with (pages_per_range = 1);

