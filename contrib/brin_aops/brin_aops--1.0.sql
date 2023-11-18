/* contrib/brin_aops/brin_aops--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION brin_aops" to load this file. \quit


CREATE TYPE brin_aops_minmax_storage;

CREATE FUNCTION brin_aops_minmax_storage_in(cstring)
	RETURNS brin_aops_minmax_storage
AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_storage_out(brin_aops_minmax_storage)
	RETURNS cstring
AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_storage_send(brin_aops_minmax_storage)
	RETURNS bytea
AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_storage_recv(internal)
	RETURNS brin_aops_minmax_storage
AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE brin_aops_minmax_storage
(
	INPUT = brin_aops_minmax_storage_in,
	OUTPUT = brin_aops_minmax_storage_out,
	SEND = brin_aops_minmax_storage_send,
	RECEIVE = brin_aops_minmax_storage_recv,
	ALIGNMENT = int4,
	INTERNALLENGTH = VARIABLE
);

CREATE FUNCTION brin_aops_minmax_opcinfo(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_add_value(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_consistent(internal, internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_union(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE OPERATOR FAMILY integer_minmax_aops USING brin;
ALTER OPERATOR FAMILY integer_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS int2_minmax_aops
DEFAULT FOR TYPE int2[] USING brin FAMILY integer_minmax_aops
AS
	OPERATOR	1	<	(int2, int2),
	OPERATOR	2	<=	(int2, int2),
	OPERATOR	3	=	(int2, int2),
	OPERATOR	4	>=	(int2, int2),
	OPERATOR	5	>	(int2, int2),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE int2;

CREATE OPERATOR CLASS int4_minmax_aops
DEFAULT FOR TYPE int4[] USING brin FAMILY integer_minmax_aops
AS
	OPERATOR	1	<	(int4, int4),
	OPERATOR	2	<=	(int4, int4),
	OPERATOR	3	=	(int4, int4),
	OPERATOR	4	>=	(int4, int4),
	OPERATOR	5	>	(int4, int4),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE int4;

CREATE OPERATOR CLASS int8_minmax_aops
DEFAULT FOR TYPE int8[] USING brin FAMILY integer_minmax_aops
AS
	OPERATOR	1	<	(int8, int8),
	OPERATOR	2	<=	(int8, int8),
	OPERATOR	3	=	(int8, int8),
	OPERATOR	4	>=	(int8, int8),
	OPERATOR	5	>	(int8, int8),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE int8;

CREATE OPERATOR FAMILY numeric_minmax_aops USING brin;
ALTER OPERATOR FAMILY numeric_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS numeric_minmax_aops
DEFAULT FOR TYPE numeric[] USING brin FAMILY numeric_minmax_aops
AS
	OPERATOR	1	<	(numeric, numeric),
	OPERATOR	2	<=	(numeric, numeric),
	OPERATOR	3	=	(numeric, numeric),
	OPERATOR	4	>=	(numeric, numeric),
	OPERATOR	5	>	(numeric, numeric),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE numeric;

CREATE OPERATOR FAMILY text_minmax_aops USING brin;
ALTER OPERATOR FAMILY text_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS text_minmax_aops
DEFAULT FOR TYPE text[] USING brin FAMILY text_minmax_aops
AS
	OPERATOR	1	<	(text, text),
	OPERATOR	2	<=	(text, text),
	OPERATOR	3	=	(text, text),
	OPERATOR	4	>=	(text, text),
	OPERATOR	5	>	(text, text),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE text;

CREATE OPERATOR FAMILY timetz_minmax_aops USING brin;
ALTER OPERATOR FAMILY timetz_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS timetz_minmax_aops
DEFAULT FOR TYPE timetz[] USING brin FAMILY timetz_minmax_aops
AS
	OPERATOR	1	<	(timetz, timetz),
	OPERATOR	2	<=	(timetz, timetz),
	OPERATOR	3	=	(timetz, timetz),
	OPERATOR	4	>=	(timetz, timetz),
	OPERATOR	5	>	(timetz, timetz),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE timetz;

CREATE OPERATOR FAMILY datetime_minmax_aops USING brin;
ALTER OPERATOR FAMILY datetime_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS date_minmax_aops
	DEFAULT FOR TYPE date[] USING brin FAMILY datetime_minmax_aops
	AS
	OPERATOR	1	<	(date, date),
	OPERATOR	2	<=	(date, date),
	OPERATOR	3	=	(date, date),
	OPERATOR	4	>=	(date, date),
	OPERATOR	5	>	(date, date),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE date;

CREATE OPERATOR CLASS timestamp_minmax_aops
DEFAULT FOR TYPE timestamp[] USING brin FAMILY datetime_minmax_aops
AS
	OPERATOR	1	<	(timestamp, timestamp),
	OPERATOR	2	<=	(timestamp, timestamp),
	OPERATOR	3	=	(timestamp, timestamp),
	OPERATOR	4	>=	(timestamp, timestamp),
	OPERATOR	5	>	(timestamp, timestamp),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE timestamp;

CREATE OPERATOR CLASS timestamptz_minmax_aops
DEFAULT FOR TYPE timestamptz[] USING brin FAMILY datetime_minmax_aops
AS
	OPERATOR	1	<	(timestamptz, timestamptz),
	OPERATOR	2	<=	(timestamptz, timestamptz),
	OPERATOR	3	=	(timestamptz, timestamptz),
	OPERATOR	4	>=	(timestamptz, timestamptz),
	OPERATOR	5	>	(timestamptz, timestamptz),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
STORAGE timestamptz;

CREATE OPERATOR FAMILY char_minmax_aops USING brin;
ALTER OPERATOR FAMILY char_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS char_minmax_aops
	DEFAULT FOR TYPE "char"[] USING brin FAMILY char_minmax_aops
	AS
	OPERATOR	1	<	("char", "char"),
	OPERATOR	2	<=	("char", "char"),
	OPERATOR	3	=	("char", "char"),
	OPERATOR	4	>=	("char", "char"),
	OPERATOR	5	>	("char", "char"),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE "char";

CREATE OPERATOR FAMILY bytea_minmax_aops USING brin;
ALTER OPERATOR FAMILY bytea_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS bytea_minmax_aops
	DEFAULT FOR TYPE bytea[] USING brin FAMILY bytea_minmax_aops
	AS
	OPERATOR	1	<	(bytea, bytea),
	OPERATOR	2	<=	(bytea, bytea),
	OPERATOR	3	=	(bytea, bytea),
	OPERATOR	4	>=	(bytea, bytea),
	OPERATOR	5	>	(bytea, bytea),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE bytea;

CREATE OPERATOR FAMILY name_minmax_aops USING brin;
ALTER OPERATOR FAMILY name_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS name_minmax_aops
	DEFAULT FOR TYPE name[] USING brin FAMILY name_minmax_aops
	AS
	OPERATOR	1	<	(name, name),
	OPERATOR	2	<=	(name, name),
	OPERATOR	3	=	(name, name),
	OPERATOR	4	>=	(name, name),
	OPERATOR	5	>	(name, name),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE name;

CREATE OPERATOR FAMILY oid_minmax_aops USING brin;
ALTER OPERATOR FAMILY oid_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS oid_minmax_aops
	DEFAULT FOR TYPE oid[] USING brin FAMILY oid_minmax_aops
	AS
	OPERATOR	1	<	(oid, oid),
	OPERATOR	2	<=	(oid, oid),
	OPERATOR	3	=	(oid, oid),
	OPERATOR	4	>=	(oid, oid),
	OPERATOR	5	>	(oid, oid),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE oid;

CREATE OPERATOR FAMILY tid_minmax_aops USING brin;
ALTER OPERATOR FAMILY tid_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS tid_minmax_aops
	DEFAULT FOR TYPE tid[] USING brin FAMILY tid_minmax_aops
	AS
	OPERATOR	1	<	(tid, tid),
	OPERATOR	2	<=	(tid, tid),
	OPERATOR	3	=	(tid, tid),
	OPERATOR	4	>=	(tid, tid),
	OPERATOR	5	>	(tid, tid),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE tid;

CREATE OPERATOR FAMILY float_minmax_aops USING brin;
ALTER OPERATOR FAMILY float_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS float4_minmax_aops
	DEFAULT FOR TYPE float4[] USING brin FAMILY float_minmax_aops
	AS
	OPERATOR	1	<	(float4, float4),
	OPERATOR	2	<=	(float4, float4),
	OPERATOR	3	=	(float4, float4),
	OPERATOR	4	>=	(float4, float4),
	OPERATOR	5	>	(float4, float4),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE float4;

CREATE OPERATOR CLASS float8_minmax_aops
	DEFAULT FOR TYPE float8[] USING brin FAMILY float_minmax_aops
	AS
	OPERATOR	1	<	(float8, float8),
	OPERATOR	2	<=	(float8, float8),
	OPERATOR	3	=	(float8, float8),
	OPERATOR	4	>=	(float8, float8),
	OPERATOR	5	>	(float8, float8),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE float8;

CREATE OPERATOR FAMILY macaddr_minmax_aops USING brin;
ALTER OPERATOR FAMILY macaddr_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS macaddr_minmax_aops
	DEFAULT FOR TYPE macaddr[] USING brin FAMILY macaddr_minmax_aops
	AS
	OPERATOR	1	<	(macaddr, macaddr),
	OPERATOR	2	<=	(macaddr, macaddr),
	OPERATOR	3	=	(macaddr, macaddr),
	OPERATOR	4	>=	(macaddr, macaddr),
	OPERATOR	5	>	(macaddr, macaddr),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE macaddr;

CREATE OPERATOR FAMILY network_minmax_aops USING brin;
ALTER OPERATOR FAMILY network_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS inet_minmax_aops
	DEFAULT FOR TYPE inet[] USING brin FAMILY network_minmax_aops
	AS
	OPERATOR	1	<	(inet, inet),
	OPERATOR	2	<=	(inet, inet),
	OPERATOR	3	=	(inet, inet),
	OPERATOR	4	>=	(inet, inet),
	OPERATOR	5	>	(inet, inet),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE inet;

CREATE OPERATOR FAMILY bpchar_minmax_aops USING brin;
ALTER OPERATOR FAMILY bpchar_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS bpchar_minmax_aops
	DEFAULT FOR TYPE character[] USING brin FAMILY bpchar_minmax_aops
	AS
	OPERATOR	1	<	(character, character),
	OPERATOR	2	<=	(character, character),
	OPERATOR	3	=	(character, character),
	OPERATOR	4	>=	(character, character),
	OPERATOR	5	>	(character, character),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE character;

CREATE OPERATOR FAMILY time_minmax_aops USING brin;
ALTER OPERATOR FAMILY time_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS time_minmax_aops
	DEFAULT FOR TYPE time[] USING brin FAMILY time_minmax_aops
	AS
	OPERATOR	1	<	(time, time),
	OPERATOR	2	<=	(time, time),
	OPERATOR	3	=	(time, time),
	OPERATOR	4	>=	(time, time),
	OPERATOR	5	>	(time, time),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE time;

CREATE OPERATOR FAMILY interval_minmax_aops USING brin;
ALTER OPERATOR FAMILY interval_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS interval_minmax_aops
	DEFAULT FOR TYPE interval[] USING brin FAMILY interval_minmax_aops
	AS
	OPERATOR	1	<	(interval, interval),
	OPERATOR	2	<=	(interval, interval),
	OPERATOR	3	=	(interval, interval),
	OPERATOR	4	>=	(interval, interval),
	OPERATOR	5	>	(interval, interval),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE interval;

CREATE OPERATOR FAMILY bit_minmax_aops USING brin;
ALTER OPERATOR FAMILY bit_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS bit_minmax_aops
	DEFAULT FOR TYPE bit[] USING brin FAMILY bit_minmax_aops
	AS
	OPERATOR	1	<	(bit, bit),
	OPERATOR	2	<=	(bit, bit),
	OPERATOR	3	=	(bit, bit),
	OPERATOR	4	>=	(bit, bit),
	OPERATOR	5	>	(bit, bit),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE bit;

CREATE OPERATOR FAMILY varbit_minmax_aops USING brin;
ALTER OPERATOR FAMILY varbit_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS varbit_minmax_aops
	DEFAULT FOR TYPE varbit[] USING brin FAMILY varbit_minmax_aops
	AS
	OPERATOR	1	<	(varbit, varbit),
	OPERATOR	2	<=	(varbit, varbit),
	OPERATOR	3	=	(varbit, varbit),
	OPERATOR	4	>=	(varbit, varbit),
	OPERATOR	5	>	(varbit, varbit),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE varbit;

CREATE OPERATOR FAMILY uuid_minmax_aops USING brin;
ALTER OPERATOR FAMILY uuid_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS uuid_minmax_aops
	DEFAULT FOR TYPE uuid[] USING brin FAMILY uuid_minmax_aops
	AS
	OPERATOR	1	<	(uuid, uuid),
	OPERATOR	2	<=	(uuid, uuid),
	OPERATOR	3	=	(uuid, uuid),
	OPERATOR	4	>=	(uuid, uuid),
	OPERATOR	5	>	(uuid, uuid),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE uuid;

CREATE OPERATOR FAMILY pg_lsn_minmax_aops USING brin;
ALTER OPERATOR FAMILY pg_lsn_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS pg_lsn_minmax_aops
	DEFAULT FOR TYPE pg_lsn[] USING brin FAMILY pg_lsn_minmax_aops
	AS
	OPERATOR	1	<	(pg_lsn, pg_lsn),
	OPERATOR	2	<=	(pg_lsn, pg_lsn),
	OPERATOR	3	=	(pg_lsn, pg_lsn),
	OPERATOR	4	>=	(pg_lsn, pg_lsn),
	OPERATOR	5	>	(pg_lsn, pg_lsn),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE pg_lsn;

CREATE OPERATOR FAMILY macaddr8_minmax_aops USING brin;
ALTER OPERATOR FAMILY macaddr8_minmax_aops USING brin ADD
	OPERATOR	1	<	(anyarray, anyarray),
	OPERATOR	2	<=	(anyarray, anyarray),
	OPERATOR	3	=	(anyarray, anyarray),
	OPERATOR	4	>=	(anyarray, anyarray),
	OPERATOR	5	>	(anyarray, anyarray),
	OPERATOR	6	@>	(anyarray, anyarray),
	OPERATOR	7	<@	(anyarray, anyarray),
	OPERATOR	8	&&	(anyarray, anyarray);

CREATE OPERATOR CLASS macaddr8_minmax_aops
	DEFAULT FOR TYPE macaddr8[] USING brin FAMILY macaddr8_minmax_aops
	AS
	OPERATOR	1	<	(macaddr8, macaddr8),
	OPERATOR	2	<=	(macaddr8, macaddr8),
	OPERATOR	3	=	(macaddr8, macaddr8),
	OPERATOR	4	>=	(macaddr8, macaddr8),
	OPERATOR	5	>	(macaddr8, macaddr8),
	FUNCTION	1	brin_aops_minmax_opcinfo(internal),
	FUNCTION	2	brin_aops_minmax_add_value(internal),
	FUNCTION	3	brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION	4	brin_aops_minmax_union(internal),
	STORAGE macaddr8;
