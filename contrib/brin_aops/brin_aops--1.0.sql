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
    OPERATOR   1    <    (anyarray, anyarray),
    OPERATOR   2    <=   (anyarray, anyarray),
    OPERATOR   3    =    (anyarray, anyarray),
    OPERATOR   4    >=   (anyarray, anyarray),
    OPERATOR   5    >    (anyarray, anyarray),
    OPERATOR   6    @>   (anyarray, anyarray),
    OPERATOR   7    <@   (anyarray, anyarray),
    OPERATOR   8    &&   (anyarray, anyarray);

CREATE OPERATOR CLASS int2_minmax_aops
DEFAULT FOR TYPE int2[] USING brin FAMILY integer_minmax_aops
AS
    OPERATOR   1    <    (int2, int2),
    OPERATOR   2    <=   (int2, int2),
    OPERATOR   3    =    (int2, int2),
    OPERATOR   4    >=   (int2, int2),
    OPERATOR   5    >    (int2, int2),
	FUNCTION   1    brin_aops_minmax_opcinfo(internal),
	FUNCTION   2    brin_aops_minmax_add_value(internal),
	FUNCTION   3    brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION   4    brin_aops_minmax_union(internal),
STORAGE int2;

CREATE OPERATOR CLASS int4_minmax_aops
DEFAULT FOR TYPE int4[] USING brin FAMILY integer_minmax_aops
AS
    OPERATOR   1    <    (int4, int4),
    OPERATOR   2    <=   (int4, int4),
    OPERATOR   3    =    (int4, int4),
    OPERATOR   4    >=   (int4, int4),
    OPERATOR   5    >    (int4, int4),
	FUNCTION   1    brin_aops_minmax_opcinfo(internal),
	FUNCTION   2    brin_aops_minmax_add_value(internal),
	FUNCTION   3    brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION   4    brin_aops_minmax_union(internal),
STORAGE int4;

CREATE OPERATOR CLASS int8_minmax_aops
DEFAULT FOR TYPE int8[] USING brin FAMILY integer_minmax_aops
AS
    OPERATOR   1    <    (int8, int8),
    OPERATOR   2    <=   (int8, int8),
    OPERATOR   3    =    (int8, int8),
    OPERATOR   4    >=   (int8, int8),
    OPERATOR   5    >    (int8, int8),
	FUNCTION   1    brin_aops_minmax_opcinfo(internal),
	FUNCTION   2    brin_aops_minmax_add_value(internal),
	FUNCTION   3    brin_aops_minmax_consistent(internal, internal, internal, internal),
	FUNCTION   4    brin_aops_minmax_union(internal),
STORAGE int8;
