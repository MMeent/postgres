/* contrib/brin_aops/brin_aops--1.0-1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION brin_aops" to load this file. \quit

CREATE FUNCTION brin_aops_bloom_opcinfo(oid)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_bloom_add_value(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_bloom_consistent(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_bloom_union(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_bloom_options(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS int2_bloom_aops
DEFAULT FOR TYPE int2[] USING brin
AS
    OPERATOR   1    ==(int2[], int2[]),
    OPERATOR   2    @>(int2[], int2[]),
    OPERATOR   3    <@(int2[], int2[]),
    OPERATOR   4    &&(int2[], int2[]),

    FUNCTION   1    brin_aops_bloom_opcinfo(oid),
    FUNCTION   2    brin_aops_bloom_add_value(internal),
    FUNCTION   3    brin_aops_bloom_consistent(internal),
    FUNCTION   4    brin_aops_bloom_union(internal),
    FUNCTION   5    brin_aops_bloom_options(internal),
    FUNCTION   11   hashint2(int2),
STORAGE int2;

create type brin_aops_bloom_storage;

CREATE FUNCTION brin_aops_bloom_storage_in(cstring)
RETURNS brin_aops_bloom_storage
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_bloom_storage_out(brin_aops_bloom_storage)
RETURNS cstring
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_bloom_storage_send(brin_aops_bloom_storage)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_bloom_storage_receive(internal)
RETURNS brin_aops_bloom_storage
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE TYPE brin_aops_bloom_storage
(
    INPUT = brin_aops_bloom_storage_in,
    OUTPUT = brin_aops_bloom_storage_out,
    SEND = brin_aops_bloom_storage_send,
    RECEIVE = brin_aops_bloom_storage_receive,
    ALIGNMENT = 4,
    INTERNALLENGTH = VARIABLE
);
