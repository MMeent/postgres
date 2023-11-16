/* contrib/brin_aops/brin_aops--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION brin_aops" to load this file. \quit

CREATE FUNCTION brin_aops_minmax_opcinfo(oid)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_add_value(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_consistent(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION brin_aops_minmax_union(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT IMMUTABLE;


CREATE OPERATOR CLASS brin_aops_int2_minmax_ops
DEFAULT FOR TYPE int2[] USING brin
AS
    OPERATOR   1    <(int2[], int2[]),
    OPERATOR   2    <=(int2[], int2[]),
    OPERATOR   3    ==(int2[], int2[]),
    OPERATOR   4    >=(int2[], int2[]),
    OPERATOR   5    >(int2[], int2[]),
    OPERATOR   6    <>(int2[], int2[]),
    OPERATOR   7    !=(int2[], int2[]),
    OPERATOR   8    @>(int2[], int2[]),

    FUNCTION   1    brin_aops_minmax_opcinfo(oid),
    FUNCTION   2    brin_aops_minmax_add_value(internal),
    FUNCTION   3    brin_aops_minmax_consistent(internal),
    FUNCTION   4    brin_aops_minmax_union(internal),
STORAGE int2;
