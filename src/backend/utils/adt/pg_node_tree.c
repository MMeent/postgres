/*-------------------------------------------------------------------------
 *
 * pg_node_tree.c
 *	  Operations for the pg_node_tree pseudotype.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/pg_node_tree.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq/pqformat.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/builtins.h"
#include "nodes/nodeFuncs.h"
#include "nodes/ndio.h"

Datum
pg_node_tree_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_node_tree")));
	PG_RETURN_VOID();
}

Datum
pg_node_tree_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "pg_node_tree")));
	PG_RETURN_VOID();
}

Datum
pg_node_tree_out(PG_FUNCTION_ARGS)
{
	MemoryContext	tmp;
	MemoryContext	prev;
	Node		   *tmpNode;
	Datum			arg = PG_GETARG_DATUM(0);
	NodeTree		tree;
	StringInfoData	data;

	tmp = AllocSetContextCreate(fcinfo->flinfo->fn_mcxt, "tmp", ALLOCSET_DEFAULT_SIZES);
	prev = MemoryContextSwitchTo(tmp);

	tree = DatumGetNodeTree(arg);
	tmpNode = (Node *) nodeTreeToNode(tree);

	MemoryContextSwitchTo(prev);

	initStringInfo(&data);

	WriteNode(&data, tmpNode, TextNodeWriter, ND_WRITE_NO_SKIP_DEFAULTS);

	MemoryContextDelete(tmp);

	PG_RETURN_CSTRING(data.data);
}

static Datum
pg_node_tree_send_ext(PG_FUNCTION_ARGS)
{
	MemoryContext	tmp;
	MemoryContext	prev;
	Node		   *tmpNode;
	Datum			arg = PG_GETARG_DATUM(0);
	NodeTree		tree;
	StringInfoData	data;

	if (!fcinfo->flinfo->fn_extra)
	{
		fcinfo->flinfo->fn_extra
			= AllocSetContextCreate(fcinfo->flinfo->fn_mcxt,
									"pg_node_tree_send_ext",
									ALLOCSET_DEFAULT_SIZES);
	}
	tmp = fcinfo->flinfo->fn_extra;
	prev = MemoryContextSwitchTo(tmp);

	tree = DatumGetNodeTree(arg);
	tmpNode = (Node *) nodeTreeToNode(tree);

	MemoryContextSwitchTo(prev);

	pq_begintypsend(&data);

	WriteNode(&data, tmpNode, TextNodeWriter, ND_WRITE_NO_SKIP_DEFAULTS);

	MemoryContextDelete(tmp);

	PG_RETURN_BYTEA_P(pq_endtypsend(&data));
}

Datum
pg_node_tree_to_text(PG_FUNCTION_ARGS)
{
	return pg_node_tree_send_ext(fcinfo);
}

Datum
pg_node_tree_send(PG_FUNCTION_ARGS)
{
	return pg_node_tree_send_ext(fcinfo);
}

Datum
pg_node_tree_to_json(PG_FUNCTION_ARGS)
{
	MemoryContext	tmp;
	MemoryContext	prev;
	Node		   *tmpNode;
	Datum			arg = PG_GETARG_DATUM(0);
	NodeTree		tree;
	StringInfoData	data;
	
	if (!fcinfo->flinfo->fn_extra)
	{
		fcinfo->flinfo->fn_extra
			= AllocSetContextCreate(fcinfo->flinfo->fn_mcxt,
									"pg_node_tree_to_json",
									ALLOCSET_DEFAULT_SIZES);
	}
	tmp = fcinfo->flinfo->fn_extra;
	prev = MemoryContextSwitchTo(tmp);

	tree = DatumGetNodeTree(arg);
	tmpNode = (Node *) nodeTreeToNode(tree);

	MemoryContextSwitchTo(prev);

	pq_begintypsend(&data);
	WriteNode(&data, tmpNode, JSONNodeWriter, ND_WRITE_NO_SKIP_DEFAULTS);
	MemoryContextReset(tmp);

	PG_RETURN_BYTEA_P(pq_endtypsend(&data));
}
