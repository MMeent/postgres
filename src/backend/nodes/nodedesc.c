/*-------------------------------------------------------------------------
 *
 * nodedesc.c
 *		Various definitions and helpers for NodeDesc infrastructure
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/nodedesc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/amapi.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/tsmapi.h"
#include "commands/event_trigger.h"
#include "commands/trigger.h"
#include "executor/tuptable.h"
#include "foreign/fdwapi.h"
#include "nodes/bitmapset.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/lockoptions.h"
#include "nodes/miscnodes.h"
#include "nodes/ndio.h"
#include "nodes/nodes.h"
#include "nodes/nodedesc.h"
#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/replnodes.h"
#include "nodes/supportnodes.h"
#include "nodes/value.h"
#include "utils/rel.h"


#define MakeNodeDesc(_type_, _fields_, _fields_offset_, \
					 _flags_, _field_flags_, _custom_offset_) \
	[T_##_type_] = (NodeDescData) { \
		.nd_name = #_type_, \
		.nd_nodetag = T_##_type_, \
		.nd_size = sizeof(_type_), \
		.nd_namelen = sizeof(#_type_) - 1, \
		.nd_fields = (_fields_), \
		.nd_fields_offset = (_fields_offset_), \
		.nd_flags = (_flags_), \
		.nd_fld_flags = (_field_flags_), \
		.nd_custom_off = (_custom_offset_), \
	},

#define MakeNodeFieldDesc(_node_type_, _fldname_, _fldtype_, _fldnum_, \
						  _flags_, _custom_off_, _arr_len_off_) \
	{ \
		.nfd_name = #_fldname_, \
		.nfd_node = T_##_node_type_, \
		.nfd_type = (_fldtype_), \
		.nfd_namelen = sizeof(#_fldname_) - 1, \
		.nfd_field_no = (_fldnum_), \
		.nfd_offset = offsetof(_node_type_, _fldname_), \
		.nfd_flags = (_flags_), \
		.nfd_arr_len_off = (int16) ((ssize_t) (_arr_len_off_)), \
		.nfd_custom_off = (_custom_off_), \
	},

const NodeDescData NodeDescriptors[] = {
#include "nodedesc.nodes.c"
	{0},
};
#undef MakeNodeDesc

const NodeFieldDescData NodeFieldDescriptors[] = {
#include "nodedesc.fields.c"
	{0},
};
#undef MakeNodeFieldDesc

NodeDesc NodeDescriptorsByName;
static int num_sorted_nodedescriptors;

struct NameKey {
	const char * name;
	int			namelen;
};

static void
ResetOrderedNodeDescriptors(void *arg)
{
	NodeDescriptorsByName = NULL;
}

static int
OrderNodeName(const char *a, int a_len, const void *b, int b_len)
{
	int ret = memcmp(a, b, Min(a_len, b_len));

	if (ret != 0)
		return ret;

	return a_len - b_len;
}

static int
OrderNodeDescByName(const void *a, const void *b)
{
	NodeDesc	left = (NodeDesc) a;
	NodeDesc	right = (NodeDesc) b;

	return OrderNodeName(left->nd_name, left->nd_namelen,
						 right->nd_name, right->nd_namelen);
}

static int
CompareNodeDescWithKey(const void *k, const void *d)
{
	const struct NameKey *key = (const struct NameKey *) k;
	NodeDesc	desc = (NodeDesc) d;

	return OrderNodeName(key->name, key->namelen,
						 desc->nd_name, desc->nd_namelen);
};

NodeDesc
GetNodeDescByNodeName(const char *name, int len)
{
	struct NameKey	key = {
		.name = name,
		.namelen = len,
	};

	UseOrderedNodeDescs();

	return bsearch(&key, NodeDescriptorsByName, num_sorted_nodedescriptors,
				   sizeof(NodeDescData), CompareNodeDescWithKey);
}

void
InitializeOrderedNodeDescriptors(void)
{
	MemoryContext prev;
	int			num_entries = sizeof(NodeDescriptors) / sizeof(NodeDescData);
	NodeDescData *sorted;
	int			i, j;
	static MemoryContextCallback cb = {
		.arg = NULL,
		.func = ResetOrderedNodeDescriptors,
		.next = NULL,
	};

	Assert(NodeDescriptorsByName == NULL);

	prev = MemoryContextSwitchTo(CacheMemoryContext);
	MemoryContextRegisterResetCallback(CurrentMemoryContext, &cb);
	sorted = palloc(sizeof(NodeDescriptors));

	for (i = 0, j = 0; i < num_entries; i++)
	{
		if (NodeDescriptors[i].nd_nodetag != T_Invalid)
			sorted[j++] = NodeDescriptors[i];
	}

	qsort(sorted, j, sizeof(NodeDescData), OrderNodeDescByName);
	num_sorted_nodedescriptors = j;

	MemoryContextSwitchTo(prev);

	NodeDescriptorsByName = (NodeDesc) sorted;
}

#define CustomNodeRead(_type_) \
extern Node *ReadNode##_type_(StringInfo from, NodeReader reader, uint32 flags);

#define CustomNodeWrite(_type_) \
extern bool WriteNode##_type_(StringInfo into, const Node *node, \
							  NodeWriter writer, uint32 flags);

#include "nodedesc.overrides.c"

#undef CustomNodeRead
#undef CustomNodeWrite

#define CustomNodeRead(_type_) \
	{ .cndf_read_node = ReadNode##_type_ },

#define CustomNodeWrite(_type_) \
	{ .cndf_write_node = WriteNode##_type_ },

const CustomNodeDescFunc CustomNodeDescFunctions[] = {
#include "nodedesc.overrides.c"
	{0},
};

#undef CustomNodeRead
#undef CustomNodeWrite
