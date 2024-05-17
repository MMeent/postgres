/*-------------------------------------------------------------------------
 *
 * ndio.c
 *		Various definitions and for NodeDesc-based IO infrastructure
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/ndio.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/nodedesc.h"
#include "port/pg_bitutils.h"

Node *
ReadNode(StringInfo from, NodeReader reader, uint32 flags)
{
	NodeTag		tag;
	NodeDesc	desc;
	char	   *ptr;
	Node	   *node;

	check_stack_depth();

	if (!reader->nr_read_tag(from, flags, &tag))
		return NULL;

	desc = GetNodeDesc(tag);

	if (desc->nd_flags & NODEDESC_DISABLE_READ)
		elog(ERROR, "Can't read node type %s", desc->nd_name);

	if (desc->nd_flags & NODEDESC_CUSTOM_READ)
	{
		ReadNodeFunc	nreader;
		int	off = desc->nd_custom_off;
		off += pg_number_of_ones[desc->nd_flags & (NODEDESC_CUSTOM_READ - 1)];

		nreader = CustomNodeDescFunctions[off].cndf_read_node;
		return nreader(from, reader, flags);
	}

	Assert(!(desc->nd_fld_flags & NODEDESC_CUSTOM_READ));

	node = palloc(desc->nd_size);
	ptr = (char *) node;

	if (desc->nd_fld_flags & NODEDESC_DISABLE_READ)
	{
		/* branchy handling here */
		for (int i = 0; i < desc->nd_fields; i++)
		{
			NodeFieldDesc fdesc = GetNodeFieldDesc(desc, i);
			ReadTypedField freader;

			if (fdesc->nfd_flags & NODEDESC_DISABLE_READ)
				continue;

			freader = reader->nr_fld_readers[fdesc->nfd_type];
			freader(from, fdesc, ptr + fdesc->nfd_offset, flags);
		}
	}
	else
	{
		/* non-branchy handling here */
		for (int i = 0; i < desc->nd_fields; i++)
		{
			NodeFieldDesc fdesc = GetNodeFieldDesc(desc, i);
			ReadTypedField freader;

			freader = reader->nr_fld_readers[fdesc->nfd_type];
			freader(from, fdesc, ptr + fdesc->nfd_offset, flags);
		}
	}

	reader->nr_finish_node(from, desc, flags);

	return node;
}

bool
WriteNode(StringInfo into, const Node *node, NodeWriter writer, uint32 flags)
{
	NodeTag		tag = nodeTag(node);
	NodeDesc	desc = GetNodeDesc(tag);
	char	   *ptr = (char *) node;
	int			last_written_field = 0;

	check_stack_depth();

	if (unlikely(desc->nd_flags & NODEDESC_DISABLE_WRITE))
		elog(ERROR, "Can't write node type %s", desc->nd_name);

	if (unlikely(desc->nd_flags & NODEDESC_CUSTOM_WRITE))
	{
		WriteNodeFunc	nwriter;
		int	off = desc->nd_custom_off;
		off += pg_number_of_ones[desc->nd_flags & (NODEDESC_CUSTOM_READ - 1)];

		nwriter = CustomNodeDescFunctions[off].cndf_write_node;
		return nwriter(into, node, writer, flags);
	}

	Assert(!(desc->nd_fld_flags & NODEDESC_CUSTOM_WRITE));

	writer->nw_start_node(into, desc, flags);

	if (desc->nd_fld_flags & (NODEDESC_DISABLE_WRITE))
	{
		for (int i = 0; i < desc->nd_fields; i++)
		{
			NodeFieldDesc fdesc = GetNodeFieldDesc(desc, i);
			WriteTypedField fwriter;

			if (fdesc->nfd_flags & NODEDESC_DISABLE_WRITE)
				continue;

			fwriter = writer->nw_fld_writers[fdesc->nfd_type];

			if (fwriter(into, fdesc, ptr + fdesc->nfd_offset, flags))
				last_written_field = i;
		}
	}
	else
	{
		for (int i = 0; i < desc->nd_fields; i++)
		{
			NodeFieldDesc fdesc = GetNodeFieldDesc(desc, i);
			WriteTypedField fwriter = writer->nw_fld_writers[fdesc->nfd_type];

			if (fwriter(into, fdesc, ptr + fdesc->nfd_offset, flags))
				last_written_field = i;
		}
	}

	return writer->nw_finish_node(into, desc, last_written_field, flags);
}

#define CustomNodeRead(_type_) \
extern Node *ReadNode##_type_(StringInfo from, NodeReader reader, uint32 flags);
#define CustomNodeWrite(_type_) \
extern bool WriteNode##_type_(StringInfo into, const Node *node, \
							  NodeWriter writer, uint32 flags);
#include "nodedesc.overrides.c"
#undef CustomNodeRead
#undef CustomNodeWrite

typedef struct BitmapsetTool {
	int			nwords;
	const bitmapword  *words;
} BitmapsetTool;

static const NodeFieldDescData bitmapIoTooling[] = {
	{
		.nfd_name = "nwords",
		.nfd_node = T_Bitmapset,
		.nfd_type = NFT_INT,
		.nfd_namelen = sizeof("nwords"),
		.nfd_field_no = 0,
		.nfd_offset = offsetof(BitmapsetTool, nwords),
		.nfd_flags = 0,
		.nfd_arr_len_off = 0,
		.nfd_custom_off = 0,
	},
	{
		.nfd_name = "words",
		.nfd_node = T_Bitmapset,
		.nfd_type = NFT_UINT64 + NFT_ARRAYTYPE,
		.nfd_namelen = sizeof("words"),
		.nfd_field_no = 0,
		.nfd_offset = offsetof(BitmapsetTool, words),
		.nfd_flags = 0,
		.nfd_arr_len_off = (ssize_t) (offsetof(BitmapsetTool, nwords) -
									  offsetof(BitmapsetTool, words)),
		.nfd_custom_off = 0,
	},
};

bool
WriteNodeBitmapset(StringInfo into, const Node *node, NodeWriter writer,
				   uint32 flags)
{
	const Bitmapset *bitmapset = castNode(Bitmapset, node);
	BitmapsetTool	tool = {
		.nwords = bitmapset->nwords,
		.words = bitmapset->words,
	};
	WriteTypedField	fieldWriter;
	int				last_field;

	NodeDesc	desc = GetNodeDesc(T_Bitmapset);

	writer->nw_start_node(into, desc, flags);

	fieldWriter = writer->nw_fld_writers[bitmapIoTooling[0].nfd_type];
	if (fieldWriter(into, &bitmapIoTooling[0], &tool.nwords, flags))
		last_field = 0;

	fieldWriter = writer->nw_fld_writers[bitmapIoTooling[1].nfd_type];
	if (fieldWriter(into, &bitmapIoTooling[1], &tool.words, flags))
		last_field = 1;

	return writer->nw_finish_node(into, desc, last_field, flags);
}

Node *
ReadNodeBitmapset(StringInfo from, NodeReader reader, uint32 flags)
{
	Bitmapset *bitmapset;
	BitmapsetTool	tool = {
		.nwords = 0,
		.words = NULL,
	};
	ReadTypedField	fieldReader;

	NodeDesc	desc = GetNodeDesc(T_Bitmapset);

	fieldReader = reader->nr_fld_readers[bitmapIoTooling[0].nfd_type];
	fieldReader(from, &bitmapIoTooling[0], &tool.nwords, flags);

	bitmapset = palloc(sizeof(Bitmapset) + tool.nwords * sizeof(bitmapword));
	tool.words = bitmapset->words;

	fieldReader = reader->nr_fld_readers[bitmapIoTooling[1].nfd_type];
	fieldReader(from, &bitmapIoTooling[1], &tool.words, flags | ND_READ_ARRAY_PREALLOCATED);

	reader->nr_finish_node(from, desc, flags);

	return (Node *) bitmapset;
}

bool
WriteNodeList(StringInfo into, const Node *node, NodeWriter writer,
			  uint32 flags)
{
	/* TODO */
	return false;
}

Node *
ReadNodeList(StringInfo into, NodeReader reader, uint32 flags)
{
	/* TODO */
	return NULL;
}

bool
WriteNodeInteger(StringInfo into, const Node *node, NodeWriter writer,
				 uint32 flags)
{
	/* TODO */
	return false;
}

Node *
ReadNodeInteger(StringInfo into, NodeReader reader, uint32 flags)
{
	/* TODO */
	return NULL;
}

bool
WriteNodeFloat(StringInfo into, const Node *node, NodeWriter writer,
			   uint32 flags)
{
	/* TODO */
	return false;
}

Node *
ReadNodeFloat(StringInfo into, NodeReader reader, uint32 flags)
{
	/* TODO */
	return NULL;
}

bool
WriteNodeBoolean(StringInfo into, const Node *node, NodeWriter writer,
				 uint32 flags)
{
	/* TODO */
	return false;
}

Node *
ReadNodeBoolean(StringInfo into, NodeReader reader, uint32 flags)
{
	/* TODO */
	return NULL;
}

bool
WriteNodeString(StringInfo into, const Node *node, NodeWriter writer,
				uint32 flags)
{
	/* TODO */
	return false;
}

Node *
ReadNodeString(StringInfo into, NodeReader reader, uint32 flags)
{
	/* TODO */
	return NULL;
}

bool
WriteNodeBitString(StringInfo into, const Node *node, NodeWriter writer,
				   uint32 flags)
{
	/* TODO */
	return false;
}

Node *
ReadNodeBitString(StringInfo into, NodeReader reader, uint32 flags)
{
	/* TODO */
	return NULL;
}
