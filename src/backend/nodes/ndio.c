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
#include "nodes/ndio.h"
#include "nodes/nodedesc.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "port/pg_bitutils.h"

#define TYPESIZE(_name_, _type_) \
	[NFT_##_name_] = sizeof(_type_)

int		type_sizes[NFT_NUM_TYPES] = {
	TYPESIZE(BOOL, bool),
	TYPESIZE(PARSELOC, ParseLoc),
	TYPESIZE(INT, int),
	TYPESIZE(INT16, int16),
	TYPESIZE(INT32, int32),
	TYPESIZE(LONG, long),
	TYPESIZE(UINT, uint),
	TYPESIZE(UINT16, uint16),
	TYPESIZE(UINT32, uint32),
	TYPESIZE(UINT64, uint64),
	TYPESIZE(OID, Oid),
	TYPESIZE(CHAR, char),
	TYPESIZE(DOUBLE, double),
	TYPESIZE(ENUM, int),
	TYPESIZE(CSTRING, char *),
	TYPESIZE(NODE, Node *),
	TYPESIZE(PARAM_PATH_INFO, ParamPathInfo *),
};
#undef TYPESIZE


static bool
ReadArrayValue(StringInfo from, NodeReader reader, NodeFieldType type,
			   const void **array, int arrlen, uint32 flags)
{
	char	   *fld;
	NodeFieldType basetype = type & ~NFT_ARRAYTYPE;
	const int	val_size = type_sizes[basetype];
	ReadTypedValue	fvr = reader->nr_val_readers[basetype];

	if (!reader->nr_start_array(from, flags))
	{
		Assert(!(flags & ND_READ_ARRAY_PREALLOCATED));
		*array = NULL;
		return true;
	}

	/* we're at the start of array data, now we can start reading values */
	if (flags & ND_READ_ARRAY_PREALLOCATED)
		fld = (char *) (*array);
	else
		fld = palloc(arrlen * val_size);

	for (int i = 0; i < arrlen; i++)
	{
		if (i > 0 && reader->nr_array_value_separator)
			reader->nr_array_value_separator(from, flags);
		fvr(from, fld + i * val_size, flags);
	}

	if (reader->nr_end_array)
		reader->nr_end_array(from, flags);
	*array = (void *) fld;
	return true;
}
static bool
ReadArrayField(StringInfo from, NodeReader reader, NodeFieldDesc fdesc,
			   const void **array, uint32 flags)
{
	int			arrlen = *(int *)(((char *) array) + fdesc->nfd_arr_len_off);

	if (!reader->nr_start_field(from, fdesc, flags))
		return false;

	ReadArrayValue(from, reader, fdesc->nfd_type, array, arrlen, flags);

	if (reader->nr_end_field)
		reader->nr_end_field(from, flags);
	return true;
}

Node *
ReadNode(StringInfo from, NodeReader reader, uint32 flags)
{
	NodeTag		tag;
	NodeDesc	desc;
	char	   *ptr;
	Node	   *node;
	int			last_read = -1;

	check_stack_depth();

	if (!reader->nr_read_tag(from, flags, &tag))
	{
		Assert(tag == T_Invalid);
		return NULL;
	}

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
	node->type = tag;
	ptr = (char *) node;

	/* branchy handling here */
	for (int i = 0; i < desc->nd_fields; i++)
	{
		NodeFieldDesc fdesc = GetNodeFieldDesc(desc, i);

		if (fdesc->nfd_flags & NODEDESC_DISABLE_READ)
			continue;

		if (fdesc->nfd_type & NFT_ARRAYTYPE)
		{
			if (ReadArrayField(from, reader, fdesc,
							   (const void **) (ptr + fdesc->nfd_offset),
							   flags))
				last_read = i;
		}
		else
		{
			ReadTypedField freader;
			freader = reader->nr_fld_readers[fdesc->nfd_type];
			if (freader(from, fdesc, ptr + fdesc->nfd_offset, flags))
				last_read = i;
		}
	}

	reader->nr_finish_node(from, desc, last_read, flags);

	return node;
}

static bool
WriteArrayValue(StringInfo into, NodeWriter writer, NodeFieldType type,
				const void **array, int arraylen, uint32 flags)
{

	char	   *fld = *(char **) (array);
	NodeFieldType basetype = type & ~NFT_ARRAYTYPE;
	const int	val_len = type_sizes[basetype];
	WriteTypedValue	fvw = writer->nw_val_writers[basetype];

	if (fld == NULL)
	{
		writer->nw_write_null(into, flags);
		return true;
	}

	writer->nw_start_array(into, flags);

	for (int j = 0; j < arraylen; j++)
	{
		if (j > 0 && writer->nw_field_entry_separator)
			writer->nw_field_entry_separator(into, flags);
		fvw(into, fld, flags);
		fld += val_len;
	}

	writer->nw_end_array(into, flags);

	return true;
}

static bool
WriteArrayField(StringInfo into, NodeWriter writer, NodeFieldDesc fdesc,
				const void **array, uint32 flags)
{

	char	   *fld = *(char **) (array);

	if (fld == NULL)
	{
		if ((writer->flags | flags) & ND_WRITE_NO_SKIP_DEFAULTS)
		{
			writer->nw_start_field(into, fdesc, flags);
			writer->nw_write_null(into, flags);
			writer->nw_end_field(into, flags);
			return true;
		}
		else
		{
			return false;
		}
	}

	writer->nw_start_field(into, fdesc, flags);
	WriteArrayValue(into, writer, fdesc->nfd_type, array,
					*(int *) (((char *) array) + fdesc->nfd_arr_len_off),
					flags);
	writer->nw_end_field(into, flags);

	return true;
}


bool
WriteNode(StringInfo into, const Node *node, NodeWriter writer, uint32 flags)
{
	NodeTag		tag;
	NodeDesc	desc;
	char	   *ptr = (char *) node;
	int			last_written_field = -1;

	check_stack_depth();

	if (node == NULL)
	{
		writer->nw_write_null(into, flags);

		return true;
	}

	tag = nodeTag(node);
	desc = GetNodeDesc(tag);

	if (unlikely(desc->nd_flags & NODEDESC_DISABLE_WRITE))
		elog(ERROR, "Can't write node type %s", desc->nd_name);

	if (desc->nd_flags & NODEDESC_CUSTOM_WRITE)
	{
		WriteNodeFunc	nwriter;
		int	off = desc->nd_custom_off;
		off += pg_number_of_ones[desc->nd_flags & (NODEDESC_CUSTOM_WRITE - 1)];

		nwriter = CustomNodeDescFunctions[off].cndf_write_node;
		return nwriter(into, node, writer, flags);
	}

	Assert(!(desc->nd_fld_flags & (NODEDESC_CUSTOM_READ |
								   NODEDESC_CUSTOM_WRITE)));
	int namelen = NodeDescriptors[1].nd_namelen;


	writer->nw_start_node(into, desc, flags);

	for (int i = 0; i < desc->nd_fields; i++)
	{
		NodeFieldDesc fdesc = GetNodeFieldDesc(desc, i);

		if (fdesc->nfd_flags & NODEDESC_DISABLE_WRITE)
			continue;

		if (fdesc->nfd_type & NFT_ARRAYTYPE)
		{
			if (WriteArrayField(into, writer, fdesc,
								(const void **) ptr + fdesc->nfd_offset,
								flags))
				last_written_field = i;
		}
		else
		{
			WriteTypedField fwriter;
			fwriter = writer->nw_fld_writers[fdesc->nfd_type];

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
#include "varatt.h"

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
		.nfd_namelen = sizeof("nwords") - 1,
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
		.nfd_namelen = sizeof("words") - 1,
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

	writer->nw_start_field(into, &bitmapIoTooling[1], flags);
	WriteArrayValue(into, writer, bitmapIoTooling[1].nfd_type,
					(const void **) &tool.words, tool.nwords, flags);
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

	reader->nr_start_field(from, &bitmapIoTooling[1], flags);
	ReadArrayValue(from, reader, bitmapIoTooling[1].nfd_type,
					(const void **) &tool.words, tool.nwords, flags | ND_READ_ARRAY_PREALLOCATED);

	reader->nr_finish_node(from, desc, 1, flags);

	return (Node *) bitmapset;
}

typedef struct ListIOTool {
	int			list_len;
	const ListCell  *words;
} ListIOTool;

static const NodeFieldDescData listIoTooling[] = {
	{
		.nfd_name = "n",
		.nfd_node = T_List,
		.nfd_type = NFT_INT,
		.nfd_namelen = sizeof("n") - 1,
		.nfd_field_no = 0,
		.nfd_offset = offsetof(ListIOTool, list_len),
		.nfd_flags = 0,
		.nfd_arr_len_off = 0,
		.nfd_custom_off = 0,
	},
	{
		.nfd_name = "items",
		.nfd_node = T_List,
		.nfd_type = NFT_NODE + NFT_ARRAYTYPE,
		.nfd_namelen = sizeof("items") - 1,
		.nfd_field_no = 1,
		.nfd_offset = offsetof(BitmapsetTool, words),
		.nfd_flags = 0,
		.nfd_arr_len_off = (ssize_t) (offsetof(BitmapsetTool, nwords) -
									  offsetof(BitmapsetTool, words)),
		.nfd_custom_off = 0,
	},
};

static bool
WriteNodeListTyped(StringInfo into, const Node *node, NodeWriter writer,
				   uint32 flags, NodeFieldType type)
{
	NodeDesc	listDesc = GetNodeDesc(node->type);
	const List  *list = (const List *) node;
	WriteTypedValue vw;
	WriteTypedField fldw;

	Assert(node != NULL);
	Assert(list->length > 0);
	writer->nw_start_node(into, listDesc, flags);

	fldw = writer->nw_fld_writers[NFT_INT];
	/* Note: We use only unconditional writes here */
	fldw(into, &listIoTooling[0], &list->length, flags);

	if (writer->nw_field_entry_separator)
		writer->nw_field_entry_separator(into, flags);

	/* List entries, always Node for normal Lists */
	vw = writer->nw_val_writers[type];
	writer->nw_start_field(into, &listIoTooling[1], flags);
	writer->nw_start_array(into, flags);

	for (int i = 0; i < list->length; i++)
	{
		if (i > 0 && writer->nw_field_entry_separator)
			writer->nw_field_entry_separator(into, flags);

		vw(into, list_nth_cell(list, i), flags);
	}

	if (writer->nw_end_array)
		writer->nw_end_array(into, flags);
	if (writer->nw_end_field)
		writer->nw_end_field(into, flags);
	writer->nw_finish_node(into, listDesc, 0, flags);
	return true;
}


static Node *
ReadNodeListTyped(StringInfo from, NodeReader reader, uint32 flags,
				  NodeFieldType type, NodeTag listType)
{
	int			len = -1;
	ReadTypedValue vr;
	ReadTypedField fr;
	List	   *list;
	NodeDesc	desc = GetNodeDesc(listType);

	fr = reader->nr_fld_readers[NFT_INT];
	fr(from, &listIoTooling[0], &len, flags);

	Assert(len > 0);
	list = (List *) palloc(sizeof(List));
	list->type = listType;
	list->length = len;
	list->max_length = len;
	list->elements = palloc(sizeof(ListCell) * len);

	vr = reader->nr_val_readers[type];
	reader->nr_start_field(from, &listIoTooling[1], flags);
	reader->nr_start_array(from, flags);
	for (int i = 0; i < len; i++)
	{
		if (i > 0 && reader->nr_array_value_separator)
			reader->nr_array_value_separator(from, flags);

		vr(from, list_nth_cell(list, i), flags);
	}
	if (reader->nr_end_array)
		reader->nr_end_array(from, flags);
	if (reader->nr_end_field)
		reader->nr_end_field(from, flags);
	reader->nr_finish_node(from, desc, 0, flags);

	return (Node *) list;
}

bool
WriteNodeList(StringInfo into, const Node *node, NodeWriter writer,
			  uint32 flags)
{
	return WriteNodeListTyped(into, node, writer, flags, NFT_NODE);
}

Node *
ReadNodeList(StringInfo from, NodeReader reader, uint32 flags)
{
	return ReadNodeListTyped(from, reader, flags, NFT_NODE, T_List);
}

bool
WriteNodeIntList(StringInfo into, const Node *node, NodeWriter writer,
				 uint32 flags)
{
	return WriteNodeListTyped(into, node, writer, flags, NFT_INT);
}

Node *
ReadNodeIntList(StringInfo from, NodeReader reader, uint32 flags)
{
	return ReadNodeListTyped(from, reader, flags, NFT_INT, T_IntList);
}

bool
WriteNodeOidList(StringInfo into, const Node *node, NodeWriter writer,
				 uint32 flags)
{
	return WriteNodeListTyped(into, node, writer, flags, NFT_OID);
}


Node *
ReadNodeOidList(StringInfo from, NodeReader reader, uint32 flags)
{
	return ReadNodeListTyped(from, reader, flags, NFT_OID, T_OidList);
}

bool
WriteNodeXidList(StringInfo into, const Node *node, NodeWriter writer,
				 uint32 flags)
{
	return WriteNodeListTyped(into, node, writer, flags, NFT_UINT32);
}

Node *
ReadNodeXidList(StringInfo from, NodeReader reader, uint32 flags)
{
	return ReadNodeListTyped(from, reader, flags, NFT_UINT32, T_XidList);
}

bool
WriteNodeInteger(StringInfo into, const Node *node, NodeWriter writer,
				 uint32 flags)
{
	NodeDesc	desc = GetNodeDesc(T_Integer);
	Integer	   *intNode = castNode(Integer, node);

	writer->nw_start_node(into, desc, flags);

	if (writer->nw_field_entry_separator)
		writer->nw_field_entry_separator(into, flags);

	writer->nw_val_writers[NFT_INT](into, &intNode->ival, flags);

	return true;
}

Node *
ReadNodeInteger(StringInfo from, NodeReader reader, uint32 flags)
{
	Integer	   *node = makeNode(Integer);

	if (reader->nr_array_value_separator)
		reader->nr_array_value_separator(from, flags);

	reader->nr_val_readers[NFT_INT](from, &node->ival, flags);

	return (Node *) node;
}

bool
WriteNodeFloat(StringInfo into, const Node *node, NodeWriter writer,
			   uint32 flags)
{
	NodeDesc	desc = GetNodeDesc(T_Float);
	Float	   *floatNode = castNode(Float, node);

	writer->nw_start_node(into, desc, flags);

	if (writer->nw_field_entry_separator)
		writer->nw_field_entry_separator(into, flags);

	writer->nw_val_writers[NFT_CSTRING](into, &floatNode->fval, flags);

	return true;
}

Node *
ReadNodeFloat(StringInfo from, NodeReader reader, uint32 flags)
{
	Float	   *node = makeNode(Float);

	if (reader->nr_array_value_separator)
		reader->nr_array_value_separator(from, flags);

	reader->nr_val_readers[NFT_CSTRING](from, &node->fval, flags);

	return (Node *) node;
}

bool
WriteNodeBoolean(StringInfo into, const Node *node, NodeWriter writer,
				 uint32 flags)
{
	NodeDesc	desc = GetNodeDesc(T_Boolean);
	Boolean	   *boolNode = castNode(Boolean, node);

	writer->nw_start_node(into, desc, flags);

	if (writer->nw_field_entry_separator)
		writer->nw_field_entry_separator(into, flags);

	writer->nw_val_writers[NFT_BOOL](into, &boolNode->boolval, flags);

	return true;
}

Node *
ReadNodeBoolean(StringInfo from, NodeReader reader, uint32 flags)
{
	Boolean	   *node = makeNode(Boolean);

	if (reader->nr_array_value_separator)
		reader->nr_array_value_separator(from, flags);

	reader->nr_val_readers[NFT_BOOL](from, &node->boolval, flags);

	return (Node *) node;
}

bool
WriteNodeString(StringInfo into, const Node *node, NodeWriter writer,
				uint32 flags)
{
	NodeDesc	desc = GetNodeDesc(T_String);
	String	   *stringNode = castNode(String, node);

	Assert(PointerIsValid(stringNode->sval));

	writer->nw_start_node(into, desc, flags);
	if (writer->nw_field_entry_separator)
		writer->nw_field_entry_separator(into, flags);
	writer->nw_val_writers[NFT_CSTRING](into, &stringNode->sval, flags);

	return true;
}

Node *
ReadNodeString(StringInfo from, NodeReader reader, uint32 flags)
{
	String	   *node = makeNode(String);

	if (reader->nr_array_value_separator)
		reader->nr_array_value_separator(from, flags);

	reader->nr_val_readers[NFT_CSTRING](from, &node->sval, flags);

	return (Node *) node;
}

bool
WriteNodeBitString(StringInfo into, const Node *node, NodeWriter writer,
				   uint32 flags)
{
	NodeDesc	desc = GetNodeDesc(T_BitString);
	BitString  *bsNode = castNode(BitString, node);

	Assert(PointerIsValid(bsNode->bsval));

	writer->nw_start_node(into, desc, flags);
	if (writer->nw_field_entry_separator)
		writer->nw_field_entry_separator(into, flags);
	writer->nw_val_writers[NFT_CSTRING](into, &bsNode->bsval, flags);

	return true;
}

Node *
ReadNodeBitString(StringInfo from, NodeReader reader, uint32 flags)
{
	BitString	   *node = makeNode(BitString);

	if (reader->nr_array_value_separator)
		reader->nr_array_value_separator(from, flags);

	reader->nr_val_readers[NFT_CSTRING](from, &node->bsval, flags);

	return (Node *) node;
}

#define WRITE_NORMAL_FIELD(num, name) \
	do { \
		fdesc = GetNodeFieldDesc(desc, (num)); \
		Assert(strcmp(fdesc->nfd_name, (name)) == 0); \
		fwriter = writer->nw_fld_writers[fdesc->nfd_type]; \
		if (fwriter(into, fdesc, ptr + fdesc->nfd_offset, flags)) \
			last_written_field = (num); \
	} while (false)

#define READ_NORMAL_FIELD(num, name) \
	do { \
		fdesc = GetNodeFieldDesc(desc, (num)); \
		Assert(strcmp(fdesc->nfd_name, (name)) == 0); \
		freader = reader->nr_fld_readers[fdesc->nfd_type]; \
		if (freader(from, fdesc, ptr + fdesc->nfd_offset, flags)) \
			last_read = (num); \
	} while (false)

typedef struct ConstIoTool {
	int			len;
	const void *payload;
} ConstIoTool;

static const NodeFieldDescData constIoTooling[] = {
	{
		/* */
		.nfd_name = "varlen_hdr",
		.nfd_node = T_Const,
		.nfd_type = NFT_INT,
		.nfd_namelen = sizeof("varlen_hdr") - 1,
		.nfd_field_no = 8, /* start counting after original fields */
		.nfd_offset = 0,
		.nfd_flags = 0,
		.nfd_arr_len_off = 0,
		.nfd_custom_off = 0,
	},
	{
		.nfd_name = "constvalue",
		.nfd_node = T_Const,
		.nfd_type = (NFT_CHAR | NFT_ARRAYTYPE),
		.nfd_namelen = sizeof("constvalue") - 1,
		.nfd_field_no = 8, /* start counting after original fields */
		.nfd_offset = 0,
		.nfd_flags = 0,
		.nfd_arr_len_off = (ssize_t) offsetof(ConstIoTool, len) - (ssize_t) offsetof(ConstIoTool, payload),
		.nfd_custom_off = 0,
	},
	{
		/* Note: Field is equivalently named and defined to the one above,
		 * with only differentiator the type. That's fine, we handle this in
		 * deserialization. */
		.nfd_name = "constvalue",
		.nfd_node = T_Const,
		.nfd_type = NFT_CSTRING,
		.nfd_namelen = sizeof("constvalue") - 1,
		.nfd_field_no = 8, /* start counting after original fields */
		.nfd_offset = 0,
		.nfd_flags = 0,
		.nfd_arr_len_off = 0,
		.nfd_custom_off = 0,
	},
};

bool
WriteNodeConst(StringInfo into, const Node *node, NodeWriter writer,
				   uint32 flags)
{
	const Const *constNode = castNode(Const, node);
	NodeDesc	desc = GetNodeDesc(T_Const);
	NodeFieldDesc fdesc;
	WriteTypedField fwriter;
	char	   *ptr = (char *) node;
	int			last_written_field = -1;

	writer->nw_start_node(into, desc, flags);

	WRITE_NORMAL_FIELD(0, "consttype");
	WRITE_NORMAL_FIELD(1, "consttypmod");
	WRITE_NORMAL_FIELD(2, "constcollid");
	WRITE_NORMAL_FIELD(3, "constlen");
	WRITE_NORMAL_FIELD(5, "constisnull");
	WRITE_NORMAL_FIELD(6, "constbyval");
	WRITE_NORMAL_FIELD(7, "location");

	if (!constNode->constisnull)
	{
		if (constNode->constlen > 0)
		{
			ConstIoTool local;

			fdesc = &constIoTooling[1];

			if (constNode->constbyval)
			{
				local = (ConstIoTool) {
					.len = sizeof(Datum),
					.payload = &constNode->constvalue,
				};
			}
			else
			{
				local = (ConstIoTool) {
					.len = constNode->constlen,
					.payload = DatumGetPointer(constNode->constvalue),
				};
			}

			WriteArrayField(into, writer, fdesc, &local.payload, flags);
		}
		else if (constNode->constlen == -1) /* TOAST */
		{
			ConstIoTool local;

			local = (ConstIoTool) {
				.len = VARSIZE_ANY_EXHDR(constNode->constvalue),
				.payload = VARDATA_ANY(constNode->constvalue),
			};
			fdesc = &constIoTooling[0];

			fwriter = writer->nw_fld_writers[fdesc->nfd_type];
			fwriter(into, fdesc, &local.len, flags);

			fdesc = &constIoTooling[1];
			WriteArrayField(into, writer, fdesc, &local.payload, flags);
		}
		else if (constNode->constlen == -2) /* cstring */
		{
			fdesc = &constIoTooling[2];

			fwriter = writer->nw_fld_writers[fdesc->nfd_type];
			fwriter(into, fdesc, (void *) &constNode->constvalue, flags);
		}
	}

	/* all fields serialized but */
	return writer->nw_finish_node(into, desc, -1, flags);
}

Node *
ReadNodeConst(StringInfo from, NodeReader reader, uint32 flags)
{
	Const		   *node = makeNode(Const);
	NodeDesc		desc = GetNodeDesc(T_Const);
	NodeFieldDesc	fdesc;
	ReadTypedField	freader;
	char		   *ptr = (char *) node;
	int				last_read = -1;

	READ_NORMAL_FIELD(0, "consttype");
	READ_NORMAL_FIELD(1, "consttypmod");
	READ_NORMAL_FIELD(2, "constcollid");
	READ_NORMAL_FIELD(3, "constlen");
	READ_NORMAL_FIELD(5, "constisnull");
	READ_NORMAL_FIELD(6, "constbyval");
	READ_NORMAL_FIELD(7, "location");

	if (!node->constisnull)
	{
		if (node->constlen > 0)
		{
			ConstIoTool local;
			fdesc = &constIoTooling[1];

			if (node->constbyval)
			{
				local = (ConstIoTool) {
					.len = sizeof(Datum),
					.payload = &node->constvalue,
				};
			}
			else
			{
				node->constvalue = PointerGetDatum(palloc(node->constlen));
				local = (ConstIoTool) {
					.len = node->constlen,
					.payload = DatumGetPointer(node->constvalue),
				};
			}

			ReadArrayField(from, reader, fdesc, &local.payload,
						   flags | ND_READ_ARRAY_PREALLOCATED);
		}
		else if (node->constlen == -1) /* TOAST */
		{
			ConstIoTool		local;
			void		   *data;
			
			fdesc = &constIoTooling[0];

			freader = reader->nr_fld_readers[fdesc->nfd_type];
			freader(from, fdesc, &local.len, flags);
			data = palloc(local.len + 4);
			local.payload = ((char *) data) + 4;

			SET_VARSIZE(data, local.len + 4);

			fdesc = &constIoTooling[1];
			ReadArrayField(from, reader, fdesc, &local.payload,
						   flags | ND_READ_ARRAY_PREALLOCATED);
		}
		else if (node->constlen == -2) /* cstring */
		{
			fdesc = &constIoTooling[2];

			freader = reader->nr_fld_readers[fdesc->nfd_type];
			freader(from, fdesc, (void *) &node->constvalue, flags);
		}
	}

	reader->nr_finish_node(from, desc, -1, flags);

	return (Node *) node;
}

static const NodeFieldDescData a_constIoTooling[] = {
	{
		.nfd_name = "val",
		.nfd_node = T_A_Const,
		.nfd_type = NFT_NODE,
		.nfd_namelen = sizeof("val") - 1,
		.nfd_field_no = 0,
		.nfd_offset = 0,
		.nfd_flags = 0,
		.nfd_arr_len_off = 0,
	}
};

bool
WriteNodeA_Const(StringInfo into, const Node *node, NodeWriter writer,
				  uint32 flags)
{
	const A_Const *constNode = castNode(A_Const, node);
	NodeDesc		desc = GetNodeDesc(T_A_Const);
	NodeFieldDesc	fdesc;
	WriteTypedField	fwriter;
	char		   *ptr = (char *) node;
	int				last_written_field = -1;

	writer->nw_start_node(into, desc, flags);

	if (!constNode->isnull || (writer->flags | flags) & ND_WRITE_NO_SKIP_DEFAULTS)
	{
		Node	   *inl = (Node *) &constNode->val;
		fdesc = &a_constIoTooling[0];
		fwriter = writer->nw_fld_writers[fdesc->nfd_type];
		if (fwriter(into, fdesc, &inl, flags))
			last_written_field = 0;
	}

	WRITE_NORMAL_FIELD(1, "isnull");
	WRITE_NORMAL_FIELD(2, "location");

	/* all fields serialized */
	return writer->nw_finish_node(into, desc, last_written_field, flags);
}

Node *
ReadNodeA_Const(StringInfo from, NodeReader reader, uint32 flags)
{
	A_Const		   *node = makeNode(A_Const);
	NodeDesc		desc = GetNodeDesc(T_A_Const);
	NodeFieldDesc	fdesc;
	ReadTypedField	freader;
	char		   *ptr = (char *) node;
	int				last_read = -1;

	do {
		/*
		 * I'm not super happy about leaking this, but it's arguably better
		 * than the alternative.
		 */
		Node	   *inl;
		fdesc = &a_constIoTooling[0];
		freader = reader->nr_fld_readers[fdesc->nfd_type];
		if (freader(from, fdesc, &inl, flags))
		{
			memcpy(&node->val, inl,
				   GetNodeDesc(nodeTag(inl))->nd_size);
		}
	} while (false);

	READ_NORMAL_FIELD(1, "isnull");
	READ_NORMAL_FIELD(2, "location");

	reader->nr_finish_node(from, desc, last_read, flags);
	return (Node *) node;
}

bool
WriteNodeEquivalenceClass(StringInfo into, const Node *node,
						  NodeWriter writer, uint32 flags)
{
	const EquivalenceClass *ecNode = castNode(EquivalenceClass, node);
	NodeDesc		desc = GetNodeDesc(T_EquivalenceClass);
	NodeFieldDesc	fdesc;
	WriteTypedField	fwriter;
	char		   *ptr;
	int				last_written_field = -1;
	while (ecNode->ec_merged)
		ecNode = ecNode->ec_merged;

	ptr = (char *) ecNode;

	writer->nw_start_node(into, desc, flags);

	WRITE_NORMAL_FIELD(0, "ec_opfamilies");
	WRITE_NORMAL_FIELD(1, "ec_collation");
	WRITE_NORMAL_FIELD(2, "ec_members");
	WRITE_NORMAL_FIELD(3, "ec_sources");
	WRITE_NORMAL_FIELD(4, "ec_derives");
	WRITE_NORMAL_FIELD(5, "ec_relids");
	WRITE_NORMAL_FIELD(6, "ec_has_const");
	WRITE_NORMAL_FIELD(7, "ec_has_volatile");
	WRITE_NORMAL_FIELD(8, "ec_broken");
	WRITE_NORMAL_FIELD(9, "ec_sortref");
	WRITE_NORMAL_FIELD(10, "ec_min_security");
	WRITE_NORMAL_FIELD(11, "ec_max_security");
	WRITE_NORMAL_FIELD(12, "ec_merged");

	/* all fields serialized */
	return writer->nw_finish_node(into, desc, last_written_field, flags);
}

Node *
ReadNodeEquivalenceClass(StringInfo from, NodeReader reader, uint32 flags)
{
	Assert(false);
	return NULL;
}

bool
WriteNodeExtensibleNode(StringInfo into, const Node *node,
						NodeWriter writer, uint32 flags)
{
	Assert(false);
	return false;
}

Node *
ReadNodeExtensibleNode(StringInfo from, NodeReader reader, uint32 flags)
{
	Assert(false);
	return NULL;
}

bool
WriteNodeForeignKeyOptInfo(StringInfo into, const Node *node,
						NodeWriter writer, uint32 flags)
{
	Assert(false);
	return false;
}

Node *
ReadNodeForeignKeyOptInfo(StringInfo from, NodeReader reader, uint32 flags)
{
	Assert(false);
	return NULL;
}

bool
WriteNodeA_Expr(StringInfo into, const Node *node,
						NodeWriter writer, uint32 flags)
{
	Assert(false);
	return false;
}

Node *
ReadNodeA_Expr(StringInfo from, NodeReader reader, uint32 flags)
{
	Assert(false);
	return NULL;
}

bool
WriteNodeRangeTblEntry(StringInfo into, const Node *node,
						NodeWriter writer, uint32 flags)
{
	Assert(false);
	return false;
}

Node *
ReadNodeRangeTblEntry(StringInfo from, NodeReader reader, uint32 flags)
{
	Assert(false);
	return NULL;
}

bool
WriteNodeBoolExpr(StringInfo into, const Node *node,
						NodeWriter writer, uint32 flags)
{
	Assert(false);
	return false;
}

Node *
ReadNodeBoolExpr(StringInfo from, NodeReader reader, uint32 flags)
{
	Assert(false);
	return NULL;
}
