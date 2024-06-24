/*-------------------------------------------------------------------------
 *
 * ndio_binary.c
 *		Implementation for binary (de)serialization using NodeDesc-based
 *		infrastructure.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/ndio_binary.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/nodes.h"
#include "nodes/ndio.h"
#include "nodes/nodedesc.h"
#include "nodes/plannodes.h"
#include "nodes/replnodes.h"
#include "varatt.h"
#include "fmgr.h"


#define CHECK_CAN_READ(info, length, ...) \
	if ((info)->len - (info)->cursor < (length)) \
		elog(ERROR, __VA_ARGS__)

#define CHECK_CAN_READ_FLD(info, length, desc) \
	CHECK_CAN_READ(info, length, "Error reading field %s: out of data", (desc)->nfd_name)

static inline void
CHECK_FLD_IS_COMPATIBLE(NodeFieldDesc fdesc)
{
	if (fdesc->nfd_flags & NODEDESC_DISABLE_WRITE)
		Assert(fdesc->nfd_flags & NODEDESC_DISABLE_READ);
	else
		Assert(!(fdesc->nfd_flags & NODEDESC_DISABLE_READ));
}

static inline void
CHECK_NODE_IS_COMPATIBLE(NodeDesc desc)
{
	if (desc->nd_flags & NODEDESC_DISABLE_WRITE)
		Assert(desc->nd_flags & NODEDESC_DISABLE_READ);
	else
		Assert(!(desc->nd_flags & NODEDESC_DISABLE_READ));
}

/*---
 * Binary Node Reader
 *		Reader for a binary node serialization format.
 *
 * The serialization format for a normal Node is nodetag (as uint16), followed
 * by its fields.  Individual fields are serialized with a single byte
 * indicating the field number, followed by the field's contents.  
 * If a field contains its datatype default value, it is omitted from the
 * serialized format; the missing field is recognised during deserialization,
 * where the default value is re-inserted into the deserialized node.
 *
 * To detect the last field of a node, a virtual field with field_no=0xFF
 * is inserted at the end if the final field of the node was omitted.
 */

static bool
bnr_read_tag(StringInfo from, uint32 flags, NodeTag *out)
{
	uint16	tag_value;

	CHECK_CAN_READ(from, sizeof(uint16), "Out of data reading node tag");

	memcpy(&tag_value, &from->data[from->cursor], sizeof(uint16));
	from->cursor += sizeof(uint16);

	*out = tag_value;

	if (tag_value == 0)
		return false;

	return true;
}

static void
bnr_finish_node(StringInfo from, NodeDesc desc, int final_field,
				uint32 flags)
{
	uint8		final;

	CHECK_NODE_IS_COMPATIBLE(desc);
	
	if (final_field == desc->nd_fields - 1)
		return;

	CHECK_CAN_READ(from, sizeof(uint8), "Out of data reading node close tag");
	final = from->data[from->cursor];

	if (unlikely(final != PG_BINSER_NODE_END))
		ereport(PANIC,
				errbacktrace(),
				errmsg_internal("Unexpected byte %02x at end of node %s during deserialization",
								final, desc->nd_name));

	from->cursor += sizeof(uint8);
}

static bool
bnr_start_field(StringInfo from, NodeFieldDesc desc, uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);

	if (from->data[from->cursor] == desc->nfd_field_no)
	{
		from->cursor += sizeof(uint8);
		return true;
	}

	return false;
}
static bool
bnr_start_array(StringInfo from, uint32 flags)
{
	CHECK_CAN_READ(from, sizeof(char), "Ran out of data at array");

	/* no-op: no array start/end delineation. */
	return true;
}

static bool
bnvr_VARLENA(StringInfo from, void *field, uint32 flags)
{
	uint32		vl_len_;
	struct vardata *value;

	CHECK_CAN_READ(from, sizeof(uint32),
				   "Error reading varlena data: Out of data");
	memcpy(&vl_len_, from->data + from->cursor, sizeof(uint32));
	from->cursor += sizeof(uint32);

	CHECK_CAN_READ(from, vl_len_,
				   "Error reading varlena data: Out of data");
	value = palloc(vl_len_ + VARHDRSZ);

	SET_VARSIZE(value, vl_len_ + VARHDRSZ);
	memcpy(VARDATA(value), from->data + from->cursor, vl_len_);
	from->cursor += vl_len_;

	*((struct vardata **) field) = value;

	return true;
}

static bool
bnfr_VARLENA(StringInfo from, NodeFieldDesc desc, void *field,
				 uint32 flags)
{
	uint8		fldno;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);
	fldno = from->data[from->cursor];

	if (fldno != desc->nfd_field_no)
	{
		Assert(fldno > desc->nfd_field_no);
		*((char **) field) = NULL;
		return false;
	}

	from->cursor += sizeof(uint8);

	if (unlikely(desc->nfd_flags & NFD_READ_DEFAULT))
	{
		bool ret = bnvr_VARLENA(from, field, flags);
		*(struct varlena **) field = NULL;
		return ret;
	}

	return bnvr_VARLENA(from, field, flags);
}

static bool
bnvr_CSTRING(StringInfo from, void *field, uint32 flags)
{
	int		len = strnlen(from->data + from->cursor, from->len - from->cursor);
	char   *fld;

	if (len == from->len - from->cursor)
		elog(PANIC, "Error reading cstring: Out of data");

	Assert(from->data[from->cursor + len] == '\0');

	fld = palloc(len + 1);

	memcpy(fld, from->data + from->cursor,
		   len + 1);

	from->cursor += len + 1;

	*((char **) field) = fld;

	return true;
}

static bool
bnfr_CSTRING(StringInfo from, NodeFieldDesc desc,
			 void *field, uint32 flags)
{
	uint8		fldno;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);
	fldno = from->data[from->cursor];

	if (fldno != desc->nfd_field_no)
	{
		Assert(fldno > desc->nfd_field_no);
		*((char **) field) = NULL;
		return false;
	}

	from->cursor += sizeof(uint8);

	if (unlikely(desc->nfd_flags & NFD_READ_DEFAULT))
	{
		bool ret = bnvr_CSTRING(from, field, flags);
		*(struct varlena **) field = NULL;
		return ret;
	}

	return bnvr_CSTRING(from, field, flags);
}

static bool
bnvr_NODE(StringInfo from, void *field, uint32 flags)
{
	Node	  **fld = (Node **) field;

	*fld = ReadNode(from, BinaryNodeReader, flags);
	return true;
}

static bool
bnfr_NODE(StringInfo from, NodeFieldDesc desc,
		  void *field, uint32 flags)
{
	uint8		fldno;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);

	fldno = from->data[from->cursor];

	if (fldno != desc->nfd_field_no)
	{
		Assert(fldno > desc->nfd_field_no);
		*((char **) field) = NULL;
		return false;
	}

	from->cursor += sizeof(uint8);

	if (unlikely(desc->nfd_flags & NFD_READ_DEFAULT))
	{
		bool ret = bnvr_NODE(from, field, flags);
		*(struct varlena **) field = NULL;
		return ret;
	}

	return bnvr_NODE(from, field, flags);
}

static bool
bnvr_PARSELOC(StringInfo from, void *field, uint32 flags)
{
	CHECK_CAN_READ(from, sizeof(ParseLoc),
				   "Ran out of data whilst reading " "ParseLoc" " value");

	memcpy(field, from->data + from->cursor, sizeof(ParseLoc));
	from->cursor += sizeof(ParseLoc);
	return true;
}

static bool
bnfr_PARSELOC(StringInfo from, NodeFieldDesc desc, void *field, uint32 flags)
{
	uint8 fldno;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);

	fldno = from->data[from->cursor];

	if (fldno != desc->nfd_field_no)
	{
		Assert(fldno > desc->nfd_field_no);
		*((ParseLoc *) field) = -1;
		return false;
	}

	from->cursor += sizeof(uint8);
	if (unlikely(desc->nfd_flags & NFD_READ_DEFAULT))
	{
		bool ret = bnvr_PARSELOC(from, field, flags);
		*((ParseLoc *) field) = -1;
		return ret;
	}

	return bnvr_PARSELOC(from, field, flags);
}

#define BinaryScalarFieldReader(_type_, _uctype_, type_default) \
static bool \
bnvr_##_uctype_(StringInfo from, void *field, uint32 flags) \
{ \
	CHECK_CAN_READ(from, sizeof(_type_), \
				   "Ran out of data whilst reading " #_type_ " value"); \
	\
	memcpy(field, from->data + from->cursor, sizeof(_type_)); \
	from->cursor += sizeof(_type_); \
	return true; \
} \
static bool \
bnfr_##_uctype_(StringInfo from, NodeFieldDesc desc, void *field, \
				uint32 flags) \
{ \
	uint8		fldno; \
	\
	CHECK_FLD_IS_COMPATIBLE(desc); \
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc); \
	\
	fldno = from->data[from->cursor]; \
	\
	if (fldno != desc->nfd_field_no) \
	{ \
		Assert(fldno > desc->nfd_field_no); \
		*((_type_ *) field) = (type_default); \
		return false; \
	} \
	from->cursor += sizeof(uint8); \
	\
	if (unlikely(desc->nfd_flags & NFD_READ_DEFAULT)) \
	{ \
		bool ret = bnvr_##_uctype_(from, field, flags); \
		*((_type_ *) field) = (type_default); \
		return ret; \
	} \
	\
	return bnvr_##_uctype_(from, field, flags); \
}

static bool
bnfr_unimplemented(StringInfo from, NodeFieldDesc desc,
				   void *field, uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	elog(ERROR, "Node field %s's type %02x deserialization is unimplemented",
		 desc->nfd_name, desc->nfd_type);
}

static bool
bnvr_unimplemented(StringInfo from, void *field, uint32 flags)
{
	elog(ERROR, "Node value deserialization is unimplemented for this type");
}

BinaryScalarFieldReader(bool, BOOL, false)
BinaryScalarFieldReader(TypMod, TYPMOD, -1)
BinaryScalarFieldReader(int, INT, 0)
BinaryScalarFieldReader(int16, INT16, 0)
BinaryScalarFieldReader(int32, INT32, 0)
BinaryScalarFieldReader(long, LONG, 0)
BinaryScalarFieldReader(uint8, UINT8, 0)
BinaryScalarFieldReader(uint16, UINT16, 0)
BinaryScalarFieldReader(uint32, UINT32, 0)
BinaryScalarFieldReader(uint64, UINT64, 0)
BinaryScalarFieldReader(Oid, OID, 0)
BinaryScalarFieldReader(char, CHAR, 0)
BinaryScalarFieldReader(double, DOUBLE, 0.0)

#undef BinaryScalarFieldReader
#undef CHECK_CAN_READ
#undef CHECK_CAN_READ_FLD


#define bsfr(_type_) \
	[NFT_##_type_] = bnfr_##_type_
#define bsfr_unimpl(_type_) \
	[NFT_##_type_] = bnfr_unimplemented

#define bsvr(_type_) \
	[NFT_##_type_] = bnvr_##_type_
#define bsvr_unimpl(_type_) \
	[NFT_##_type_] = bnvr_unimplemented

const NodeReader BinaryNodeReader = &(NodeReaderData) {
	.nr_read_tag = bnr_read_tag,
	.nr_finish_node = bnr_finish_node,
	.nr_start_field = bnr_start_field,
	.nr_start_array = bnr_start_array,
	.nr_array_value_separator = NULL, /* no separators here */
	.nr_end_array = NULL,
	.nr_end_field = NULL,
	.nr_fld_readers = {
		bsfr_unimpl(UNDEFINED),
		bsfr(BOOL),
		bsfr(PARSELOC),
		bsfr(TYPMOD),
		bsfr(INT),
		bsfr(INT16),
		bsfr(INT32),
		bsfr(LONG),
		bsfr(UINT8),
		bsfr(UINT16),
		bsfr(UINT32),
		bsfr(UINT64),
		bsfr(OID),
		bsfr(CHAR),
		bsfr(DOUBLE),
		/* Enum has the same layout as INT, and is treated the same for
		 * (de)serialization */
		[NFT_ENUM] = &bnfr_INT, 
		bsfr(VARLENA),
		bsfr(CSTRING),
		bsfr(NODE),
	},
	.nr_val_readers = {
		bsvr_unimpl(UNDEFINED),
		bsvr(BOOL),
		bsvr(PARSELOC),
		bsvr(TYPMOD),
		bsvr(INT),
		bsvr(INT16),
		bsvr(INT32),
		bsvr(LONG),
		bsvr(UINT8),
		bsvr(UINT16),
		bsvr(UINT32),
		bsvr(UINT64),
		bsvr(OID),
		bsvr(CHAR),
		bsvr(DOUBLE),
		/* Enum has the same layout as INT, and is treated the same for
		 * (de)serialization */
		[NFT_ENUM] = &bnvr_INT, 
		bsvr(VARLENA),
		bsvr(CSTRING),
		bsvr(NODE),
	}
};

#undef tsfr
#undef tsfr_unimpl
#undef tsfr_unimpl_arr
#undef tsfr_unknown_type


/*---
 * Binary Node Writer
 *		Writer for a binary node serialization format.
 */

static void
bnw_start_node(StringInfo into, NodeDesc desc, uint32 flags)
{
	uint16		tag = desc->nd_nodetag;

	Assert(desc->nd_nodetag <= UINT16_MAX);
	CHECK_NODE_IS_COMPATIBLE(desc);

	appendBinaryStringInfo(into, &tag, sizeof(uint16));
}

static bool
bnw_finish_node(StringInfo into, NodeDesc desc, int last_field,
				uint32 flags)
{
	uint8		final = PG_BINSER_NODE_END;

	Assert(last_field <= desc->nd_fields);
	CHECK_NODE_IS_COMPATIBLE(desc);

	/* save a byte on end-of-node if we know we've written the last field */
	if (last_field != desc->nd_fields - 1)
		appendBinaryStringInfo(into, &final, sizeof(uint8));

	return true;
}

static void
bnw_start_field(StringInfo into, NodeFieldDesc desc, uint32 flags)
{
	appendBinaryStringInfo(into, &desc->nfd_field_no, sizeof(uint8));
}

static void
bnw_start_array(StringInfo into, uint32 flags)
{
	/* no start-of-array identifier */
}

static void
bnw_write_null(StringInfo into, uint32 flags)
{
	appendBinaryStringInfo(into, "\0\0", 2);
}

static bool
bnvw_VARLENA(StringInfo into, const void *value, uint32 flags)
{
	struct varlena *vlna = *(struct varlena **) value;
	struct varlena *vlnb;
	uint32		vl_len_;
	Assert(vlna != NULL);

	vlnb = pg_detoast_datum(vlna);
	vl_len_ = VARSIZE_ANY_EXHDR(vlnb);

	appendBinaryStringInfo(into, &vl_len_, sizeof(uint32));
	appendBinaryStringInfo(into, VARDATA_ANY(vlnb), VARSIZE_ANY_EXHDR(vlnb));

	if (vlna != vlnb)
		pfree(vlnb);

	return true;
}

static bool
bnfw_VARLENA(StringInfo into, NodeFieldDesc desc, const void *field,
				 uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS) && \
		*(const char **) field == NULL)
		return false;

	appendBinaryStringInfo(into, &desc->nfd_field_no, sizeof(uint8));

	return bnvw_VARLENA(into, field, flags);
}

static bool
bnvw_CSTRING(StringInfo into, const void *value, uint32 flags)
{
	char	   *cstr = *(char **) value;

	appendStringInfoString(into, cstr);
	appendStringInfoCharMacro(into, '\0');

	return true;
}

static bool
bnfw_CSTRING(StringInfo into, NodeFieldDesc desc, const void *field,
			 uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS) && \
		*(const char **) field == NULL)
		return false;

	appendBinaryStringInfo(into, &desc->nfd_field_no, sizeof(uint8));

	return bnvw_CSTRING(into, field, flags);
}

static bool
bnvw_NODE(StringInfo into, const void *field, uint32 flags)
{
	Node	   *val = *(Node **) field;

	return WriteNode(into, val, BinaryNodeWriter, flags);
}

static bool
bnfw_NODE(StringInfo into, NodeFieldDesc desc, const void *field,
		  uint32 flags)
{
	Node	   *node = *(Node **) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS) && node == NULL)
		return false;

	appendBinaryStringInfo(into, &desc->nfd_field_no, sizeof(uint8));

	return bnvw_NODE(into, field, flags);
}

static bool
bnvw_unimplemented(StringInfo into, const void *field, uint32 flags)
{
	Assert(false);
	return false;
}

static bool
bnfw_unimplemented(StringInfo into, NodeFieldDesc desc, const void *field,
				   uint32 flags)
{
	return bnvw_unimplemented(into, field, flags);
}

static bool
bnvw_PARSELOC(StringInfo into, const void *field, uint32 flags)
{
	appendBinaryStringInfo(into, field, sizeof(ParseLoc));
	return true;
}

static bool
bnfw_PARSELOC(StringInfo into, NodeFieldDesc desc, const void *field,
			  uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);
	if ((!(flags & ND_WRITE_NO_SKIP_DEFAULTS) &&
		 *(const ParseLoc *) field == -1) ||
		(flags & ND_WRITE_IGNORE_PARSELOC))
		return false;

	appendBinaryStringInfo(into, &desc->nfd_field_no, sizeof(uint8));
	return bnvw_PARSELOC(into, field, flags);
}

#define BinaryScalarFieldWriter(_type_, _uctype_, type_default) \
static bool \
bnvw_##_uctype_(StringInfo into, const void *field, uint32 flags) \
{ \
	appendBinaryStringInfo(into, field, sizeof(_type_)); \
	return true; \
} \
static bool \
bnfw_##_uctype_(StringInfo into, NodeFieldDesc desc, const void *field, \
				uint32 flags) \
{ \
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS) && \
		*(const _type_ *) field == (type_default)) \
		return false; \
	\
	appendBinaryStringInfo(into, &desc->nfd_field_no, sizeof(uint8)); \
	return bnvw_##_uctype_(into, field, flags); \
}

BinaryScalarFieldWriter(bool, BOOL, false)
BinaryScalarFieldWriter(TypMod, TYPMOD, -1)
BinaryScalarFieldWriter(int, INT, 0)
BinaryScalarFieldWriter(int16, INT16, 0)
BinaryScalarFieldWriter(int32, INT32, 0)
BinaryScalarFieldWriter(long, LONG, 0)
BinaryScalarFieldWriter(uint8, UINT8, 0)
BinaryScalarFieldWriter(uint16, UINT16, 0)
BinaryScalarFieldWriter(uint32, UINT32, 0)
BinaryScalarFieldWriter(uint64, UINT64, 0)
BinaryScalarFieldWriter(Oid, OID, 0)
BinaryScalarFieldWriter(char, CHAR, 0)
BinaryScalarFieldWriter(double, DOUBLE, 0.0)

#define bsfw(_type_) \
	[NFT_##_type_] = bnfw_##_type_
#define bsfw_unimpl(_type_) \
	[NFT_##_type_] = bnfw_unimplemented

#define bsvw(_type_) \
	[NFT_##_type_] = bnvw_##_type_
#define bsvw_unimpl(_type_) \
	[NFT_##_type_] = bnvw_unimplemented

const NodeWriter BinaryNodeWriter = &(NodeWriterData) {
	.nw_start_node = bnw_start_node,
	.nw_finish_node = bnw_finish_node,
	.nw_start_field = bnw_start_field,
	.nw_start_array = bnw_start_array,
	.nw_write_null = bnw_write_null,
	.nw_end_array = NULL,
	.nw_end_field = NULL,
	.nw_fld_writers = {
		bsfw_unimpl(UNDEFINED),
		bsfw(BOOL),
		bsfw(PARSELOC),
		bsfw(TYPMOD),
		bsfw(INT),
		bsfw(INT16),
		bsfw(INT32),
		bsfw(LONG),
		bsfw(UINT8),
		bsfw(UINT16),
		bsfw(UINT32),
		bsfw(UINT64),
		bsfw(OID),
		bsfw(CHAR),
		bsfw(DOUBLE),
		[NFT_ENUM] = &bnfw_INT, /* enum has the same layout as INT */
		bsfw(VARLENA),
		bsfw(CSTRING),
		bsfw(NODE),
	},
	.nw_val_writers = {
		bsvw_unimpl(UNDEFINED),
		bsvw(BOOL),
		bsvw(PARSELOC),
		bsvw(TYPMOD),
		bsvw(INT),
		bsvw(INT16),
		bsvw(INT32),
		bsvw(LONG),
		bsvw(UINT8),
		bsvw(UINT16),
		bsvw(UINT32),
		bsvw(UINT64),
		bsvw(OID),
		bsvw(CHAR),
		bsvw(DOUBLE),
		[NFT_ENUM] = &bnvw_INT, /* enum has the same layout as INT */
		bsvw(VARLENA),
		bsvw(CSTRING),
		bsvw(NODE),
	}
};
