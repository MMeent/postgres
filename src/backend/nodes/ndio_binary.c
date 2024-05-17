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
#include "nodes/nodedesc.h"
#include "nodes/plannodes.h"
#include "nodes/replnodes.h"


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
 * by its fields, followed by a trailing 0xFF byte.  Individual fields are
 * serialized with a single byte indicating the field number, followed by the
 * field's contents.  If a field contains its datatype default value, it is
 * omitted from the serialized format; the missing field is recognised during
 * deserialization, re-inserting the default value into the deserialized node.
 */

static bool
bnr_read_tag(StringInfo from, uint32 flags, NodeTag *out)
{
	uint16	tag_value;

	CHECK_CAN_READ(from, sizeof(uint16), "Out of data reading node tag");

	memcpy(&tag_value, &from->data[from->cursor], sizeof(uint16));
	from->cursor += sizeof(uint16);

	*out = tag_value;
	return true;
}

static void
bnr_finish_node(StringInfo from, NodeDesc desc,
				uint32 flags)
{
	uint8		final;

	CHECK_NODE_IS_COMPATIBLE(desc);
	CHECK_CAN_READ(from, sizeof(uint8), "Out of data reading node tag");
	final = from->data[from->cursor];

	if (unlikely(final != PG_BINSER_NODE_END))
		elog(ERROR, "Unexpected byte %02x at end of node %s during deserialization",
			 final, desc->nd_name);
	from->cursor += sizeof(uint8);
}

static void
bnfr_CSTRING(StringInfo from, NodeFieldDesc desc,
			 void *field, uint32 flags)
{
	uint8		fldno;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);
	fldno = from->data[from->cursor];
	if (fldno == desc->nfd_field_no)
	{
		int		offset = from->cursor + sizeof(uint8);
		int		len = strnlen(&from->data[offset], from->len - offset);
		char   *fld;

		if (len == from->len - offset)
			elog(ERROR, "Error reading field %s: out of data",
				 desc->nfd_name);

		len += 1; /* Include the trailing 0-byte */
		from->cursor += len + sizeof(uint8);
		fld = palloc(len);

		*((char **) field) = fld;

		memcpy(fld, &from->data[offset],
			   len);
		return;
	}
	else
	{
		Assert(fldno > desc->nfd_field_no);
		*((char **) field) = NULL;
		return;
	}
}

static void
bnfr_array_CSTRING(StringInfo from, NodeFieldDesc desc,
				   void *field, uint32 flags)
{
	uint8		fldno;
	int		n = *(int *) (((char *) field) + desc->nfd_arr_len_off);
	char  **arr;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);

	fldno = from->data[from->cursor];
	if (fldno == desc->nfd_field_no)
	{
		int		offset = from->cursor + sizeof(uint8);

		arr = palloc(sizeof(char *) * n);
		*(char ***) field = arr;

		for (int i = 0; i < n; i++)
		{
			int		len;
			char   *fld;

			/* make sure there are bytes in the buffer before reading */
			CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);
			len = strnlen(&from->data[offset], from->len - offset);

			if (len == from->len - offset)
				elog(ERROR, "Error reading field %s: out of data",
					 desc->nfd_name);
	
			len += 1; /* Include the trailing 0-byte */
			fld = palloc(len);
	
			*((char **) field) = fld;
	
			memcpy(fld, &from->data[offset],
				   len);
			offset += len;

			from->cursor = offset;
		}

		return;
	}
	else
	{
		Assert(fldno > desc->nfd_field_no);
		*(char ***) field = NULL;
		return;
	}
}

static void
bnfr_NODE(StringInfo from, NodeFieldDesc desc,
		  void *field, uint32 flags)
{
	uint8		fldno;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);

	fldno = from->data[from->cursor];

	if (fldno != desc->nfd_field_no)
		*(Node **) field = NULL;
	else
	{
		from->cursor += sizeof(uint8);

		*(Node **) field = ReadNode(from, BinaryNodeReader, flags);
	}
}

static void
bnfr_array_NODE(StringInfo from, NodeFieldDesc desc,
				void *field, uint32 flags)
{
	uint8		fldno;

	CHECK_FLD_IS_COMPATIBLE(desc);
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc);

	fldno = from->data[from->cursor];

	if (fldno != desc->nfd_field_no)
	{
		from->cursor += sizeof(uint8);

		*(Node **) field = ReadNode(from, BinaryNodeReader, flags);
	}
	else
	{
		*(Node **) field = NULL;
	}

}


#define BinaryScalarFieldReader(_type_, _uctype_, type_default) \
static void \
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
	if (fldno == desc->nfd_field_no) \
	{ \
		int offset = from->cursor + sizeof(uint8); \
		CHECK_CAN_READ_FLD(from, sizeof(uint8) + sizeof(_type_), desc); \
		from->cursor += sizeof(_type_) + sizeof(uint8); \
		memcpy(field, &from->data[offset], \
			   sizeof(_type_)); \
		return; \
	} \
	else \
	{ \
		Assert(fldno > desc->nfd_field_no); \
		*((_type_ *) field) = (type_default); \
		return; \
	} \
} \
static void \
bnfr_array_##_uctype_(StringInfo from, NodeFieldDesc desc, void *field, \
					  uint32 flags) \
{ \
	uint8		fldno; \
	int			arr_len; \
	int			arr_size; \
	_type_	   *arr; \
	\
	CHECK_FLD_IS_COMPATIBLE(desc); \
	CHECK_CAN_READ_FLD(from, sizeof(uint8), desc); \
	\
	fldno = from->data[from->cursor]; \
	\
	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \
	arr_size = arr_len * sizeof(_type_); \
	\
	if (fldno == desc->nfd_field_no) \
	{ \
		int		offset = from->cursor + sizeof(uint8); \
		\
		CHECK_CAN_READ_FLD(from, arr_size + sizeof(uint8), desc); \
		from->cursor = offset + arr_size;\
		arr = palloc(arr_size); \
		*(_type_ **) field = arr; \
		memcpy(arr, &from->data[offset], arr_size); \
		return; \
	} \
	else \
	{ \
		Assert(fldno > desc->nfd_field_no); \
		*(_type_ **) field = NULL; \
		return; \
	} \
}

static void
bnfr_unimplemented(StringInfo from, NodeFieldDesc desc,
				   void *field, uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);
	elog(ERROR, "Node field %s's type %02x deserialization is unimplemented",
		 desc->nfd_name, desc->nfd_type);
}

static void
bnfr_unknown(StringInfo from, NodeFieldDesc desc,
			 void *field, uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);
	elog(ERROR, "Node field %s is of an unknown type: %02x",
		 desc->nfd_name, desc->nfd_type);
}

BinaryScalarFieldReader(bool, BOOL, false)
BinaryScalarFieldReader(ParseLoc, PARSELOC, -1)
BinaryScalarFieldReader(int, INT, 0)
BinaryScalarFieldReader(int16, INT16, 0)
BinaryScalarFieldReader(int32, INT32, 0)
BinaryScalarFieldReader(long, LONG, 0)
BinaryScalarFieldReader(uint, UINT, 0)
BinaryScalarFieldReader(uint16, UINT16, 0)
BinaryScalarFieldReader(uint32, UINT32, 0)
BinaryScalarFieldReader(uint64, UINT64, 0)
BinaryScalarFieldReader(Oid, OID, 0)
BinaryScalarFieldReader(char, CHAR, 0)
BinaryScalarFieldReader(double, DOUBLE, 0.0)

#undef BinaryScalarFieldReader
#undef CHECK_CAN_READ
#undef CHECK_CAN_READ_FLD


#define tsfr(_type_) \
	[NFT_##_type_] = &bnfr_##_type_, \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &bnfr_array_##_type_

#define tsfr_unimpl(_type_) \
	[NFT_##_type_] = &bnfr_unimplemented

#define tsfr_unimpl_arr(_type_) \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &bnfr_unimplemented
#define tsfr_unknown_type(val) \
	[(val)] = &bnfr_unknown

const NodeReader BinaryNodeReader = &(NodeReaderData){
	.nr_read_tag = &bnr_read_tag,
	.nr_finish_node = &bnr_finish_node,
	.nr_fld_readers = {
		tsfr(BOOL),
		tsfr(PARSELOC),
		tsfr(INT),
		tsfr(INT16),
		tsfr(INT32),
		tsfr(LONG),
		tsfr(UINT),
		tsfr(UINT16),
		tsfr(UINT32),
		tsfr(UINT64),
		tsfr(OID),
		tsfr(CHAR),
		tsfr(DOUBLE),
		[NFT_ENUM] = &bnfr_INT, /* enum has the same layout as INT */
		[NFT_ENUM + NFT_ARRAYTYPE] = &bnfr_array_INT,
		tsfr(CSTRING),
		[NFT_BITMAPSET] = &bnfr_NODE,
		[NFT_BITMAPSET + NFT_ARRAYTYPE] = &bnfr_array_NODE,
		tsfr(NODE),
		tsfr_unimpl(PARAM_PATH_INFO),
		tsfr_unimpl_arr(PARAM_PATH_INFO),
		tsfr_unimpl(UNDEFINED),
		tsfr_unimpl(NUM_TYPES),
		/* fill with invalid entries */
		tsfr_unknown_type(NFT_INVALID_20),
		tsfr_unknown_type(NFT_INVALID_21),
		tsfr_unknown_type(NFT_INVALID_22),
		tsfr_unknown_type(NFT_INVALID_23),
		tsfr_unknown_type(NFT_INVALID_24),
		tsfr_unknown_type(NFT_INVALID_25),
		tsfr_unknown_type(NFT_INVALID_26),
		tsfr_unknown_type(NFT_INVALID_27),
		tsfr_unknown_type(NFT_INVALID_28),
		tsfr_unknown_type(NFT_INVALID_29),
		tsfr_unknown_type(NFT_INVALID_30),
		tsfr_unknown_type(NFT_INVALID_31),
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

	appendBinaryStringInfoNT(into, &tag, sizeof(uint16));
}

static bool
bnw_finish_node(StringInfo into, NodeDesc desc, int last_field,
				uint32 flags)
{
	uint8		final = PG_BINSER_NODE_END;

	Assert(last_field <= desc->nd_fields);
	CHECK_NODE_IS_COMPATIBLE(desc);

	appendBinaryStringInfoNT(into, &final, sizeof(uint8));

	return true;
}

static bool
bnfw_CSTRING(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	char		nil = '\0';

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (*(char **) field == NULL)
		return false;

	appendBinaryStringInfoNT(into, &desc->nfd_field_no, sizeof(uint8));
	appendStringInfoString(into, field);
	appendBinaryStringInfoNT(into, &nil, sizeof(char));

	return true;
}

static bool
bnfw_array_CSTRING(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	char		nil = '\0';
	int			arr_len;
	char	  **arr = *(char ***) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (arr == NULL)
		return false;

	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \

	appendBinaryStringInfoNT(into, &desc->nfd_field_no, sizeof(uint8));

	for (int i = 0; i < arr_len; i++)
	{
		appendStringInfoString(into, arr[i]);
		appendBinaryStringInfoNT(into, &nil, sizeof(char));
	}

	return true;
}

static bool
bnfw_NODE(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	Node	   *node = *(Node **) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (node == NULL)
		return false;

	appendBinaryStringInfoNT(into, &desc->nfd_field_no, sizeof(uint8));

	return WriteNode(into, node, BinaryNodeWriter, flags);
}

static bool
bnfw_array_NODE(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	int			arr_len;
	Node	  **arr = *(Node ***) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (arr == NULL)
		return false;

	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \

	appendBinaryStringInfoNT(into, &desc->nfd_field_no, sizeof(uint8));

	for (int i = 0; i < arr_len; i++)
	{
		WriteNode(into, arr[i], BinaryNodeWriter, flags);
	}

	return true;
}

static bool
bnfw_unimplemented(StringInfo into, NodeFieldDesc desc, void *field,
				   uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	return false;
}

static bool
bnfw_unknown(StringInfo into, NodeFieldDesc desc, void *field,
			 uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	return false;
}

#define BinaryScalarFieldWriter(_type_, _uctype_, type_default) \
static bool \
bnfw_##_uctype_(StringInfo into, NodeFieldDesc desc, void *field, \
				uint32 flags) \
{ \
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (*(_type_ *) field == (type_default)) \
		return false; \
	\
	appendBinaryStringInfoNT(into, &desc->nfd_field_no, sizeof(uint8)); \
	appendBinaryStringInfoNT(into, field, sizeof(_type_)); \
	\
	return true;\
} \
static bool \
bnfw_array_##_uctype_(StringInfo into, NodeFieldDesc desc, void *field, \
					  uint32 flags) \
{ \
	int arr_len, arr_size; \
	\
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (*(_type_ **) field == NULL) \
		return false; \
	\
	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \
	arr_size = arr_len * sizeof(_type_); \
	\
	appendBinaryStringInfoNT(into, &desc->nfd_field_no, sizeof(uint8));\
	appendBinaryStringInfoNT(into, *(void **) field, arr_size); \
	return true; \
}

BinaryScalarFieldWriter(bool, BOOL, false)
BinaryScalarFieldWriter(ParseLoc, PARSELOC, -1)
BinaryScalarFieldWriter(int, INT, 0)
BinaryScalarFieldWriter(int16, INT16, 0)
BinaryScalarFieldWriter(int32, INT32, 0)
BinaryScalarFieldWriter(long, LONG, 0)
BinaryScalarFieldWriter(uint, UINT, 0)
BinaryScalarFieldWriter(uint16, UINT16, 0)
BinaryScalarFieldWriter(uint32, UINT32, 0)
BinaryScalarFieldWriter(uint64, UINT64, 0)
BinaryScalarFieldWriter(Oid, OID, 0)
BinaryScalarFieldWriter(char, CHAR, 0)
BinaryScalarFieldWriter(double, DOUBLE, 0.0)

#define bsfw(_type_) \
	[NFT_##_type_] = &bnfw_##_type_, \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &bnfw_array_##_type_

#define bsfw_unimpl(_type_) \
	[NFT_##_type_] = &bnfw_unimplemented

#define bsfw_unimpl_arr(_type_) \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &bnfw_unimplemented
#define bsfw_unknown_type(val) \
	[(val)] = &bnfw_unknown

const NodeWriter BinaryNodeWriter = &(NodeWriterData){
	.nw_start_node = &bnw_start_node,
	.nw_finish_node = &bnw_finish_node,
	.nw_fld_writers = {
		bsfw(BOOL),
		bsfw(PARSELOC),
		bsfw(INT),
		bsfw(INT16),
		bsfw(INT32),
		bsfw(LONG),
		bsfw(UINT),
		bsfw(UINT16),
		bsfw(UINT32),
		bsfw(UINT64),
		bsfw(OID),
		bsfw(CHAR),
		bsfw(DOUBLE),
		[NFT_ENUM] = &bnfw_INT, /* enum has the same layout as INT */
		[NFT_ENUM + NFT_ARRAYTYPE] = &bnfw_array_INT,
		bsfw(CSTRING),
		[NFT_BITMAPSET] = &bnfw_NODE,
		[NFT_BITMAPSET + NFT_ARRAYTYPE] = &bnfw_array_NODE,
		bsfw(NODE),
		bsfw_unimpl(PARAM_PATH_INFO),
		bsfw_unimpl_arr(PARAM_PATH_INFO),
		bsfw_unimpl(UNDEFINED),
		bsfw_unimpl(NUM_TYPES),
		/* fill with invalid entries */
		bsfw_unknown_type(NFT_INVALID_20),
		bsfw_unknown_type(NFT_INVALID_21),
		bsfw_unknown_type(NFT_INVALID_22),
		bsfw_unknown_type(NFT_INVALID_23),
		bsfw_unknown_type(NFT_INVALID_24),
		bsfw_unknown_type(NFT_INVALID_25),
		bsfw_unknown_type(NFT_INVALID_26),
		bsfw_unknown_type(NFT_INVALID_27),
		bsfw_unknown_type(NFT_INVALID_28),
		bsfw_unknown_type(NFT_INVALID_29),
		bsfw_unknown_type(NFT_INVALID_30),
		bsfw_unknown_type(NFT_INVALID_31),
	}
};
