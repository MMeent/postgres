/*-------------------------------------------------------------------------
 *
 * ndio_text.c
 *		Implementation for textual (de)serialization using NodeDesc-based
 *		infrastructure.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/ndio_text.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "nodes/ndio.h"
#include "nodes/nodes.h"
#include "nodes/nodedesc.h"
#include "nodes/readfuncs.h"
#include "nodes/replnodes.h"
#include "common/shortest_dec.h"
#include "fmgr.h"
#include "varatt.h"


#define CHECK_CAN_READ(info, length, ...) \
	if ((info)->len - (info)->cursor < (length)) \
		ereport(PANIC, \
				errbacktrace(), \
				errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),\
				errmsg_internal(__VA_ARGS__))

#define CHECK_CAN_READ_FLD(info, length, desc) \
	CHECK_CAN_READ(info, length, "Error reading field %s: out of data", (desc)->nfd_name)

static inline void
skip_whitespace(StringInfo from)
{
	int			cursor = from->cursor;
	char	   *ptr = from->data + cursor;
	while (cursor < from->len && (*ptr == ' ' || *ptr == '\t' ||
								  *ptr == '\n'))
	{
		cursor++;
		ptr++;
	}

	from->cursor = cursor;
}

static char *
nullable_string(const char *token, int length)
{
	/* outToken emits <> for NULL, and pg_strtok makes that an empty string */
	if (length == 0)
		return NULL;
	/* outToken emits "" for empty string */
	if (length == 2 && token[0] == '"' && token[1] == '"')
		return pstrdup("");
	/* otherwise, we must remove protective backslashes added by outToken */
	return debackslash(token, length);
}

static const char *
read_next_token(StringInfo from, int *len)
{
	char	   *local_str;
	char	   *ret_str;
	const char *limit = from->data + from->len;

	skip_whitespace(from);

	local_str = from->data + from->cursor;

	if (*local_str == '\0')
	{
		*len = 0;
		from->cursor = from->len;
		return NULL;
	}

	ret_str = local_str;

	while ((*local_str != '\0' &&
			*local_str != ' ' && *local_str != '\n' &&
			*local_str != '\t' &&
			*local_str != '(' && *local_str != ')' &&
			*local_str != '{' && *local_str != '}') &&
			local_str < limit)
	{
		if (*local_str == '\\' && local_str[1] != '\0')
			local_str += 2;
		else
			local_str++;
	}
	if (local_str == limit)
	{
		ereport(PANIC,
				errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				errmsg_internal("Unexpected EOF while reading Node data"));
	}

	*len = (local_str - ret_str);
	from->cursor = (local_str - from->data);

	return ret_str;
}

static inline void
CHECK_FLD_IS_COMPATIBLE(NodeFieldDesc fdesc)
{
	if (fdesc->nfd_flags & NODEDESC_DISABLE_WRITE)
		Assert(fdesc->nfd_flags & NODEDESC_DISABLE_READ);
}

static inline void
CHECK_NODE_IS_COMPATIBLE(NodeDesc desc)
{
	if (desc->nd_flags & NODEDESC_DISABLE_WRITE)
		Assert(desc->nd_flags & NODEDESC_DISABLE_READ);
}

static bool
next_node_field_is(StringInfo from, NodeFieldDesc desc)
{
	skip_whitespace(from);

	/* next up is not a field (which would be indicated by ":fieldname ") */
	if (from->len < from->cursor + 2 + desc->nfd_namelen ||
		from->data[from->cursor] != ':' ||
		strncmp(from->data + from->cursor + 1,
				desc->nfd_name,
				desc->nfd_namelen) != 0 ||
		from->data[from->cursor + 1 + desc->nfd_namelen] != ' ')
	{
		return false;
	}
	from->cursor += 2 + desc->nfd_namelen;
	return true;
}

static inline void
skip_past_next_token_character(StringInfo from, char expect)
{
	char	   *ptr;

	skip_whitespace(from);

	if (from->len <= from->cursor)
		ereport(PANIC,
				errbacktrace(),
				errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				errmsg_internal("Unexpected EOF while deserializing Node data"));

	ptr = from->data + from->cursor;

	if (*ptr != expect)
		ereport(PANIC,
				errbacktrace(),
				errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				errmsg_internal("Unexpected character while deserializing nodes"),
				errdetail_internal("Expected character %02x, but got %02x",
								   expect, *ptr));

	from->cursor += sizeof(char);
}

/*---
 * Textual Node Reader
 *		Reader for a text-based node serialization format.
 *
 * The serialization format for a normal Node is
 *		{NODETYPE :fieldname value ...}
 */

static bool
tnr_read_tag(StringInfo from, uint32 flags, NodeTag *out)
{
	int			cursor = from->cursor;
	char	   *name_start;
	char	   *name_end;
	int			name_len;
	NodeDesc	desc;

	CHECK_CAN_READ(from, 2, "Out of data when reading node tag");

	skip_whitespace(from);

	/* "<>", thus NULL */
	if (from->data[cursor] == '<' && from->data[cursor + 1] == '>')
	{
		from->cursor += 2;
		*out = T_Invalid;
		return false;
	}

	skip_past_next_token_character(from, '{');

	cursor = from->cursor;

	name_start = name_end = &from->data[cursor];

	while (*name_end != ' ' && *name_end != '\0' &&
		   *name_end != '\t' && *name_end != '\n' &&
		   *name_end != '}' && name_end < from->data + from->len)
		name_end++;

	if (name_end == name_start)
		ereport(PANIC,
				errbacktrace(),
				errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				errmsg_internal("Unexpected EOF while deserializing Node data"));

	name_len = name_end - name_start;

	desc = GetNodeDescByNodeName(name_start, name_len);

	if (desc == NULL)
	{
		char	buf[NAMEDATALEN];
		strncpy(buf, name_start, Min(name_len, NAMEDATALEN - 1));

		ereport(PANIC,
				errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				errbacktrace(),
				errmsg_internal("Unknown node type name"),
				errdetail_internal("Node type name %s is not registered", buf));
	}

	*out = desc->nd_nodetag;
	from->cursor += name_len;

	return true;
}

static void
tnr_finish_node(StringInfo from, NodeDesc desc, int final_field,
				uint32 flags)
{
	skip_past_next_token_character(from, '}');
}

static bool
tnr_start_field(StringInfo from, NodeFieldDesc desc, uint32 flags)
{
	return next_node_field_is(from, desc);
}

static bool
tnr_start_array(StringInfo from, uint32 flags)
{
	char	   *ptr;

	skip_whitespace(from);

	ptr = from->data + from->cursor;

	/* both NULL and begin+end take up 2 bytes minimum */
	CHECK_CAN_READ(from, 2, "Out of data while reading array start");

	/* "<>", thus NULL */
	if (*ptr == '<' && *(ptr + 1) == '>')
	{
		from->cursor += 2;
		return false;
	}

	skip_past_next_token_character(from, '(');

	return true;
}

static void
tnr_array_value_separator(StringInfo from, uint32 flags)
{
	skip_whitespace(from);
}

static void
tnr_end_array(StringInfo from, uint32 flags)
{
	skip_past_next_token_character(from, ')');
}

static void
tnr_end_field(StringInfo from, uint32 flags)
{
	return;
}

static bool
tnvr_VARLENA(StringInfo from, void *field, uint32 flags)
{
	struct varlena **tfield = (struct varlena **) field;
	const char *data_start;
	char	   *vardata_tail;
	uint32		dat_len;
	int			len;
	int			i = 0;
	struct varlena *data;

	skip_past_next_token_character(from, '(');

	data_start = read_next_token(from, &len);
	dat_len = strtoul(data_start, NULL, 10);

	Assert(dat_len >= VARHDRSZ);
	data = palloc(dat_len);
	SET_VARSIZE(data, dat_len);
	dat_len -= VARHDRSZ;
	vardata_tail = VARDATA(data);

//	for (i + (sizeof(uint32) - 1) < dat_len; i += sizeof(uint32))
//	{
//		uint32	value;
//		data_start = read_next_token(from, &len);
//		value = strtoul(data_start, NULL, 10);
//		memcpy(vardata_tail, &value, sizeof(uint32));
//		vardata_tail += sizeof(uint32);
//	}

	for (i = 0; i < dat_len; i++)
	{
		uint8	value;
		data_start = read_next_token(from, &len);
		value = strtoul(data_start, NULL, 10);

		memcpy(vardata_tail, &value, sizeof(uint8));
		vardata_tail += sizeof(uint8);
	}

	skip_past_next_token_character(from, ')');

	*tfield = data;
	return true;
}

static bool
tnfr_VARLENA(StringInfo from, NodeFieldDesc desc,
				 void *field, uint32 flags)
{
	char	  **tfield = (char **) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!next_node_field_is(from, desc))
	{
		*tfield = NULL;
		return false;
	}

	return tnvr_VARLENA(from, field, flags);
}


static bool
tnvr_CSTRING(StringInfo from, void *field, uint32 flags)
{
	char	  **tfield = (char **) field;
	const char *data_start;
	int			len;

	data_start = read_next_token(from, &len);

	*tfield = nullable_string(data_start, len);
	return true;
}

static bool
tnfr_CSTRING(StringInfo from, NodeFieldDesc desc,
			 void *field, uint32 flags)
{
	char	  **tfield = (char **) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!next_node_field_is(from, desc))
	{
		*tfield = NULL;
		return false;
	}

	return tnvr_CSTRING(from, field, flags);
}

static bool
tnvr_NODE(StringInfo from, void *field, uint32 flags)
{
	Node	  **tfield = (Node **) field;

	*tfield = ReadNode(from, TextNodeReader, flags);

	return true;
}

static bool
tnfr_NODE(StringInfo from, NodeFieldDesc desc,
		  void *field, uint32 flags)
{
	Node	  **tfield = (Node **) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!next_node_field_is(from, desc))
	{
		*tfield = NULL;
		return false;
	}

	return tnvr_NODE(from, field, flags);
}

#define TextScalarFieldReader(_type_, _uctype_, type_default, parseTok) \
static bool \
tnvr_##_uctype_(StringInfo from, void *field, uint32 flags) \
{ \
	_type_	   *tfield = (_type_ *) field; \
	const char *data_start; \
	int			len; \
	\
	data_start = read_next_token(from, &len); \
	\
	*tfield = parseTok(data_start); \
	return true; \
} \
static bool \
tnfr_##_uctype_(StringInfo from, NodeFieldDesc desc, void *field, \
				uint32 flags) \
{ \
	_type_	   *tfield = (_type_ *) field; \
	\
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (!next_node_field_is(from, desc)) \
	{ \
		*tfield = type_default; \
		return false; \
	} \
	\
	return tnvr_##_uctype_(from, field, flags); \
}

static bool
tnfr_unimplemented(StringInfo from, NodeFieldDesc desc,
				   void *field, uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	Assert(false);

	elog(PANIC, "Node field %s's type %02x deserialization is unimplemented",
		 desc->nfd_name, desc->nfd_type);
}
static bool
tnvr_unimplemented(StringInfo from, void *field, uint32 flags)
{
	Assert(false);

	elog(PANIC, "Unimplemented deserialization type");
}

#define strtobool(x)	((*(x) == 't') ? true : false)
#define atoui(x) 		((unsigned int) strtoul((x), NULL, 10))
#define atou64(x)		(strtou64((x), NULL, 10))
#define atoc(x)			((len == 0) ? '\0' : ((x)[0] == '\\' ? (x)[1] : (x)[0]))

TextScalarFieldReader(bool, BOOL, false, strtobool)
TextScalarFieldReader(ParseLoc, PARSELOC, -1, atoi)
TextScalarFieldReader(TypMod, TYPMOD, -1, atoi)
TextScalarFieldReader(int, INT, 0, atoi)
TextScalarFieldReader(int16, INT16, 0, atoi)
TextScalarFieldReader(int32, INT32, 0, atoi)
TextScalarFieldReader(long, LONG, 0, atol)
TextScalarFieldReader(uint8, UINT8, 0, atoui)
TextScalarFieldReader(uint16, UINT16, 0, atoui)
TextScalarFieldReader(uint32, UINT32, 0, atoui)
TextScalarFieldReader(uint64, UINT64, 0, atou64)
TextScalarFieldReader(Oid, OID, 0, atooid)
TextScalarFieldReader(char, CHAR, 0, atoc)
TextScalarFieldReader(double, DOUBLE, 0.0, atof)

#undef TextScalarFieldReader
#undef CHECK_CAN_READ
#undef CHECK_CAN_READ_FLD

#define tsfr(_type_) \
	[NFT_##_type_] = tnfr_##_type_

#define tsfr_unimpl(_type_) \
	[NFT_##_type_] = tnfr_unimplemented

#define tsvr(_type_) \
	[NFT_##_type_] = tnvr_##_type_

#define tsvr_unimpl(_type_) \
	[NFT_##_type_] = tnvr_unimplemented

const NodeReader TextNodeReader = &(NodeReaderData){
	.nr_read_tag = tnr_read_tag,
	.nr_finish_node = tnr_finish_node,
	.nr_start_field = tnr_start_field,
	.nr_start_array = tnr_start_array,
	.nr_array_value_separator = tnr_array_value_separator,
	.nr_end_array = tnr_end_array,
	.nr_end_field = tnr_end_field,
	.nr_fld_readers = {
		tsfr_unimpl(UNDEFINED),
		tsfr(BOOL),
		tsfr(PARSELOC),
		tsfr(TYPMOD),
		tsfr(INT),
		tsfr(INT16),
		tsfr(INT32),
		tsfr(LONG),
		tsfr(UINT8),
		tsfr(UINT16),
		tsfr(UINT32),
		tsfr(UINT64),
		tsfr(OID),
		tsfr(CHAR),
		tsfr(DOUBLE),
		[NFT_ENUM] = &tnfr_INT, /* enum has the same layout as INT */
		tsfr(VARLENA),
		tsfr(CSTRING),
		tsfr(NODE),
	},
	.nr_val_readers = {
		tsvr_unimpl(UNDEFINED),
		tsvr(BOOL),
		tsvr(PARSELOC),
		tsvr(TYPMOD),
		tsvr(INT),
		tsvr(INT16),
		tsvr(INT32),
		tsvr(LONG),
		tsvr(UINT8),
		tsvr(UINT16),
		tsvr(UINT32),
		tsvr(UINT64),
		tsvr(OID),
		tsvr(CHAR),
		tsvr(DOUBLE),
		[NFT_ENUM] = &tnvr_INT, /* enum has the same layout as INT */
		tsvr(VARLENA),
		tsvr(CSTRING),
		tsvr(NODE),
	},
};

#undef tsfr
#undef tsfr_unimpl
#undef tsfr_unimpl_arr
#undef tsfr_unknown_type


/*---
 * Binary Node Writer
 *		Writer for a binary node serialization format.
 */

static inline void
appendFieldName(StringInfo into, NodeFieldDesc desc)
{
	appendStringInfoCharMacro(into, ' ');
	appendStringInfoCharMacro(into, ':');
	appendBinaryStringInfo(into, desc->nfd_name, desc->nfd_namelen);
	appendStringInfoCharMacro(into, ' ');
}

/*
 * Convert a double value, attempting to ensure the value is preserved exactly.
 */
static void
outDouble(StringInfo str, double d)
{
	char		buf[DOUBLE_SHORTEST_DECIMAL_LEN];

	double_to_shortest_decimal_buf(d, buf);
	appendStringInfoString(str, buf);
}

/*
 * Convert one char.  Goes through outToken() so that special characters are
 * escaped.
 */
static void
outChar(StringInfo str, char c)
{
	char		in[2];

	/* Traditionally, we've represented \0 as <>, so keep doing that */
	if (c == '\0')
	{
		appendStringInfoString(str, "<>");
		return;
	}

	in[0] = c;
	in[1] = '\0';

	outToken(str, in);
}

#define outbool(into, value) \
	appendStringInfoString((into), (value) ? "true" : "false")

static void
tnw_start_node(StringInfo into, NodeDesc desc, uint32 flags)
{
	appendStringInfoCharMacro(into, '{');
	appendBinaryStringInfo(into, desc->nd_name, desc->nd_namelen);
}

static bool
tnw_finish_node(StringInfo into, NodeDesc desc, int last_field,
				uint32 flags)
{
	appendStringInfoCharMacro(into, '}');
	return true;
}

static void
tnw_write_null(StringInfo into, uint32 flags)
{
	appendStringInfoString(into, "<>");
}

static void
tnw_start_field(StringInfo into, NodeFieldDesc desc, uint32 flags)
{
	appendStringInfoCharMacro(into, ' ');
	appendStringInfoCharMacro(into, ':');
	appendBinaryStringInfo(into, desc->nfd_name, desc->nfd_namelen);
	appendStringInfoCharMacro(into, ' ');
}

static void
tnw_start_array(StringInfo into, uint32 flags)
{
	appendStringInfoCharMacro(into, '(');
}

static void
tnw_field_entry_separator(StringInfo into, uint32 flags)
{
	appendStringInfoCharMacro(into, ' ');
}

static void
tnw_end_array(StringInfo into, uint32 flags)
{
	appendStringInfoCharMacro(into, ')');
}

static void
tnw_end_field(StringInfo into, uint32 flags)
{
	/* no-op */
}

static bool
tnvw_VARLENA(StringInfo into, const void *field, uint32 flags)
{
	struct varlena *vlna = *(struct varlena **) field;
	struct varlena *vlnb;
	const char	   *tail;
	uint32			vl_len_;
	int				i = 0;

	Assert(vlna != NULL);

	vlnb = pg_detoast_datum(vlna);
	vl_len_ = VARSIZE_ANY_EXHDR(vlnb);

	Assert(vl_len_ >= 0);

	appendStringInfo(into, "(%u", vl_len_ + VARHDRSZ);
	tail = VARDATA_ANY(vlnb);

//	for (i + (sizeof(uint32) - 1) < vl_len_; i += sizeof(uint32))
//	{
//		uint32	value;
//		memcpy(&value, tail, sizeof(uint32));
//		appendStringInfo(into, " %u", value);
//		tail += sizeof(uint32);
//	}

	for (; i < vl_len_; i++)
	{
		uint8	value = *(uint8 *) (tail++);
		appendStringInfo(into, " %u", value);
	}

	appendStringInfoCharMacro(into, ')');

	if (vlna != vlnb)
		pfree(vlnb);

	return true;
}

static bool
tnfw_VARLENA(StringInfo into, NodeFieldDesc desc, const void *field,
				 uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	if ((*(const void **) field) == NULL)
		return false;

	appendFieldName(into, desc);

	return tnvw_VARLENA(into, field, flags);
}

static bool
tnvw_CSTRING(StringInfo into, const void *field, uint32 flags)
{
	const char *value = *(const char **) field;

	outToken(into, value);

	return true;
}

static bool
tnfw_CSTRING(StringInfo into, NodeFieldDesc desc, const void *field,
			 uint32 flags)
{

	CHECK_FLD_IS_COMPATIBLE(desc);

	if ((*(void **) field) == NULL)
		return false;

	appendFieldName(into, desc);

	return tnvw_CSTRING(into, field, flags);
}

static bool
tnvw_NODE(StringInfo into, const void *field, uint32 flags)
{
	Node	   *node = *(Node **) field;

	return WriteNode(into, node, TextNodeWriter, flags);
}

static bool
tnfw_NODE(StringInfo into, NodeFieldDesc desc, const void *field,
		  uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	if (*(Node **) field == NULL)
		return false;

	appendFieldName(into, desc);

	return tnvw_NODE(into, field, flags);
}

static bool
tnfw_unimplemented(StringInfo into, NodeFieldDesc desc, const void *field,
				   uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	return false;
}

static bool
tnvw_unimplemented(StringInfo into, const void *field,
				   uint32 flags)
{
	return false;
}

#define TextScalarFieldWriter(_type_, _uctype_, type_default, write_value) \
static bool \
tnvw_##_uctype_(StringInfo into, const void *field, uint32 flags) \
{ \
	_type_		value = *(_type_ *) field; \
	\
	write_value; \
	\
	return true;\
} \
static bool \
tnfw_##_uctype_(StringInfo into, NodeFieldDesc desc, const void *field, \
				uint32 flags) \
{ \
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (*(_type_ *) field == (type_default)) \
		return false; \
	\
	appendFieldName(into, desc); \
	\
	return tnvw_##_uctype_(into, field, flags); \
}


#define format(fmt) appendStringInfo(into, fmt, value)
#define writeout(fnc) fnc(into, value)

TextScalarFieldWriter(bool, BOOL, false, writeout(outbool))
TextScalarFieldWriter(char, CHAR, 0, writeout(outChar))
TextScalarFieldWriter(double, DOUBLE, 0.0, writeout(outDouble))

TextScalarFieldWriter(ParseLoc, PARSELOC, -1, format("%d"))
TextScalarFieldWriter(TypMod, TYPMOD, -1, format("%d"))
TextScalarFieldWriter(int, INT, 0, format("%d"))
TextScalarFieldWriter(int16, INT16, 0, format("%d"))
TextScalarFieldWriter(int32, INT32, 0, format("%d"))
TextScalarFieldWriter(long, LONG, 0, format("%ld"))
TextScalarFieldWriter(uint8, UINT8, 0, format("%u"))
TextScalarFieldWriter(uint16, UINT16, 0, format("%u"))
TextScalarFieldWriter(uint32, UINT32, 0, format("%u"))
TextScalarFieldWriter(uint64, UINT64, 0, format(UINT64_FORMAT))
TextScalarFieldWriter(Oid, OID, 0, format("%u"))

#define tsfw(_type_) \
	[NFT_##_type_] = tnfw_##_type_

#define tsfw_unimpl(_type_) \
	[NFT_##_type_] = tnfw_unimplemented

#define tsvw(_type_) \
	[NFT_##_type_] = tnvw_##_type_

#define tsvw_unimpl(_type_) \
	[NFT_##_type_] = tnvw_unimplemented


const NodeWriter TextNodeWriter = &(NodeWriterData){
	.nw_start_node = tnw_start_node,
	.nw_finish_node = tnw_finish_node,
	.nw_write_null = tnw_write_null,
	.nw_start_field = tnw_start_field,
	.nw_start_array = tnw_start_array,
	.nw_field_entry_separator = tnw_field_entry_separator,
	.nw_end_array = tnw_end_array,
	.nw_end_field = tnw_end_field,
	.nw_fld_writers = {
		tsfw_unimpl(UNDEFINED),
		tsfw(BOOL),
		tsfw(PARSELOC),
		tsfw(TYPMOD),
		tsfw(INT),
		tsfw(INT16),
		tsfw(INT32),
		tsfw(LONG),
		tsfw(UINT8),
		tsfw(UINT16),
		tsfw(UINT32),
		tsfw(UINT64),
		tsfw(OID),
		tsfw(CHAR),
		tsfw(DOUBLE),
		[NFT_ENUM] = &tnfw_INT, /* enum has the same layout as INT */
		tsfw(VARLENA),
		tsfw(CSTRING),
		tsfw(NODE),
	},
	.nw_val_writers = {
		tsvw_unimpl(UNDEFINED),
		tsvw(BOOL),
		tsvw(PARSELOC),
		tsvw(TYPMOD),
		tsvw(INT),
		tsvw(INT16),
		tsvw(INT32),
		tsvw(LONG),
		tsvw(UINT8),
		tsvw(UINT16),
		tsvw(UINT32),
		tsvw(UINT64),
		tsvw(OID),
		tsvw(CHAR),
		tsvw(DOUBLE),
		[NFT_ENUM] = &tnvw_INT, /* enum has the same layout as INT */
		tsvw(VARLENA),
		tsvw(CSTRING),
		tsvw(NODE),
	}
};

#undef tsfw
#undef tsfw_unimpl
#undef tsfw_unimpl_arr
#undef tsfw_unknown_type
