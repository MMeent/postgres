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

#include "nodes/nodes.h"
#include "nodes/nodedesc.h"
#include "nodes/plannodes.h"
#include "nodes/readfuncs.h"
#include "nodes/replnodes.h"
#include "common/shortest_dec.h"


#define CHECK_CAN_READ(info, length, ...) \
	if ((info)->len - (info)->cursor < (length)) \
		elog(ERROR, __VA_ARGS__)

#define CHECK_CAN_READ_FLD(info, length, desc) \
	CHECK_CAN_READ(info, length, "Error reading field %s: out of data", (desc)->nfd_name)

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
	char	   *local_str = from->data;
	char	   *ret_str;

	while (*local_str == ' ' || *local_str == '\n' || *local_str == '\t')
		local_str++;

	if (*local_str == '\0')
	{
		*len = 0;
		from->cursor = from->len;
		return NULL;
	}

	ret_str = local_str;

	while (*local_str != '\0' &&
		   *local_str != ' ' && *local_str != '\n' &&
		   *local_str != '\t' &&
		   *local_str != '(' && *local_str != ')' &&
		   *local_str != '{' && *local_str != '}')
	{
		if (*local_str == '\\' && local_str[1] != '\0')
			local_str += 2;
		else
			local_str++;
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
	while (from->data[from->cursor] == ' ' && from->cursor < from->len)
		from->cursor++;
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
	int			cursor = from->cursor;
	char	   *ptr = from->data + cursor;

	while (cursor < from->len && (*ptr == ' ' || *ptr == '\t' || *ptr == '\n'))
	{
		ptr++;
		cursor++;
	}
	
	if (*ptr != expect)
		elog(ERROR, "Unexpected character %02x, expected %02x",
			 *ptr, expect);
	cursor++;
	from->cursor = cursor;
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

	if (from->len - cursor <= 2)
		elog(ERROR, "Out of data when reading node");

	/* "<>", thus NULL */
	if (from->data[cursor] == '<' && from->data[cursor + 1] == '>')
	{
		from->cursor += 2;
		*out = T_Invalid;
		return false;
	}
	
	if (from->data[cursor] != '{')
	{
		elog(ERROR, "Unexpected character '%c' at start of node",
			 from->data[cursor]);
	}

	cursor++;
	name_start = name_end = &from->data[cursor];

	while (*name_end != ' ' && *name_end != '\0' &&
		   *name_end != '\t' && *name_end != '\n' &&
		   *name_end != '}')
		name_end++;

	if (name_end == name_start)
		elog(ERROR, "Can't read type name at start of node");

	name_len = name_end - name_start;

	desc = GetNodeDescByNodeName(name_start, name_len);

	if (desc == NULL)
		elog(ERROR, "Unknown node type name");

	*out = desc->nd_nodetag;
	from->cursor += name_len;
	return true;
}

static void
tnr_finish_node(StringInfo from, NodeDesc desc,
				uint32 flags)
{
	char		final;

	CHECK_CAN_READ(from, sizeof(char), "Out of data finishing a node");
	final = from->data[from->cursor];

	if (unlikely(final != '}'))
		elog(ERROR, "Unexpected byte %02x at end of node %s during deserialization",
			 final, desc->nd_name);

	from->cursor += sizeof(char);
}

static void
tnfr_CSTRING(StringInfo from, NodeFieldDesc desc,
			 void *field, uint32 flags)
{
	char	  **tfield = (char **) field;
	const char *data_start;
	int			len;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!next_node_field_is(from, desc))
	{
		*tfield = NULL;
		return;
	}

	data_start = read_next_token(from, &len);

	*tfield = nullable_string(data_start, len);
}

static void
tnfr_array_CSTRING(StringInfo from, NodeFieldDesc desc,
				   void *field, uint32 flags)
{
	int			n = *(int *) (((char *) field) + desc->nfd_arr_len_off);
	char	 ***tfield = (char ***) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!next_node_field_is(from, desc))
	{
		*tfield = NULL;
		return;
	}
	*tfield = palloc(sizeof(char *) * n);

	skip_past_next_token_character(from, '(');
	for (int i = 0; i < n; i++)
	{
		int len;
		const char *data_start;
		if (from->cursor == from->len)
			elog(ERROR, "Out of data while reading Node field %s",
				 desc->nfd_name);

		data_start = read_next_token(from, &len);

		(*tfield)[i] = nullable_string(data_start, len);
	}
	skip_past_next_token_character(from, ')');
}

static void
tnfr_NODE(StringInfo from, NodeFieldDesc desc,
		  void *field, uint32 flags)
{
	Node	  **tfield = (Node **) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!next_node_field_is(from, desc))
	{
		*tfield = NULL;
		return;
	}

	*tfield = ReadNode(from, TextNodeReader, flags);
}

static void
tnfr_array_NODE(StringInfo from, NodeFieldDesc desc,
				void *field, uint32 flags)
{
	int			n = *(int *) (((char *) field) + desc->nfd_arr_len_off);
	Node	 ***tfield = (Node ***) field;
	Node	  **arr;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (!next_node_field_is(from, desc))
	{
		*tfield = NULL;
		return;
	}

	*tfield = arr = palloc(sizeof(Node *) * n);

	skip_past_next_token_character(from, '(');
	for (int i = 0; i < n; i++)
	{
		if (from->cursor == from->len)
			elog(ERROR, "Out of data while reading Node field %s",
				 desc->nfd_name);

		arr[i] = ReadNode(from, TextNodeReader, flags);
	}
	skip_past_next_token_character(from, ')');
}


#define TextScalarFieldReader(_type_, _uctype_, type_default, parseTok) \
static void \
tnfr_##_uctype_(StringInfo from, NodeFieldDesc desc, void *field, \
				uint32 flags) \
{ \
	_type_	   *tfield = (_type_ *) field; \
	const char *data_start; \
	int			len; \
	\
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (!next_node_field_is(from, desc)) \
	{ \
		*tfield = type_default; \
		return; \
	} \
	data_start = read_next_token(from, &len); \
	\
	*tfield = parseTok(data_start); \
} \
static void \
tnfr_array_##_uctype_(StringInfo from, NodeFieldDesc desc, void *field, \
					  uint32 flags) \
{ \
	_type_	  **tfield = (_type_ **) field; \
	int			arr_len; \
	int			arr_size; \
	_type_	   *arr; \
	\
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (!next_node_field_is(from, desc)) \
	{ \
		*tfield = NULL; \
		return; \
	} \
	\
	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \
	arr_size = arr_len * sizeof(_type_); \
	*tfield = arr = palloc(arr_size); \
	\
	skip_past_next_token_character(from, '('); \
	for (int i = 0; i < arr_len; i++) \
	{ \
		int		len; \
		const char *data_start; \
		if (from->cursor == from->len) \
			elog(ERROR, "Out of data while reading Node field %s", \
				 desc->nfd_name); \
		\
		data_start = read_next_token(from, &len); \
		\
		arr[i] = parseTok(data_start); \
	} \
	skip_past_next_token_character(from, ')'); \
}

static void
tnfr_unimplemented(StringInfo from, NodeFieldDesc desc,
				   void *field, uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);
	elog(ERROR, "Node field %s's type %02x deserialization is unimplemented",
		 desc->nfd_name, desc->nfd_type);
}

static void
tnfr_unknown(StringInfo from, NodeFieldDesc desc,
			 void *field, uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);
	elog(ERROR, "Node field %s is of an unknown type: %02x",
		 desc->nfd_name, desc->nfd_type);
}

#define strtobool(x)	((*(x) == 't') ? true : false)
#define atoui(x) 		((unsigned int) strtoul((x), NULL, 10))
#define atou64(x)		(strtou64((x), NULL, 10))
#define atoc(x)			((len == 0) ? '\0' : ((x)[0] == '\\' ? (x)[1] : (x)[0]))

TextScalarFieldReader(bool, BOOL, false, strtobool)
TextScalarFieldReader(ParseLoc, PARSELOC, -1, atoi)
TextScalarFieldReader(int, INT, 0, atoi)
TextScalarFieldReader(int16, INT16, 0, atoi)
TextScalarFieldReader(int32, INT32, 0, atoi)
TextScalarFieldReader(long, LONG, 0, atol)
TextScalarFieldReader(uint, UINT, 0, atoui)
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
	[NFT_##_type_] = &tnfr_##_type_, \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &tnfr_array_##_type_

#define tsfr_unimpl(_type_) \
	[NFT_##_type_] = &tnfr_unimplemented

#define tsfr_unimpl_arr(_type_) \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &tnfr_unimplemented
#define tsfr_unknown_type(val) \
	[(val)] = &tnfr_unknown

const NodeReader TextNodeReader = &(NodeReaderData){
	.nr_read_tag = &tnr_read_tag,
	.nr_finish_node = &tnr_finish_node,
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
		[NFT_ENUM] = &tnfr_INT, /* enum has the same layout as INT */
		[NFT_ENUM + NFT_ARRAYTYPE] = &tnfr_array_INT,
		tsfr(CSTRING),
		[NFT_BITMAPSET] = &tnfr_NODE,
		[NFT_BITMAPSET + NFT_ARRAYTYPE] = &tnfr_array_NODE,
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

static inline void
appendFieldName(StringInfo into, NodeFieldDesc desc)
{
	appendStringInfoCharMacro(into, ' ');
	appendStringInfoCharMacro(into, ':');
	appendBinaryStringInfoNT(into, desc->nfd_name, desc->nfd_namelen);
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
	appendBinaryStringInfoNT(into, desc->nd_name, desc->nd_namelen);
}

static bool
tnw_finish_node(StringInfo into, NodeDesc desc, int last_field,
				uint32 flags)
{
	appendStringInfoCharMacro(into, '}');
	return true;
}

static bool
tnfw_CSTRING(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	char	  **tfield = field;
	char	   *value = *tfield;

	CHECK_FLD_IS_COMPATIBLE(desc);

	appendFieldName(into, desc);
	outToken(into, value);

	return true;
}

static bool
tnfw_array_CSTRING(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	int			arr_len;
	char	  **arr = *(char ***) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (arr == NULL)
		return false;

	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \

	appendFieldName(into, desc);

	appendStringInfoCharMacro(into, '(');
	for (int i = 0; i < arr_len; i++)
	{
		if (i != 0)
			appendStringInfoCharMacro(into, ' ');
		outToken(into, arr[i]);
	}
	appendStringInfoCharMacro(into, ')');

	return true;
}

static bool
tnfw_NODE(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	Node	   *node = *(Node **) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (node == NULL)
		return false;

	appendFieldName(into, desc);

	return WriteNode(into, node, TextNodeWriter, flags);
}

static bool
tnfw_array_NODE(StringInfo into, NodeFieldDesc desc, void *field, uint32 flags)
{
	int			arr_len;
	Node	  **arr = *(Node ***) field;

	CHECK_FLD_IS_COMPATIBLE(desc);

	if (arr == NULL)
		return false;

	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \

	appendFieldName(into, desc);

	appendStringInfoCharMacro(into, '(');
	for (int i = 0; i < arr_len; i++)
	{
		if (i != 0)
			appendStringInfoCharMacro(into, ' ');
		WriteNode(into, arr[i], TextNodeWriter, flags);
	}
	appendStringInfoCharMacro(into, ')');

	return true;
}

static bool
tnfw_unimplemented(StringInfo into, NodeFieldDesc desc, void *field,
				   uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	return false;
}

static bool
tnfw_unknown(StringInfo into, NodeFieldDesc desc, void *field,
			 uint32 flags)
{
	CHECK_FLD_IS_COMPATIBLE(desc);

	return false;
}

#define TextScalarFieldWriter(_type_, _uctype_, type_default, write_value) \
static bool \
tnfw_##_uctype_(StringInfo into, NodeFieldDesc desc, void *field, \
				uint32 flags) \
{ \
	_type_		value; \
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (*(_type_ *) field == (type_default)) \
		return false; \
	\
	appendFieldName(into, desc); \
	value = *(_type_ *) field; \
	write_value; \
	\
	return true;\
} \
static bool \
tnfw_array_##_uctype_(StringInfo into, NodeFieldDesc desc, void *field, \
					  uint32 flags) \
{ \
	int			arr_len; \
	_type_	  *t_arr = *(_type_ **) field; \
	\
	CHECK_FLD_IS_COMPATIBLE(desc); \
	\
	if (t_arr == NULL) \
		return false; \
	\
	arr_len = (*(int *) (((char *) field) + desc->nfd_arr_len_off)); \
	\
	appendFieldName(into, desc); \
	appendStringInfoCharMacro(into, '('); \
	for (int i = 0; i < arr_len; i++) \
	{ \
		_type_	value; \
		if (i != 0) \
			appendStringInfoCharMacro(into, ' '); \
		value = t_arr[i]; \
		write_value; \
	} \
	appendStringInfoCharMacro(into, ')'); \
	\
	return true; \
}

#define format(fmt) appendStringInfo(into, fmt, value)
#define writeout(fnc) fnc(into, value)

TextScalarFieldWriter(bool, BOOL, false, writeout(outbool))
TextScalarFieldWriter(char, CHAR, 0, writeout(outChar))
TextScalarFieldWriter(double, DOUBLE, 0.0, writeout(outDouble))

TextScalarFieldWriter(ParseLoc, PARSELOC, -1, format("%d"))
TextScalarFieldWriter(int, INT, 0, format("%d"))
TextScalarFieldWriter(int16, INT16, 0, format("%d"))
TextScalarFieldWriter(int32, INT32, 0, format("%d"))
TextScalarFieldWriter(long, LONG, 0, format("%ld"))
TextScalarFieldWriter(uint, UINT, 0, format("%u"))
TextScalarFieldWriter(uint16, UINT16, 0, format("%u"))
TextScalarFieldWriter(uint32, UINT32, 0, format("%u"))
TextScalarFieldWriter(uint64, UINT64, 0, format(UINT64_FORMAT))
TextScalarFieldWriter(Oid, OID, 0, format("%u"))

#define tsfw(_type_) \
	[NFT_##_type_] = &tnfw_##_type_, \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &tnfw_array_##_type_

#define tsfw_unimpl(_type_) \
	[NFT_##_type_] = &tnfw_unimplemented

#define tsfw_unimpl_arr(_type_) \
	[NFT_##_type_ | NFT_ARRAYTYPE] = &tnfw_unimplemented
#define tsfw_unknown_type(val) \
	[(val)] = &tnfw_unknown

const NodeWriter TextNodeWriter = &(NodeWriterData){
	.nw_start_node = &tnw_start_node,
	.nw_finish_node = &tnw_finish_node,
	.nw_fld_writers = {
		tsfw(BOOL),
		tsfw(PARSELOC),
		tsfw(INT),
		tsfw(INT16),
		tsfw(INT32),
		tsfw(LONG),
		tsfw(UINT),
		tsfw(UINT16),
		tsfw(UINT32),
		tsfw(UINT64),
		tsfw(OID),
		tsfw(CHAR),
		tsfw(DOUBLE),
		[NFT_ENUM] = &tnfw_INT, /* enum has the same layout as INT */
		[NFT_ENUM + NFT_ARRAYTYPE] = &tnfw_array_INT,
		tsfw(CSTRING),
		[NFT_BITMAPSET] = &tnfw_NODE,
		[NFT_BITMAPSET + NFT_ARRAYTYPE] = &tnfw_array_NODE,
		tsfw(NODE),
		tsfw_unimpl(PARAM_PATH_INFO),
		tsfw_unimpl_arr(PARAM_PATH_INFO),
		tsfw_unimpl(UNDEFINED),
		tsfw_unimpl(NUM_TYPES),
		/* fill with invalid entries */
		tsfw_unknown_type(NFT_INVALID_20),
		tsfw_unknown_type(NFT_INVALID_21),
		tsfw_unknown_type(NFT_INVALID_22),
		tsfw_unknown_type(NFT_INVALID_23),
		tsfw_unknown_type(NFT_INVALID_24),
		tsfw_unknown_type(NFT_INVALID_25),
		tsfw_unknown_type(NFT_INVALID_26),
		tsfw_unknown_type(NFT_INVALID_27),
		tsfw_unknown_type(NFT_INVALID_28),
		tsfw_unknown_type(NFT_INVALID_29),
		tsfw_unknown_type(NFT_INVALID_30),
		tsfw_unknown_type(NFT_INVALID_31),
	}
};

#undef tsfw
#undef tsfw_unimpl
#undef tsfw_unimpl_arr
#undef tsfw_unknown_type
