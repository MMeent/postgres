/*-------------------------------------------------------------------------
 *
 * ndio_json.c
 *		Implementation for json-serialization using NodeDesc-based
 *		infrastructure.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/nodes/ndio_json.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "nodes/nodes.h"
#include "nodes/ndio.h"
#include "nodes/nodedesc.h"
#include "nodes/plannodes.h"
#include "nodes/replnodes.h"
#include "varatt.h"
#include "common/shortest_dec.h"

/*---
 * JSON Node Writer
 *		NodeWriter implementation for a JSON-based node serialization format.
 *
 * Nodes are json objects with the "_type" type tag, 
 * VARLENA value is encoded as {"varlen": <len_bytes>, "vardata": "0x<hexbytes>"}
 * 
 */

#define AppendText(into, literal) \
	appendBinaryStringInfo(into, (literal), sizeof(literal) - 1)

static void
jnw_start_node(StringInfo into, NodeDesc desc, uint32 flags)
{
	AppendText(into, "{\"_type\": \"");
	appendBinaryStringInfo(into, desc->nd_name, desc->nd_namelen);
	appendStringInfoCharMacro(into, '"');
}

static bool
jnw_finish_node(StringInfo into, NodeDesc desc, int last_field,
				uint32 flags)
{
	appendStringInfoCharMacro(into, '}');
	return true;
}

static void
jnw_start_field(StringInfo into, NodeFieldDesc desc, uint32 flags)
{
	AppendText(into, ", \"");
	appendBinaryStringInfo(into, desc->nfd_name, desc->nfd_namelen);
	AppendText(into, "\": ");
}

static void
jnw_start_array(StringInfo into, uint32 flags)
{
	appendStringInfoCharMacro(into, '[');
}
static void
jnw_end_array(StringInfo into, uint32 flags)
{
	appendStringInfoCharMacro(into, ']');
}
static void
jnw_field_entry_separator(StringInfo into, uint32 flags)
{
	AppendText(into, ", ");
}

static void
jnw_write_null(StringInfo into, uint32 flags)
{
	AppendText(into, "null");
}

static bool
jnvw_VARLENA(StringInfo into, const void *value, uint32 flags)
{
	struct varlena *varlen1 = *(struct varlena **) value;
	char	   *data;
	int32		exhdr;

	if (varlen1 == NULL)
	{
		AppendText(into, "null");
		return true;
	}

	exhdr = VARSIZE_ANY_EXHDR(varlen1);
	data = VARDATA(varlen1);

	AppendText(into, "{\"varsize\": ");
	appendStringInfo(into, "%d", exhdr);
	AppendText(into, ", \"vardata\": \"0x");
	for (int i = 0; i < exhdr; i++)
	{
		appendStringInfo(into, "%02x", (unsigned char) data[i]);
	}

	appendStringInfoCharMacro(into, '"');
	appendStringInfoCharMacro(into, '}');

	return true;
}

static bool
jnfw_VARLENA(StringInfo into, NodeFieldDesc desc, const void *field,
			 uint32 flags)
{
	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS)
		&& *(char **) field == NULL)
		return false;

	jnw_start_field(into, desc, flags);

	return jnvw_VARLENA(into, field, flags);
}

static bool
jnvw_CSTRING(StringInfo into, const void *value, uint32 flags)
{
	char	   *cstr = *(char **) value;
	
	if (cstr == NULL)
	{
		appendStringInfoString(into, "null");
		return true;
	}

	appendStringInfoCharMacro(into, '"');

	while (*cstr != '\0')
	{
		if (*cstr < ' ')
		{
			switch (*cstr)
			{
			case '\n':
				AppendText(into, "\\n");
				break;
			case '\b':
				AppendText(into, "\\b");
				break;
			case '\f':
				AppendText(into, "\\f");
				break;
			case '\r':
				AppendText(into, "\\r");
				break;
			case '\t':
				AppendText(into, "\\t");
				break;
			default:
				appendStringInfoCharMacro(into, '\\');
				appendStringInfoCharMacro(into, 'u');
				appendStringInfo(into, "%04x", *cstr);
			}
			continue;
		}
		else if (*cstr == '"' || *cstr == '\\')
		{
			appendStringInfoCharMacro(into, '\\');
		}
		appendStringInfoCharMacro(into, *cstr);

		cstr++;
	}

	appendStringInfoCharMacro(into, '"');

	return true;
}

static bool
jnfw_CSTRING(StringInfo into, NodeFieldDesc desc, const void *field,
			 uint32 flags)
{
	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS)
		&& *(const char **) field == NULL)
		return false;

	jnw_start_field(into, desc, flags);

	return jnvw_CSTRING(into, field, flags);
}

static bool
jnvw_NODE(StringInfo into, const void *field, uint32 flags)
{
	Node	   *val = *(Node **) field;

	return WriteNode(into, val, JSONNodeWriter, flags);
}

static bool
jnfw_NODE(StringInfo into, NodeFieldDesc desc, const void *field,
		  uint32 flags)
{
	Node	   *node = *(Node **) field;

	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS) && node == NULL)
		return false;

	jnw_start_field(into, desc, flags);

	return jnvw_NODE(into, field, flags);
}

static bool
jnvw_unimplemented(StringInfo into, const void *field, uint32 flags)
{
	Assert(false);
	return false;
}

static bool
jnfw_unimplemented(StringInfo into, NodeFieldDesc desc, const void *field,
				   uint32 flags)
{
	return jnvw_unimplemented(into, field, flags);
}

#define JSONScalarFieldWriter(_type_, _uctype_, type_default, write_value) \
static bool \
jnvw_##_uctype_(StringInfo into, const void *field, uint32 flags) \
{ \
	_type_		value = *(const _type_ *) field; \
	\
	write_value; \
	\
	return true; \
} \
static bool \
jnfw_##_uctype_(StringInfo into, NodeFieldDesc desc, const void *field, \
				uint32 flags) \
{ \
	if (!(flags & ND_WRITE_NO_SKIP_DEFAULTS) && \
		*(const _type_ *) field == (type_default)) \
		return false; \
	\
	jnw_start_field(into, desc, flags); \
	\
	return jnvw_##_uctype_(into, field, flags); \
}

#define outbool(into, value, flags) \
	appendStringInfoString((into), (value) ? "true" : "false")

	/*
 * Convert a double value, attempting to ensure the value is preserved exactly.
 */
static void
outDouble(StringInfo str, double d, uint32 flags)
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
outChar(StringInfo str, char c, uint32 flags)
{
	char		in[2];
	char	   *ref;

	if (c == '\0')
	{
		/* we don't support "\0" nor "\u0000", so NULL it is */
		appendStringInfoString(str, "null");
		return;
	}

	in[0] = c;
	in[1] = '\0';
	ref = &in[0];

	jnvw_CSTRING(str, &ref, 0);
}

#define format(fmt) appendStringInfo(into, fmt, value)
#define writeout(fnc) fnc(into, value, flags)

JSONScalarFieldWriter(bool, BOOL, false, writeout(outbool))
JSONScalarFieldWriter(char, CHAR, 0, writeout(outChar))
JSONScalarFieldWriter(double, DOUBLE, 0.0, writeout(outDouble))

JSONScalarFieldWriter(ParseLoc, PARSELOC, -1, format("%d"))
JSONScalarFieldWriter(TypMod, TYPMOD, -1, format("%d"))
JSONScalarFieldWriter(int, INT, 0, format("%d"))
JSONScalarFieldWriter(int16, INT16, 0, format("%d"))
JSONScalarFieldWriter(int32, INT32, 0, format("%d"))
JSONScalarFieldWriter(long, LONG, 0, format("%ld"))
JSONScalarFieldWriter(uint16, UINT8, 0, format("%u"))
JSONScalarFieldWriter(uint16, UINT16, 0, format("%u"))
JSONScalarFieldWriter(uint32, UINT32, 0, format("%u"))
JSONScalarFieldWriter(uint64, UINT64, 0, format(UINT64_FORMAT))
JSONScalarFieldWriter(Oid, OID, 0, format("%u"))

#define jsfw(_type_) \
	[NFT_##_type_] = jnfw_##_type_
#define jsfw_unimpl(_type_) \
	[NFT_##_type_] = jnfw_unimplemented

#define jsvw(_type_) \
	[NFT_##_type_] = jnvw_##_type_
#define jsvw_unimpl(_type_) \
	[NFT_##_type_] = jnvw_unimplemented

const NodeWriter JSONNodeWriter = &(NodeWriterData) {
	.nw_start_node = jnw_start_node,
	.nw_finish_node = jnw_finish_node,
	.nw_start_field = jnw_start_field,
	.nw_start_array = jnw_start_array,
	.nw_write_null = jnw_write_null,
	.nw_end_array = jnw_end_array,
	.nw_array_entry_separator = jnw_field_entry_separator,
	.nw_end_field = NULL,
	.nw_fld_writers = {
		jsfw_unimpl(UNDEFINED),
		jsfw(BOOL),
		jsfw(PARSELOC),
		jsfw(TYPMOD),
		jsfw(INT),
		jsfw(INT16),
		jsfw(INT32),
		jsfw(LONG),
		jsfw(UINT8),
		jsfw(UINT16),
		jsfw(UINT32),
		jsfw(UINT64),
		jsfw(OID),
		jsfw(CHAR),
		jsfw(DOUBLE),
		[NFT_ENUM] = &jnfw_INT, /* enum has the same layout as INT */
		jsfw(VARLENA),
		jsfw(CSTRING),
		jsfw(NODE),
	},
	.nw_val_writers = {
		jsvw_unimpl(UNDEFINED),
		jsvw(BOOL),
		jsvw(PARSELOC),
		jsvw(TYPMOD),
		jsvw(INT),
		jsvw(INT16),
		jsvw(INT32),
		jsvw(LONG),
		jsvw(UINT8),
		jsvw(UINT16),
		jsvw(UINT32),
		jsvw(UINT64),
		jsvw(OID),
		jsvw(CHAR),
		jsvw(DOUBLE),
		[NFT_ENUM] = &jnvw_INT, /* enum has the same layout as INT */
		jsvw(VARLENA),
		jsvw(CSTRING),
		jsvw(NODE),
	}
};
