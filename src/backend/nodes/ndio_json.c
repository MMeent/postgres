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

/*---
 * JSON Node Writer
 *		Writer for a JSON-based node serialization format.
 */

#define AppendText(into, literal) \
	appendBinaryStringInfo(into, (literal), sizeof(literal) - 1)

static void
jnw_start_node(StringInfo into, NodeDesc desc, uint32 flags)
{
	AppendText(into, "{\"type\": \"");
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
	struct varlena *varlen2 = pg_detoast_datum(varlen1);
	int32		exhdr;

	if (varlen2 == NULL)
	{
		AppendText(into, "null");
		return true;
	}
	exhdr = VARSIZE_ANY_EXHDR(varlen2);

	AppendText(into, "{\"varsize\": ");
	appendStringInfo(into, "%d", exhdr);
	AppendText(into, ", \"vardata\": \"0x");
	for (int i = 0; i < exhdr; i++)
	{
		appendStringInfo(into, "%02x", VARDATA(varlen2)[i]);
	}

	appendStringInfoCharMacro(into, '"');
	appendStringInfoCharMacro(into, '}');

	return true;
}

static bool
jnfw_VARLENA(StringInfo into, NodeFieldDesc desc, const void *field,
			 uint32 flags)
{
	if (*(char **) field == NULL)
		return false;

	jnw_start_field(into, desc, flags);

	return jnvw_VARLENA(into, field, flags);
}

static bool
jnvw_CSTRING(StringInfo into, const void *value, uint32 flags)
{
	char	   *cstr = *(char **) value;

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
	}
	appendStringInfoCharMacro(into, '"');

	return true;
}

static bool
jnfw_CSTRING(StringInfo into, NodeFieldDesc desc, const void *field,
			 uint32 flags)
{
	if (*(char **) field == NULL)
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

	if (node == NULL)
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

#define JSONScalarFieldWriter(_type_, _uctype_, type_default, seras) \
static bool \
jnvw_##_uctype_(StringInfo into, const void *field, uint32 flags) \
{ \
	appendBinaryStringInfo(into, field, sizeof(_type_)); \
	\
	return true; \
} \
static bool \
jnfw_##_uctype_(StringInfo into, NodeFieldDesc desc, const void *field, \
				uint32 flags) \
{ \
	if (*(_type_ *) field == (type_default)) \
		return false; \
	\
	jnw_start_field(into, desc, flags); \
	\
	return jnvw_##_uctype_(into, field, flags); \
}

JSONScalarFieldWriter(bool, BOOL, false, "")
JSONScalarFieldWriter(ParseLoc, PARSELOC, -1, "")
JSONScalarFieldWriter(int, INT, 0, "")
JSONScalarFieldWriter(int16, INT16, 0, "")
JSONScalarFieldWriter(int32, INT32, 0, "")
JSONScalarFieldWriter(long, LONG, 0, "")
JSONScalarFieldWriter(uint16, UINT16, 0, "")
JSONScalarFieldWriter(uint32, UINT32, 0, "")
JSONScalarFieldWriter(uint64, UINT64, 0, "")
JSONScalarFieldWriter(Oid, OID, 0, "")
JSONScalarFieldWriter(char, CHAR, 0, "")
JSONScalarFieldWriter(double, DOUBLE, 0.0, "")

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
	.nw_field_entry_separator = jnw_field_entry_separator,
	.nw_end_field = NULL,
	.nw_fld_writers = {
		jsfw_unimpl(UNDEFINED),
		jsfw(BOOL),
		jsfw(PARSELOC),
		jsfw(INT),
		jsfw(INT16),
		jsfw(INT32),
		jsfw(LONG),
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
		jsvw(INT),
		jsvw(INT16),
		jsvw(INT32),
		jsvw(LONG),
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
