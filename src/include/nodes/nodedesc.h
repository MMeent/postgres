#ifndef NODEDESC_H
#define NODEDESC_H
#include "nodes.h"

/*
 * Field types. Note that this counts individually serialized & processed
 * data scalars, rather than the direct fields of the nodes (and its ancestor
 * nodes when 
 */
typedef enum NodeFieldType {
	NFT_UNDEFINED,
	/* Scalar values */
	NFT_BOOL,
	NFT_PARSELOC,
	NFT_TYPMOD,
	NFT_INT,
	NFT_INT16,
	NFT_INT32,
	NFT_LONG,
	NFT_UINT8,
	NFT_UINT16,
	NFT_UINT32,
	NFT_UINT64,
	NFT_OID,
	NFT_CHAR,
	NFT_DOUBLE,
	NFT_ENUM,
	/* various by-ref field types */
	NFT_VARLENA,	/* primarily used for NodeTree values, but any varlena is valid */
	NFT_CSTRING,	/* 0-terminated strings */
	NFT_NODE,		/* any node */
	/* invalid unique type values follow */
	NFT_NUM_TYPES = 19,			/* invalid */
	/* used as bitmask for array types. */
	NFT_ARRAYTYPE = 32,
} NodeFieldType;

#define NODEDESC_CUSTOM_HANDLERS	0x000F
#define NODEDESC_CUSTOM_WRITE		0x0001
#define NODEDESC_CUSTOM_READ		0x0002

#define NODEDESC_DISABLE_HANDLERS	0x0F00
#define NODEDESC_DISABLE_WRITE		0x0100
#define NODEDESC_DISABLE_READ		0x0200

#define NFD_READ_DEFAULT			0x1000

typedef struct NodeDescData {
	char	   *nd_name;
	NodeTag		nd_nodetag;
	uint16		nd_size;			/* size of the node type */
	uint8		nd_namelen;			/* length of the name string */
	uint8		nd_fields;			/* number of fields */
	uint16		nd_fields_offset;	/* offset of the first field into the
									 * NodeFieldDescriptors array */
	uint16		nd_flags;			/* see below */
	uint16		nd_fld_flags;		/* all field flags, or-ed */
	uint16		nd_custom_off;		/* custom IO functions */
} NodeDescData;
typedef const NodeDescData *NodeDesc;

typedef struct NodeFieldDescData {
	char		   *nfd_name;		/* name of this field; unique within each node. */
	NodeTag			nfd_node;		/* NodeTag of node type of which this is a field */
	NodeFieldType	nfd_type;		/* type of this field */
	uint8			nfd_namelen;	/* length of this field's name */
	uint8			nfd_field_no;	/* field number */
	uint16			nfd_offset;		/* offset from Node base pointer */
	uint16			nfd_flags;		/* flag bits */
	int16			nfd_arrlen_off;	/*---
									 * When negative:
									 *	offset from this field to the int
									 *	field that contains the size of
									 *	the array.
									 * When non-negative:
									 *	offset to a List field whose length
									 *	indicates this array's length.
									 */
} NodeFieldDescData;

typedef const NodeFieldDescData * NodeFieldDesc;

extern const NodeDescData NodeDescriptors[];
extern const NodeFieldDescData NodeFieldDescriptors[];

/* initialized only when required by the backend */
extern NodeDesc NodeDescriptorsByName;
extern void InitializeOrderedNodeDescriptors(void);

static inline void
UseOrderedNodeDescs()
{
	if (unlikely(NodeDescriptorsByName == NULL))
		InitializeOrderedNodeDescriptors();
}

extern NodeDesc GetNodeDescByNodeName(const char *name, int len);


static inline NodeDesc
GetNodeDesc(NodeTag tag)
{
	NodeDesc	desc = &NodeDescriptors[tag];

	Assert(desc->nd_nodetag == tag);

	return desc;
}

static inline NodeFieldDesc
GetNodeFieldDesc(NodeDesc nodeDesc, int field_no)
{
	NodeFieldDesc fdesc;

	Assert(field_no < nodeDesc->nd_fields);

	fdesc = &NodeFieldDescriptors[nodeDesc->nd_fields_offset + field_no];

	Assert(fdesc->nfd_node == nodeDesc->nd_nodetag);
	Assert(fdesc->nfd_field_no == field_no);

	return fdesc;
}

#endif /* NODEDESC_H */
