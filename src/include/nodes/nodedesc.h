#ifndef POSTGRESQL_NODEDESC_H
#define POSTGRESQL_NODEDESC_H
#include "nodes.h"

/*
 * Field types. Note that this counts individually serialized & processed
 * data scalars, rather than the direct fields of the nodes (and its ancestor
 * nodes when 
 */
typedef enum NodeFieldType {
	/*
	 * Scalar values, plus their array type tags
	 * Each scalar value has a 0 argument, each _array type stores the
	 * offset off the base pointer of the 
	 */
	NFT_BOOL,
	NFT_PARSELOC,
	NFT_INT,
	NFT_INT16,
	NFT_INT32,
	NFT_LONG,
	NFT_UINT,
	NFT_UINT16,
	NFT_UINT32,
	NFT_UINT64,
	NFT_OID,
	NFT_CHAR,
	NFT_DOUBLE,
	NFT_ENUM,

	/* various other field types */
	NFT_CSTRING,
	NFT_BITMAPSET,	/* */
	NFT_NODE,		/* is a generic node */
	NFT_PARAM_PATH_INFO,	/* */
	/* invalid unique type values follow */
	NFT_UNDEFINED,
	NFT_NUM_TYPES = 19,			/* invalid, but used as n_*/
	NFT_INVALID_20 = 20,		/* invalid */
	NFT_INVALID_21,				/* invalid */
	NFT_INVALID_22,				/* invalid */
	NFT_INVALID_23,				/* invalid */
	NFT_INVALID_24,				/* invalid */
	NFT_INVALID_25,				/* invalid */
	NFT_INVALID_26,				/* invalid */
	NFT_INVALID_27,				/* invalid */
	NFT_INVALID_28,				/* invalid */
	NFT_INVALID_29,				/* invalid */
	NFT_INVALID_30,				/* invalid */
	NFT_INVALID_31,				/* invalid */
	/* used as bit showing array types. */
	NFT_ARRAYTYPE = 32,
} NodeFieldType;

#define NODEDESC_CUSTOM_HANDLERS	0x000F
#define NODEDESC_CUSTOM_READ		0x0001
#define NODEDESC_CUSTOM_WRITE		0x0002

#define NODEDESC_DISABLE_HANDLERS	0x0F00
#define NODEDESC_DISABLE_READ		0x0100
#define NODEDESC_DISABLE_WRITE		0x0200

typedef struct NodeDescData {
	char	   *nd_name;
	NodeTag		nd_nodetag;
	uint16		nd_size;			/* size of the node type */
	uint8		nd_namelen;			/* length of the name string */
	uint8		nd_fields;			/* number of fields */
	uint16		nd_fields_offset;	/* offset of the first field into the
									 * NodeFieldDescriptors array */
	uint16		nd_flags;			/* see below */
	uint16		nd_fld_flags;	/* all field flags, or-ed */
	uint16		nd_custom_off;		/* custom IO functions */
} NodeDescData;
typedef const NodeDescData *NodeDesc;

typedef struct NodeFieldDescData {
	char		   *nfd_name;		/* name of the field. Unique for each node. */
	NodeTag			nfd_node;		/* field in which node? */
	NodeFieldType	nfd_type;		/* field type */
	uint8			nfd_namelen;	/* length of field name */
	uint8			nfd_field_no;	/* field number */
	uint16			nfd_offset;		/* offset from Node base pointer */
	uint16			nfd_flags;		/* flag bits */
	int16			nfd_arr_len_off; /* offset from this field to the int field with size of array. Should essentially always be negative. */
	uint16			nfd_custom_off; /* offset of first custom attribute */
} NodeFieldDescData;
typedef const NodeFieldDescData * NodeFieldDesc;

extern const NodeDescData NodeDescriptors[];

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

extern const NodeFieldDescData NodeFieldDescriptors[];

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

typedef bool (*WriteTypedField)(StringInfo into, NodeFieldDesc desc,
								void *field, uint32 flags);

typedef struct NodeWriterData {
	void	(*nw_start_node)(StringInfo into, NodeDesc desc, uint32 flags);
	bool	(*nw_finish_node)(StringInfo into, NodeDesc desc, int last_field,
							  uint32 flags);
	WriteTypedField nw_fld_writers[NFT_NUM_TYPES + NFT_ARRAYTYPE];
} NodeWriterData;
typedef const NodeWriterData *NodeWriter;

typedef bool (*WriteNodeFunc)(StringInfo into, const Node *node,
							  NodeWriter writer, uint32 flags);
extern bool WriteNode(StringInfo into, const Node *node, NodeWriter writer,
					  uint32 flags);

/* return how many fields you've read. Must be at least one. */
typedef void (*ReadTypedField)(StringInfo from, NodeFieldDesc desc,
							   void *field, uint32 flags);

#define ND_READ_ARRAY_PREALLOCATED	1

typedef struct NodeReaderData {
	/* 
	 * return false if the node is NULL, return true and set *out to the next
	 * node's NodeTag to indicate we've started reading a node of that type.
	 */
	bool	(*nr_read_tag)(StringInfo from, uint32 flags, NodeTag *out);
	void	(*nr_finish_node)(StringInfo from, NodeDesc desc, uint32 flags);
	ReadTypedField nr_fld_readers[NFT_NUM_TYPES + NFT_ARRAYTYPE];
} NodeReaderData;
typedef const NodeReaderData * NodeReader;

typedef Node *(*ReadNodeFunc)(StringInfo from, NodeReader reader, uint32 flags);
extern Node *ReadNode(StringInfo from, NodeReader reader, uint32 flags);

typedef union CustomNodeDescFuncs {
	ReadNodeFunc	cndf_read_node;
	ReadTypedField	cndf_read_field;
	WriteNodeFunc	cndf_write_node;
	WriteTypedField	cndf_write_field;
} CustomNodeDescFunc;

extern const CustomNodeDescFunc CustomNodeDescFunctions[];

extern const NodeReader BinaryNodeReader;
extern const NodeWriter BinaryNodeWriter;
#define PG_BINSER_NODE_END 0xFF

extern const NodeReader TextNodeReader;
extern const NodeReader JSONNodeReader;
extern const NodeWriter TextNodeWriter;
extern const NodeWriter JSONNodeWriter;

#endif //POSTGRESQL_NODEDESC_H
