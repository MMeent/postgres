//
// Created by Matthias on 2024-05-20.
//

#ifndef POSTGRESQL_NDIO_H
#define POSTGRESQL_NDIO_H

#include "lib/stringinfo.h"
#include "nodes/nodedesc.h"

#define ND_WRITE_NO_SKIP_DEFAULTS	0x01
#define ND_WRITE_IGNORE_PARSELOC	0x02

typedef bool (*WriteTypedValue)(StringInfo into, const void *field,
								uint32 flags);
typedef bool (*WriteTypedField)(StringInfo into, NodeFieldDesc desc,
								const void *field, uint32 flags);

typedef struct NodeWriterData {
	int32	flags;
	/* required */
	void	(*nw_start_node)(StringInfo into, NodeDesc desc, uint32 flags);
	bool	(*nw_finish_node)(StringInfo into, NodeDesc desc, int last_field,
							  uint32 flags);
	void	(*nw_write_null)(StringInfo into, uint32 flags);
	void	(*nw_start_field)(StringInfo into, NodeFieldDesc desc, uint32 flags);
	void	(*nw_start_array)(StringInfo into, uint32 flags);
	/* optional */
	void	(*nw_field_entry_separator)(StringInfo into, uint32 flags);
	void	(*nw_end_array)(StringInfo into, uint32 flags);
	void	(*nw_end_field)(StringInfo into, uint32 flags);
	/* mostly required */
	WriteTypedField nw_fld_writers[NFT_NUM_TYPES];
	WriteTypedValue nw_val_writers[NFT_NUM_TYPES];
} NodeWriterData;
typedef const NodeWriterData *NodeWriter;

typedef bool (*WriteNodeFunc)(StringInfo into, const Node *node,
							  NodeWriter writer, uint32 flags);
extern bool WriteNode(StringInfo into, const Node *node, NodeWriter writer,
					  uint32 flags);

/*
 * Returns true if the stream contained its data.
 * For *value that is always the case, for *field this may be skipped
 * RTValue returns bool so that tail-call optimization may be used.
 */
typedef bool (*ReadTypedValue)(StringInfo from, void *field, uint32 flags);
typedef bool (*ReadTypedField)(StringInfo from, NodeFieldDesc desc, void *field, uint32 flags);

#define ND_READ_ARRAY_PREALLOCATED	0x01

typedef struct NodeReaderData {
	/* 
	 * return false if the node is NULL, return true and set *out to the next
	 * node's NodeTag to indicate we've started reading a node of that type.
	 */
	bool	(*nr_read_tag)(StringInfo from, uint32 flags, NodeTag *out);
	/* Finish reading the node from the stringinfo. */
	void	(*nr_finish_node)(StringInfo from, NodeDesc desc, int final_field,
							  uint32 flags);
	/* Begin reading a field from the stringinfo */
	bool	(*nr_start_field)(StringInfo from, NodeFieldDesc desc, uint32 flags);
	/* Begin reading an array (true), or value is null (false) */
	bool	(*nr_start_array)(StringInfo from, uint32 flags);
	/* optional */
	void	(*nr_array_value_separator)(StringInfo from, uint32 flags);
	void	(*nr_end_array)(StringInfo from, uint32 flags);
	void	(*nr_end_field)(StringInfo from, uint32 flags);
	/* Read any of the various field types */
	ReadTypedField nr_fld_readers[NFT_NUM_TYPES];
	ReadTypedValue nr_val_readers[NFT_NUM_TYPES];
} NodeReaderData;
typedef const NodeReaderData * NodeReader;

typedef Node *(*ReadNodeFunc)(StringInfo from, NodeReader reader, uint32 flags);
extern Node *ReadNode(StringInfo from, NodeReader reader, uint32 flags);

typedef union CustomNodeDescFuncs {
	ReadNodeFunc	cndf_read_node;
	WriteNodeFunc	cndf_write_node;
} CustomNodeDescFunc;

extern const CustomNodeDescFunc CustomNodeDescFunctions[];

extern const NodeReader BinaryNodeReader;
extern const NodeWriter BinaryNodeWriter;
#define PG_BINSER_NODE_END 0xFF

extern const NodeReader TextNodeReader;
extern const NodeWriter TextNodeWriter;

extern const NodeWriter JSONNodeWriter;

#endif //POSTGRESQL_NDIO_H
