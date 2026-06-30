/*--------------------------------------------------------------------
 * guc.h
 *
 * External declarations pertaining to Grand Unified Configuration.
 *
 * Copyright (c) 2000-2026, PostgreSQL Global Development Group
 * Written by Peter Eisentraut <peter_e@gmx.net>.
 *
 * src/include/utils/guc.h
 *--------------------------------------------------------------------
 */
#ifndef GUC_COLLECTION_H
#define GUC_COLLECTION_H

#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/array.h"

typedef struct GucCollectionMember GucCollectionMember;
typedef struct GucCollection GucCollection;

typedef void (*GCBoolAssignHook) (bool newval, void *extra);
typedef void (*GCIntAssignHook) (int newval, void *extra);
typedef void (*GCRealAssignHook) (double newval, void *extra);
typedef void (*GCStringAssignHook) (const char *newval, void *extra);
typedef void (*GCEnumAssignHook) (int newval, void *extra);


typedef struct GCBoolMember
{
	bool		bootValue;
	bool		resetValue;
} GCBoolMember;
//
//typedef struct GucCollectionMember
//{
//	const char *member_name;
//	int			offset;
//	enum config_type type;
//
//	union {
//		struct GCBoolMember b;
//	} val;
//	union {
//		GucBoolCheckHook b;
//	} check;
//	union {
//		GucBoolAssignHook b;
//	} assign;
//} GucCollectionMember;
//
//typedef struct GucCollection
//{
//	const char *name_prefix;
//	int			size;
//	int			nmembers;
//	GucCollectionMember members[FLEXIBLE_ARRAY_MEMBER];
//} GucCollection;
//
//extern void RegisterGucCollection(const char *name,
//								  const GucCollection *collection);
//
///* return */
//extern void *GetGucCollection(const char *name);


#endif							/* GUC_COLLECTION_H */
