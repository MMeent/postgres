/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/hashfn.h"
#include "storage/buf_internals.h"
#include "utils/dynahash.h"

/*
 * Hash element: Singly linked list with hash of value contained, and
 * the reference to the buffer header that contains the to-be-hashed value.
 *
 * Note: when negative, buffer is a sentinel value indicating it as a freelist
 * entry for list at offset (-1 - offset). The user should lock that freelist
 * to acquire that slot, but not before confirming that the value hasn't
 * been updated since.
 */
typedef struct HashElement
{
	int32		next;
	uint32		hashValue;
	pg_atomic_uint32	buffer;
} HashElement;

/*
 * Free list entry for the hash.
 *
 * Note tha
 */
typedef struct HashFreeListEntry
{
	int32		next;
	int32		prev;
	pg_atomic_uint32 tag;
} HashFreeListEntry;

StaticAssertDecl(offsetof(HashFreeListEntry, tag) == offsetof(HashElement, buffer),
				 "tag and buffer should be at the same offset into HashFreeListEntry and HashElement");

typedef struct HashBucket
{
	int32		bucketStartElement;
	union {
		HashElement	element;
		HashFreeListEntry freeEntry;
	};
} HashBucket;


typedef struct FreeList
{
	slock_t	mutex;
	int32	firstFree;
	int32	lastFree;
	int32	myTag;
} FreeList;


typedef union FreeListPadded
{
	FreeList	list;
	char		padding[PG_CACHE_LINE_SIZE];
} FreeListPadded;

typedef struct BufTableHeader
{
	uint32		numBuckets;
	uint32		mask;
} BufTableHeader;

typedef struct BufTable {
	union {
		BufTableHeader hdr;
		char	pad[CACHELINEALIGN(sizeof(BufTableHeader))];
	};
	FreeListPadded	freeList[NUM_BUFFER_PARTITIONS];
	HashBucket	buckets[FLEXIBLE_ARRAY_MEMBER];
} BufTable;

static BufTable *bufLookupTable;

#define NumBucketsFromSize(size) (1 << my_log2((size * 5) / 4))

#define FreeListIdxFromHash(hash) (hash % NUM_BUFFER_PARTITIONS)

static inline FreeList *
BufTableGetFreelist(BufTable *table, uint32 idx)
{
	Assert(idx < NUM_BUFFER_PARTITIONS);
	return &table->freeList[idx].list;
}

static inline HashBucket *
BufTableGetBucket(BufTable *table, uint32 hash)
{
	int32		offset = (hash & table->hdr.mask);

	Assert(offset < table->hdr.numBuckets);

	return &table->buckets[offset];
}

static inline HashElement *
BufTableGetElement(BufTable *table, int32 offset)
{
	Assert(offset >= 0 && offset <= table->hdr.numBuckets);
	return &table->buckets[offset].element;
}

static inline HashFreeListEntry *
BufTableGetFreeEntry(BufTable *table, int32 offset)
{
	Assert(offset >= 0 && offset <= table->hdr.numBuckets);
	return &table->buckets[offset].freeEntry;
}

/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
Size
BufTableShmemSize(int size)
{
	uint64		nbuckets = NumBucketsFromSize(size);
	Size		alloc = 0;

	alloc = add_size(alloc, PG_CACHE_LINE_SIZE);
	alloc = add_size(alloc, offsetof(BufTable, buckets));
	alloc = add_size(alloc, mul_size(nbuckets, sizeof(HashBucket)));

	return alloc;
}


static inline void
add_to_freelist(BufTable *table, HashFreeListEntry *freeEntry, uint32 index)
{
	uint32		freeListIdx;
	FreeList   *freeList;
	int32		next;
	int32		freeListTag;
	Assert(freeEntry == BufTableGetFreeEntry(table, index));

	freeListIdx = FreeListIdxFromHash(index);
	freeList = BufTableGetFreelist(bufLookupTable, freeListIdx);
	freeListTag = (-1 - (int32) freeListIdx);
	freeEntry->next = freeListTag;
	freeEntry->prev = freeListTag;

	S_LOCK(&freeList->mutex);
	next = freeList->firstFree;
	if (next < 0)
	{
		pg_atomic_write_u32(&freeEntry->tag, (uint32) freeListTag);
		freeList->firstFree = (int32) index;
		freeList->lastFree = (int32) index;
	}
	else
	{
		HashFreeListEntry *prevEntry;

		prevEntry = BufTableGetFreeEntry(table, next);
		Assert(pg_atomic_read_u32(&prevEntry->tag) == (uint32) freeListTag);

		pg_atomic_write_u32(&freeEntry->tag, (uint32) freeListTag);

		freeEntry->prev = freeList->firstFree;
		freeList->firstFree = prevEntry->next = (int32) index;
	}
	S_UNLOCK(&freeList->mutex);
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 */
void
InitBufTable(int size)
{
	void	   *baseptr;
	bool		found = false;
	uint32		nbuckets = NumBucketsFromSize(size);

	baseptr = ShmemInitStruct("Shared Buffer Lookup Table",
							  BufTableShmemSize(size),
							  &found);

	/*
	 * Runs only in postmaster during startup; there is no concurrent process
	 * that can or is allowed to initialize this.
	 */
	Assert(!found);

	if (found)
		return;

	Assert(PointerIsAligned(baseptr, void *));
	bufLookupTable = (void *) CACHELINEALIGN((char *) baseptr);

	/* nbuckets is always a power of 2, so mask is always nbuckets-1 */
	bufLookupTable->hdr.numBuckets = nbuckets;
	bufLookupTable->hdr.mask = nbuckets - 1;

	for (int i = 0; i < NUM_BUFFER_PARTITIONS; i++)
	{
		FreeList *list = &bufLookupTable->freeList[i].list;

		S_INIT_LOCK(&list->mutex);
		list->myTag = (-1 - i);

		list->firstFree = list->myTag;
		list->lastFree = list->myTag;
	}

	/* build per-partition freelists */
	for (int i = 0; i < nbuckets; i++)
	{
		HashBucket *bucket = BufTableGetBucket(bufLookupTable, i);
		bucket->bucketStartElement = -1;
		add_to_freelist(bufLookupTable, &bucket->freeEntry, i);
	}
}

/* Convert a hash value to a bucket number */
static inline int32
calc_bucket(BufTable *table, uint32 hash_val)
{
	uint32		bucket;

	bucket = hash_val & table->hdr.mask;

	return (int32) bucket;
}

static inline HashElement *
initial_lookup(BufTable *table, uint32 hashvalue, HashBucket **hashBucket, int32 *idx)
{
	HashBucket *bucket;
	int32		bucketno;
	int32		startElementOffset;

	bucketno = calc_bucket(table, hashvalue);

	bucket = BufTableGetBucket(bufLookupTable, bucketno);
	startElementOffset = bucket->bucketStartElement;
	*idx = startElementOffset;
	*hashBucket = bucket;

	if (startElementOffset < 0)
		return NULL;

	if (likely(startElementOffset == bucketno))
		return &bucket->element;

	return BufTableGetElement(table, startElementOffset);
}

/*
 * Get a new free element from a freelist.
 *
 * NOTE: This assumes there are free elements available. If that assumption
 * does not hold this code will loop infinitely.
 */
static HashElement *
buftable_get_free_element(BufTable *table, uint32 hashValue, int32 *pInt)
{
	FreeList   *freeList;
	HashFreeListEntry *entry;

	do {
		for (;; hashValue++)
		{
			freeList = BufTableGetFreelist(table, FreeListIdxFromHash(hashValue));

			S_LOCK(&freeList->mutex);
			if (freeList->firstFree != freeList->myTag)
				break;
			S_UNLOCK(&freeList->mutex);
		}

		entry = BufTableGetFreeEntry(table, freeList->firstFree);
		*pInt = freeList->firstFree;
	} while (false);

	Assert(entry->next == freeList->myTag);

	/* unlink the entry from the linked list*/
	if (entry->prev != freeList->myTag)
	{
		HashFreeListEntry *prevEntry = BufTableGetFreeEntry(table, entry->prev);
		prevEntry->next = entry->next;
		freeList->firstFree = entry->prev;
	}
	else
	{
		Assert(freeList->firstFree == freeList->lastFree);
		Assert(entry->next == entry->prev);
	}

	S_UNLOCK(&freeList->mutex);

	return (HashElement *) entry;
}


/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
BufTableHashCode(BufferTag *tagPtr)
{
	return hash_bytes((void *) tagPtr, sizeof(BufferTag));
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 */
int
BufTableLookup(BufferTag *tagPtr, uint32 hashcode)
{
	BufTable *table = bufLookupTable;
	HashBucket *currBucket;
	HashElement *matchElement;
	int32 curIdx;

	/*
	 * Do the initial lookup
	 */
	matchElement = initial_lookup(table, hashcode, &currBucket,
								  &curIdx);

	/* follow the bucket's chain */
	while (matchElement != NULL)
	{
		if (matchElement->hashValue == hashcode && BufferTagsEqual(
		tagPtr,
		&GetBufferDescriptor(pg_atomic_read_u32(&matchElement->buffer))->tag
		))
			break;

		if (matchElement->next < 0)
		{
			matchElement = NULL;
			break;
		}

		curIdx = matchElement->next;
		matchElement = BufTableGetElement(table, curIdx);
	}

	if (!matchElement)
		return -1;

	return (int) pg_atomic_read_u32(&matchElement->buffer);
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	BufTable *table = bufLookupTable;
	HashBucket *currBucket;
	HashElement *matchElement;
	int32	   *prevNextPtr;
	int32		matchElemIdx;

	Assert(buf_id >= 0);		/* -1 is reserved for not-in-table */
	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	/*
	 * Do the initial lookup
	 */
	matchElement = initial_lookup(table, hashcode, &currBucket,
								  &matchElemIdx);
	prevNextPtr = &currBucket->bucketStartElement;

	while (matchElement != NULL)
	{
		/*
		 * Make sure it's a valid element in the chain. We have a lock on
		 * that chain, so any breakage is a broken invariant.
		 */
		Assert(0 <= (int32) pg_atomic_read_u32(&matchElement->buffer));

		if (matchElement->hashValue == hashcode && BufferTagsEqual(
			tagPtr,
			&GetBufferDescriptor(pg_atomic_read_u32(&matchElement->buffer))->tag
		))
			break;

		prevNextPtr = &matchElement->next;

		if (matchElement->next < 0)
		{
			Assert(matchElement->next == -1);
			matchElement = NULL;
			break;
		}

		matchElemIdx = matchElement->next;
		matchElement = BufTableGetElement(table, matchElemIdx);
	}

	/* Return existing buffer ID if we found a pre-existing buffer */
	if (matchElement != NULL)
		return (int) pg_atomic_read_u32(&matchElement->buffer);

	/* else, create a new one */
	matchElement = buftable_get_free_element(table, hashcode, &matchElemIdx);

	/* link into hashbucket chain */
	*prevNextPtr = matchElemIdx;

	/* copy data into the element */
	matchElement->next = -1;	/* it's the tail of the bucket */
	matchElement->hashValue = hashcode;

	pg_atomic_write_u32(&matchElement->buffer, (uint32) ((int32) buf_id));

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
void
BufTableDelete(BufferTag *tagPtr, uint32 hashcode, int buffer)
{
	BufTable	   *table = bufLookupTable;
	HashBucket	   *currBucket;
	HashElement	   *matchElement;
	int32		   *prevNextPtr;
	int32			matchElemIdx;

	/*
	 * Do the initial lookup
	 */
	matchElement = initial_lookup(table, hashcode, &currBucket,
								  &matchElemIdx);
	prevNextPtr = &currBucket->bucketStartElement;

	while (matchElement != NULL)
	{
		if (matchElement->hashValue == hashcode &&
			pg_atomic_read_u32(&matchElement->buffer) == buffer)
			break;

		prevNextPtr = &matchElement->next;

		if (matchElement->next < 0)
		{
			matchElement = NULL;
			break;
		}

		matchElemIdx = matchElement->next;
		matchElement = BufTableGetElement(table, matchElemIdx);
	}

	if (matchElement != NULL)
	{
		/*
		 * We can't test the buffer tag, because that may have been cleared
		 * by the caller by now.
		 */
		Assert(0 <= (int32) pg_atomic_read_u32(&matchElement->buffer));

		/* unlink the element from the singly-linked list */
		*prevNextPtr = matchElement->next;

		/*
		 * Remove record from hash bucket's chain.
		 *
		 * Note that bucket chains are singly linked lists, but freelists
		 * doubly linked lists.
		 */
		add_to_freelist(bufLookupTable, (HashFreeListEntry *) matchElement,
						matchElemIdx);
	}
	else
	{
		elog(PANIC, "shared buffer hash table corrupted");
	}
}
