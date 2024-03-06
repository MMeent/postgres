/*-------------------------------------------------------------------------
 *
 * nbtutils.c
 *	  Utility code for Postgres btree implementation.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/nbtree/nbtutils.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>

#include "access/nbtree.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "commands/progress.h"
#include "lib/qunique.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"


typedef struct BTSortArrayContext
{
	FmgrInfo   *sortproc;
	Oid			collation;
	bool		reverse;
} BTSortArrayContext;

typedef struct ScanKeyAttr
{
	ScanKey		skey;
	int			ikey;
} ScanKeyAttr;

static void _bt_setup_array_cmp(IndexScanDesc scan, ScanKey skey, Oid elemtype,
								FmgrInfo *orderproc, FmgrInfo **sortprocp);
static Datum _bt_find_extreme_element(IndexScanDesc scan, ScanKey skey,
									  Oid elemtype, StrategyNumber strat,
									  Datum *elems, int nelems);
static int	_bt_sort_array_elements(ScanKey skey, FmgrInfo *sortproc,
									bool reverse, Datum *elems, int nelems);
static int	_bt_merge_arrays(ScanKey skey, FmgrInfo *sortproc, bool reverse,
							 Datum *elems_orig, int nelems_orig,
							 Datum *elems_next, int nelems_next);
static ScanKey _bt_preprocess_array_keys(IndexScanDesc scan);
static int	_bt_compare_array_elements(const void *a, const void *b, void *arg);
static inline int32 _bt_compare_array_skey(FmgrInfo *orderproc,
										   Datum tupdatum, bool tupnull,
										   Datum arrdatum, ScanKey cur);
static int	_bt_binsrch_array_skey(FmgrInfo *orderproc,
								   bool cur_elem_start, ScanDirection dir,
								   Datum tupdatum, bool tupnull,
								   BTArrayKeyInfo *array, ScanKey cur,
								   int32 *set_elem_result);
static bool _bt_advance_array_keys_increment(IndexScanDesc scan, ScanDirection dir);
static void _bt_rewind_nonrequired_arrays(IndexScanDesc scan, ScanDirection dir);
static bool _bt_tuple_before_array_skeys(IndexScanDesc scan, ScanDirection dir,
										 IndexTuple tuple, bool readpagetup,
										 int sktrig, bool *scanBehind);
static bool _bt_advance_array_keys(IndexScanDesc scan, BTReadPageState *pstate,
								   IndexTuple tuple, int sktrig);
#ifdef USE_ASSERT_CHECKING
static bool _bt_verify_arrays_bt_first(IndexScanDesc scan, ScanDirection dir);
static bool _bt_verify_keys_with_arraykeys(IndexScanDesc scan);
#endif
static bool _bt_compare_scankey_args(IndexScanDesc scan, ScanKey op,
									 ScanKey leftarg, ScanKey rightarg,
									 bool *result);
static bool _bt_fix_scankey_strategy(ScanKey skey, int16 *indoption);
static void _bt_mark_scankey_required(ScanKey skey);
static bool _bt_check_compare(ScanDirection dir, BTScanOpaque so,
							  IndexTuple tuple, int tupnatts, TupleDesc tupdesc,
							  bool arrayKeys, bool prechecked, bool firstmatch,
							  bool *continuescan, int *ikey);
static bool _bt_check_rowcompare(ScanKey skey,
								 IndexTuple tuple, int tupnatts, TupleDesc tupdesc,
								 ScanDirection dir, bool *continuescan);
static int	_bt_keep_natts(Relation rel, IndexTuple lastleft,
						   IndexTuple firstright, BTScanInsert itup_key);


/*
 * _bt_mkscankey
 *		Build an insertion scan key that contains comparison data from itup
 *		as well as comparator routines appropriate to the key datatypes.
 *
 *		The result is intended for use with _bt_compare() and _bt_truncate().
 *		Callers that don't need to fill out the insertion scankey arguments
 *		(e.g. they use an ad-hoc comparison routine, or only need a scankey
 *		for _bt_truncate()) can pass a NULL index tuple.  The scankey will
 *		be initialized as if an "all truncated" pivot tuple was passed
 *		instead.
 *
 *		Note that we may occasionally have to share lock the metapage to
 *		determine whether or not the keys in the index are expected to be
 *		unique (i.e. if this is a "heapkeyspace" index).  We assume a
 *		heapkeyspace index when caller passes a NULL tuple, allowing index
 *		build callers to avoid accessing the non-existent metapage.  We
 *		also assume that the index is _not_ allequalimage when a NULL tuple
 *		is passed; CREATE INDEX callers call _bt_allequalimage() to set the
 *		field themselves.
 */
BTScanInsert
_bt_mkscankey(Relation rel, IndexTuple itup)
{
	BTScanInsert key;
	ScanKey		skey;
	TupleDesc	itupdesc;
	int			indnkeyatts;
	int16	   *indoption;
	int			tupnatts;
	int			i;

	itupdesc = RelationGetDescr(rel);
	indnkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	indoption = rel->rd_indoption;
	tupnatts = itup ? BTreeTupleGetNAtts(itup, rel) : 0;

	Assert(tupnatts <= IndexRelationGetNumberOfAttributes(rel));

	/*
	 * We'll execute search using scan key constructed on key columns.
	 * Truncated attributes and non-key attributes are omitted from the final
	 * scan key.
	 */
	key = palloc(offsetof(BTScanInsertData, scankeys) +
				 sizeof(ScanKeyData) * indnkeyatts);
	if (itup)
		_bt_metaversion(rel, &key->heapkeyspace, &key->allequalimage);
	else
	{
		/* Utility statement callers can set these fields themselves */
		key->heapkeyspace = true;
		key->allequalimage = false;
	}
	key->anynullkeys = false;	/* initial assumption */
	key->nextkey = false;		/* usual case, required by btinsert */
	key->backward = false;		/* usual case, required by btinsert */
	key->keysz = Min(indnkeyatts, tupnatts);
	key->scantid = key->heapkeyspace && itup ?
		BTreeTupleGetHeapTID(itup) : NULL;
	skey = key->scankeys;
	for (i = 0; i < indnkeyatts; i++)
	{
		FmgrInfo   *procinfo;
		Datum		arg;
		bool		null;
		int			flags;

		/*
		 * We can use the cached (default) support procs since no cross-type
		 * comparison can be needed.
		 */
		procinfo = index_getprocinfo(rel, i + 1, BTORDER_PROC);

		/*
		 * Key arguments built from truncated attributes (or when caller
		 * provides no tuple) are defensively represented as NULL values. They
		 * should never be used.
		 */
		if (i < tupnatts)
			arg = index_getattr(itup, i + 1, itupdesc, &null);
		else
		{
			arg = (Datum) 0;
			null = true;
		}
		flags = (null ? SK_ISNULL : 0) | (indoption[i] << SK_BT_INDOPTION_SHIFT);
		ScanKeyEntryInitializeWithInfo(&skey[i],
									   flags,
									   (AttrNumber) (i + 1),
									   InvalidStrategy,
									   InvalidOid,
									   rel->rd_indcollation[i],
									   procinfo,
									   arg);
		/* Record if any key attribute is NULL (or truncated) */
		if (null)
			key->anynullkeys = true;
	}

	/*
	 * In NULLS NOT DISTINCT mode, we pretend that there are no null keys, so
	 * that full uniqueness check is done.
	 */
	if (rel->rd_index->indnullsnotdistinct)
		key->anynullkeys = false;

	return key;
}

/*
 * free a retracement stack made by _bt_search.
 */
void
_bt_freestack(BTStack stack)
{
	BTStack		ostack;

	while (stack != NULL)
	{
		ostack = stack;
		stack = stack->bts_parent;
		pfree(ostack);
	}
}


/*
 *	_bt_preprocess_array_keys() -- Preprocess SK_SEARCHARRAY scan keys
 *
 * If there are any SK_SEARCHARRAY scan keys, deconstruct the array(s) and
 * set up BTArrayKeyInfo info for each one that is an equality-type key.
 * Return modified scan keys as input for further, standard preprocessing.
 *
 * Currently we perform two kinds of preprocessing to deal with redundancies.
 * For inequality array keys, it's sufficient to find the extreme element
 * value and replace the whole array with that scalar value.  This eliminates
 * all but one array key as redundant.  Similarly, we are capable of "merging
 * together" multiple equality array keys (from two or more input scan keys)
 * into a single output scan key that contains only the intersecting array
 * elements.  This can eliminate many redundant array elements, as well as
 * eliminating whole array scan keys as redundant.  It can also allow us to
 * detect contradictory quals early.
 *
 * Note: the reason we need to return a temp scan key array, rather than just
 * scribbling on scan->keyData, is that callers are permitted to call btrescan
 * without supplying a new set of scankey data.
 */
static ScanKey
_bt_preprocess_array_keys(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	rel = scan->indexRelation;
	int			numberOfKeys = scan->numberOfKeys;
	int16	   *indoption = rel->rd_indoption;
	int			numArrayKeys;
	int			prevArrayAtt = -1;
	Oid			prevElemtype = InvalidOid;
	ScanKey		cur;
	MemoryContext oldContext;
	ScanKey		arrayKeyData;	/* modified copy of scan->keyData */

	Assert(numberOfKeys && so->advanceDir == NoMovementScanDirection);

	/* Quick check to see if there are any array keys */
	numArrayKeys = 0;
	for (int i = 0; i < numberOfKeys; i++)
	{
		cur = &scan->keyData[i];
		if (cur->sk_flags & SK_SEARCHARRAY)
		{
			numArrayKeys++;
			Assert(!(cur->sk_flags & (SK_ROW_HEADER | SK_SEARCHNULL | SK_SEARCHNOTNULL)));
			/* If any arrays are null as a whole, we can quit right now. */
			if (cur->sk_flags & SK_ISNULL)
			{
				so->qual_ok = false;
				return NULL;
			}
		}
	}

	/* Quit if nothing to do. */
	if (numArrayKeys == 0)
		return NULL;

	/*
	 * Make a scan-lifespan context to hold array-associated data, or reset it
	 * if we already have one from a previous rescan cycle.
	 */
	if (so->arrayContext == NULL)
		so->arrayContext = AllocSetContextCreate(CurrentMemoryContext,
												 "BTree array context",
												 ALLOCSET_SMALL_SIZES);
	else
		MemoryContextReset(so->arrayContext);

	oldContext = MemoryContextSwitchTo(so->arrayContext);

	/* Create modifiable copy of scan->keyData in the workspace context */
	arrayKeyData = (ScanKey) palloc(numberOfKeys * sizeof(ScanKeyData));
	memcpy(arrayKeyData, scan->keyData, numberOfKeys * sizeof(ScanKeyData));

	/* Allocate space for per-array data in the workspace context */
	so->arrayKeys = (BTArrayKeyInfo *) palloc(numArrayKeys * sizeof(BTArrayKeyInfo));

	/* Allocate space for ORDER procs used during array binary searches */
	so->orderProcs = (FmgrInfo *) palloc(numberOfKeys * sizeof(FmgrInfo));
	so->keyDataMap = (int *) palloc(numberOfKeys * sizeof(int));

	/* Now process each array key */
	numArrayKeys = 0;
	for (int i = 0; i < numberOfKeys; i++)
	{
		FmgrInfo	sortproc;
		FmgrInfo   *sortprocp = &sortproc;
		bool		reverse;
		Oid			elemtype;
		ArrayType  *arrayval;
		int16		elmlen;
		bool		elmbyval;
		char		elmalign;
		int			num_elems;
		Datum	   *elem_values;
		bool	   *elem_nulls;
		int			num_nonnulls;
		int			j;

		cur = &arrayKeyData[i];
		reverse = (indoption[cur->sk_attno - 1] & INDOPTION_DESC) != 0;

		/*
		 * Determine the nominal datatype of the array elements.  We have to
		 * support the convention that sk_subtype == InvalidOid means the
		 * opclass input type; this is a hack to simplify life for
		 * ScanKeyInit().
		 */
		elemtype = cur->sk_subtype;
		if (elemtype == InvalidOid)
			elemtype = rel->rd_opcintype[cur->sk_attno - 1];

		/*
		 * Attributes with equality-type scan keys (including but not limited
		 * to array scan keys) will need a 3-way ORDER proc to perform binary
		 * searches for the next matching array element.  Set that up now.
		 *
		 * Array scan keys with cross-type equality operators will require a
		 * separate same-type ORDER proc for sorting their array.  Otherwise,
		 * sortproc just points to the same proc used during binary searches.
		 */
		if (cur->sk_strategy == BTEqualStrategyNumber)
			_bt_setup_array_cmp(scan, cur, elemtype,
								&so->orderProcs[i], &sortprocp);

		if (!(cur->sk_flags & SK_SEARCHARRAY))
			continue;

		/*
		 * First, deconstruct the array into elements.  Anything allocated
		 * here (including a possibly detoasted array value) is in the
		 * workspace context.
		 */
		arrayval = DatumGetArrayTypeP(cur->sk_argument);
		/* We could cache this data, but not clear it's worth it */
		get_typlenbyvalalign(ARR_ELEMTYPE(arrayval),
							 &elmlen, &elmbyval, &elmalign);
		deconstruct_array(arrayval,
						  ARR_ELEMTYPE(arrayval),
						  elmlen, elmbyval, elmalign,
						  &elem_values, &elem_nulls, &num_elems);

		/*
		 * Compress out any null elements.  We can ignore them since we assume
		 * all btree operators are strict.
		 */
		num_nonnulls = 0;
		for (j = 0; j < num_elems; j++)
		{
			if (!elem_nulls[j])
				elem_values[num_nonnulls++] = elem_values[j];
		}

		/* We could pfree(elem_nulls) now, but not worth the cycles */

		/* If there's no non-nulls, the scan qual is unsatisfiable */
		if (num_nonnulls == 0)
		{
			so->qual_ok = false;
			return NULL;
		}

		/*
		 * If the comparison operator is not equality, then the array qual
		 * degenerates to a simple comparison against the smallest or largest
		 * non-null array element, as appropriate.
		 */
		switch (cur->sk_strategy)
		{
			case BTLessStrategyNumber:
			case BTLessEqualStrategyNumber:
				cur->sk_argument =
					_bt_find_extreme_element(scan, cur, elemtype,
											 BTGreaterStrategyNumber,
											 elem_values, num_nonnulls);
				continue;
			case BTEqualStrategyNumber:
				/* proceed with rest of loop */
				break;
			case BTGreaterEqualStrategyNumber:
			case BTGreaterStrategyNumber:
				cur->sk_argument =
					_bt_find_extreme_element(scan, cur, elemtype,
											 BTLessStrategyNumber,
											 elem_values, num_nonnulls);
				continue;
			default:
				elog(ERROR, "unrecognized StrategyNumber: %d",
					 (int) cur->sk_strategy);
				break;
		}

		/*
		 * Sort the non-null elements and eliminate any duplicates.  We must
		 * sort in the same ordering used by the index column, so that the
		 * arrays can be advanced in lockstep with the scan's progress through
		 * the index's key space.
		 */
		Assert(cur->sk_strategy == BTEqualStrategyNumber);
		num_elems = _bt_sort_array_elements(cur, sortprocp, reverse,
											elem_values, num_nonnulls);

		/*
		 * If this scan key is semantically equivalent to a previous equality
		 * operator array scan key, merge the two arrays together to eliminate
		 * redundant non-intersecting elements (and whole scan keys).
		 *
		 * We don't support merging arrays (for same-attribute scankeys) when
		 * the array element types don't match.  Note that this is orthogonal
		 * to whether cross-type operators are used (whether the element type
		 * matches or fails to match the on-disk/opclass type is irrelevant).
		 */
		if (prevArrayAtt == cur->sk_attno && prevElemtype == elemtype)
		{
			BTArrayKeyInfo *prev = &so->arrayKeys[numArrayKeys - 1];

			Assert(arrayKeyData[prev->scan_key].sk_attno == cur->sk_attno);
			Assert(arrayKeyData[prev->scan_key].sk_func.fn_oid ==
				   cur->sk_func.fn_oid);
			Assert(arrayKeyData[prev->scan_key].sk_collation ==
				   cur->sk_collation);

			num_elems = _bt_merge_arrays(cur, sortprocp, reverse,
										 prev->elem_values, prev->num_elems,
										 elem_values, num_elems);

			pfree(elem_values);

			/*
			 * If there are no intersecting elements left from merging this
			 * array into the previous array on the same attribute, the scan
			 * qual is unsatisfiable
			 */
			if (num_elems == 0)
			{
				so->qual_ok = false;
				return NULL;
			}

			/*
			 * Lower the number of elements from the previous array.  This
			 * scan key/array is redundant.  Dealing with that is finalized
			 * within _bt_preprocess_keys.
			 */
			prev->num_elems = num_elems;
			cur->sk_strategy = InvalidStrategy; /* for _bt_preprocess_keys */
			continue;
		}

		/*
		 * And set up the BTArrayKeyInfo data.
		 */
		so->arrayKeys[numArrayKeys].scan_key = i;	/* will be adjusted later */
		so->arrayKeys[numArrayKeys].num_elems = num_elems;
		so->arrayKeys[numArrayKeys].elem_values = elem_values;
		numArrayKeys++;
		prevArrayAtt = cur->sk_attno;
		prevElemtype = elemtype;
	}

	so->numArrayKeys = numArrayKeys;

	MemoryContextSwitchTo(oldContext);

	return arrayKeyData;
}

/*
 * _bt_setup_array_cmp() -- Set up array comparison functions
 *
 * Sets ORDER proc in caller's orderproc argument, which is used during binary
 * searches of arrays during the index scan.  Also sets a same-type ORDER proc
 * in caller's *sortprocp argument.
 *
 * Caller should pass an orderproc pointing to space that'll store the ORDER
 * proc for the scan, and a *sortprocp pointing to its own separate space.
 *
 * In the common case where we don't need to deal with cross-type operators,
 * only one ORDER proc is actually required by caller.  We'll set *sortprocp
 * to point to the same memory that caller's orderproc continues to point to.
 * Otherwise, *sortprocp will continue to point to separate memory, which
 * we'll initialize separately (with an "(elemtype, elemtype)" ORDER proc that
 * can be used to sort arrays).
 *
 * Array preprocessing calls here with all equality strategy scan keys,
 * including any that don't use an array at all.  See _bt_advance_array_keys
 * for an explanation of why we need to treat these as degenerate single-value
 * arrays when the scan advances its arrays.
 */
static void
_bt_setup_array_cmp(IndexScanDesc scan, ScanKey skey, Oid elemtype,
					FmgrInfo *orderproc, FmgrInfo **sortprocp)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	rel = scan->indexRelation;
	RegProcedure cmp_proc;
	Oid			opclasstype = rel->rd_opcintype[skey->sk_attno - 1];

	Assert(skey->sk_strategy == BTEqualStrategyNumber);
	Assert(OidIsValid(elemtype));

	/*
	 * Look up the appropriate comparison function in the opfamily.  This must
	 * use the opclass type as its left hand arg type, and the array element
	 * as its right hand arg type (since binary searches search for the array
	 * value that best matches the next on-disk index tuple for the scan).
	 *
	 * Note: it's possible that this would fail, if the opfamily lacks the
	 * required cross-type ORDER proc.  But this is no different to the case
	 * where _bt_first fails to find an ORDER proc for its insertion scan key.
	 */
	cmp_proc = get_opfamily_proc(rel->rd_opfamily[skey->sk_attno - 1],
								 opclasstype, elemtype, BTORDER_PROC);
	if (!RegProcedureIsValid(cmp_proc))
		elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
			 BTORDER_PROC, opclasstype, elemtype,
			 skey->sk_attno, RelationGetRelationName(rel));

	/* Set ORDER proc for caller */
	fmgr_info_cxt(cmp_proc, orderproc, so->arrayContext);

	if (opclasstype == elemtype || !(skey->sk_flags & SK_SEARCHARRAY))
	{
		/*
		 * A second opfamily support proc lookup can be avoided in the common
		 * case where the ORDER proc used for the scan's binary searches uses
		 * the opclass/on-disk datatype for both its left and right arguments.
		 *
		 * Also avoid a separate lookup whenever scan key lacks an array.
		 * There is nothing for caller to sort anyway, but be consistent.
		 */
		*sortprocp = orderproc;
		return;
	}

	/*
	 * Look up the appropriate same-type comparison function in the opfamily.
	 *
	 * Note: it's possible that this would fail, if the opfamily is
	 * incomplete, but it seems quite unlikely that an opfamily would omit
	 * non-cross-type support functions for any datatype that it supports at
	 * all.
	 */
	cmp_proc = get_opfamily_proc(rel->rd_opfamily[skey->sk_attno - 1],
								 elemtype, elemtype, BTORDER_PROC);
	if (!RegProcedureIsValid(cmp_proc))
		elog(ERROR, "missing support function %d(%u,%u) for attribute %d of index \"%s\"",
			 BTORDER_PROC, elemtype, elemtype,
			 skey->sk_attno, RelationGetRelationName(rel));

	/* Set same-type ORDER proc for caller */
	fmgr_info_cxt(cmp_proc, *sortprocp, so->arrayContext);
}

/*
 * _bt_find_extreme_element() -- get least or greatest array element
 *
 * scan and skey identify the index column, whose opfamily determines the
 * comparison semantics.  strat should be BTLessStrategyNumber to get the
 * least element, or BTGreaterStrategyNumber to get the greatest.
 */
static Datum
_bt_find_extreme_element(IndexScanDesc scan, ScanKey skey, Oid elemtype,
						 StrategyNumber strat,
						 Datum *elems, int nelems)
{
	Relation	rel = scan->indexRelation;
	Oid			cmp_op;
	RegProcedure cmp_proc;
	FmgrInfo	flinfo;
	Datum		result;
	int			i;

	/*
	 * Look up the appropriate comparison operator in the opfamily.
	 *
	 * Note: it's possible that this would fail, if the opfamily is
	 * incomplete, but it seems quite unlikely that an opfamily would omit
	 * non-cross-type comparison operators for any datatype that it supports
	 * at all.
	 */
	Assert(skey->sk_strategy != BTEqualStrategyNumber);
	Assert(OidIsValid(elemtype));
	cmp_op = get_opfamily_member(rel->rd_opfamily[skey->sk_attno - 1],
								 elemtype,
								 elemtype,
								 strat);
	if (!OidIsValid(cmp_op))
		elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
			 strat, elemtype, elemtype,
			 rel->rd_opfamily[skey->sk_attno - 1]);
	cmp_proc = get_opcode(cmp_op);
	if (!RegProcedureIsValid(cmp_proc))
		elog(ERROR, "missing oprcode for operator %u", cmp_op);

	fmgr_info(cmp_proc, &flinfo);

	Assert(nelems > 0);
	result = elems[0];
	for (i = 1; i < nelems; i++)
	{
		if (DatumGetBool(FunctionCall2Coll(&flinfo,
										   skey->sk_collation,
										   elems[i],
										   result)))
			result = elems[i];
	}

	return result;
}

/*
 * _bt_sort_array_elements() -- sort and de-dup array elements
 *
 * The array elements are sorted in-place, and the new number of elements
 * after duplicate removal is returned.
 *
 * skey identifies the index column whose opfamily determines the comparison
 * semantics, and sortproc is a corresponding ORDER proc.  If reverse is true,
 * we sort in descending order.
 *
 * Note: sortproc arg must be an ORDER proc suitable for sorting: it must
 * compare arguments that are both of the same type as the array elements
 * being sorted (even during scans that perform binary searches against the
 * arrays using distinct cross-type ORDER procs).
 */
static int
_bt_sort_array_elements(ScanKey skey, FmgrInfo *sortproc, bool reverse,
						Datum *elems, int nelems)
{
	BTSortArrayContext cxt;

	if (nelems <= 1)
		return nelems;			/* no work to do */

	/* Sort the array elements */
	cxt.sortproc = sortproc;
	cxt.collation = skey->sk_collation;
	cxt.reverse = reverse;
	qsort_arg(elems, nelems, sizeof(Datum),
			  _bt_compare_array_elements, &cxt);

	/* Now scan the sorted elements and remove duplicates */
	return qunique_arg(elems, nelems, sizeof(Datum),
					   _bt_compare_array_elements, &cxt);
}

/*
 * _bt_merge_arrays() -- merge together duplicate array keys
 *
 * Both scan keys have array elements that have already been sorted and
 * deduplicated.
 */
static int
_bt_merge_arrays(ScanKey skey, FmgrInfo *sortproc, bool reverse,
				 Datum *elems_orig, int nelems_orig,
				 Datum *elems_next, int nelems_next)
{
	BTSortArrayContext cxt;
	Datum	   *merged = palloc(sizeof(Datum) * Min(nelems_orig, nelems_next));
	int			merged_nelems = 0;

	/*
	 * Incrementally copy the original array into a temp buffer, skipping over
	 * any items that are missing from the "next" array
	 */
	cxt.sortproc = sortproc;
	cxt.collation = skey->sk_collation;
	cxt.reverse = reverse;
	for (int i = 0; i < nelems_orig; i++)
	{
		Datum	   *elem = elems_orig + i;

		if (bsearch_arg(elem, elems_next, nelems_next, sizeof(Datum),
						_bt_compare_array_elements, &cxt))
			merged[merged_nelems++] = *elem;
	}

	/*
	 * Overwrite the original array with temp buffer so that we're only left
	 * with intersecting array elements
	 */
	memcpy(elems_orig, merged, merged_nelems * sizeof(Datum));
	pfree(merged);

	return merged_nelems;
}

/*
 * qsort_arg comparator for sorting array elements
 */
static int
_bt_compare_array_elements(const void *a, const void *b, void *arg)
{
	Datum		da = *((const Datum *) a);
	Datum		db = *((const Datum *) b);
	BTSortArrayContext *cxt = (BTSortArrayContext *) arg;
	int32		compare;

	compare = DatumGetInt32(FunctionCall2Coll(cxt->sortproc,
											  cxt->collation,
											  da, db));
	if (cxt->reverse)
		INVERT_COMPARE_RESULT(compare);
	return compare;
}

/*
 * _bt_compare_array_skey() -- apply array comparison function
 *
 * Compares caller's tuple attribute value to a scan key/array element.
 * Helper function used during binary searches of SK_SEARCHARRAY arrays.
 *
 *		This routine returns:
 *			<0 if tupdatum < arrdatum;
 *			 0 if tupdatum == arrdatum;
 *			>0 if tupdatum > arrdatum.
 *
 * This is essentially the same interface as _bt_compare: both functions
 * compare the value that they're searching for to a binary search pivot.
 * However, unlike _bt_compare, this function's "tuple argument" comes first,
 * while its "array/scankey argument" comes second.
*/
static inline int32
_bt_compare_array_skey(FmgrInfo *orderproc,
					   Datum tupdatum, bool tupnull,
					   Datum arrdatum, ScanKey cur)
{
	int32		result = 0;

	Assert(cur->sk_strategy == BTEqualStrategyNumber);

	if (tupnull)				/* NULL tupdatum */
	{
		if (cur->sk_flags & SK_ISNULL)
			result = 0;			/* NULL "=" NULL */
		else if (cur->sk_flags & SK_BT_NULLS_FIRST)
			result = -1;		/* NULL "<" NOT_NULL */
		else
			result = 1;			/* NULL ">" NOT_NULL */
	}
	else if (cur->sk_flags & SK_ISNULL) /* NOT_NULL tupdatum, NULL arrdatum */
	{
		if (cur->sk_flags & SK_BT_NULLS_FIRST)
			result = 1;			/* NOT_NULL ">" NULL */
		else
			result = -1;		/* NOT_NULL "<" NULL */
	}
	else
	{
		/*
		 * Like _bt_compare, we need to be careful of cross-type comparisons,
		 * so the left value has to be the value that came from an index tuple
		 */
		result = DatumGetInt32(FunctionCall2Coll(orderproc, cur->sk_collation,
												 tupdatum, arrdatum));

		/*
		 * We flip the sign by following the obvious rule: flip whenever the
		 * column is a DESC column.
		 *
		 * _bt_compare does it the wrong way around (flip when *ASC*) in order
		 * to compensate for passing its orderproc arguments backwards.  We
		 * don't need to play these games because we find it natural to pass
		 * tupdatum as the left value (and arrdatum as the right value).
		 */
		if (cur->sk_flags & SK_BT_DESC)
			INVERT_COMPARE_RESULT(result);
	}

	return result;
}

/*
 * _bt_binsrch_array_skey() -- Binary search for next matching array key
 *
 * Returns an index to the first array element >= caller's tupdatum argument.
 * This convention is more natural for forwards scan callers, but that can't
 * really matter to backwards scan callers.  Both callers require handling for
 * the case where the match we return is < tupdatum, and symmetric handling
 * for the case where our best match is > tupdatum.
 *
 * Also sets *set_elem_result to whatever _bt_compare_array_skey returned when
 * we compared the returned array element to caller's tupdatum argument.  This
 * helps our caller to determine how advancing its array (to the element we'll
 * return an offset to) might need to carry to higher order arrays.
 *
 * cur_elem_start indicates if the binary search should begin at the array's
 * current element (or have the current element as an upper bound for backward
 * scans).  It's safe for searches against required scan key arrays to reuse
 * earlier search bounds like this because such arrays always advance in
 * lockstep with the index scan's progress through the index's key space.
 */
static int
_bt_binsrch_array_skey(FmgrInfo *orderproc,
					   bool cur_elem_start, ScanDirection dir,
					   Datum tupdatum, bool tupnull,
					   BTArrayKeyInfo *array, ScanKey cur,
					   int32 *set_elem_result)
{
	int			low_elem = 0,
				mid_elem = -1,
				high_elem = array->num_elems - 1,
				result = 0;

	Assert(cur->sk_flags & SK_SEARCHARRAY);
	Assert(cur->sk_strategy == BTEqualStrategyNumber);

	if (cur_elem_start)
	{
		if (ScanDirectionIsForward(dir))
			low_elem = array->cur_elem;
		else
			high_elem = array->cur_elem;
	}

	while (high_elem > low_elem)
	{
		Datum		arrdatum;

		mid_elem = low_elem + ((high_elem - low_elem) / 2);
		arrdatum = array->elem_values[mid_elem];

		result = _bt_compare_array_skey(orderproc, tupdatum, tupnull,
										arrdatum, cur);

		if (result == 0)
		{
			/*
			 * It's safe to quit as soon as we see an equal array element.
			 * This often saves an extra comparison or two...
			 */
			low_elem = mid_elem;
			break;
		}

		if (result > 0)
			low_elem = mid_elem + 1;
		else
			high_elem = mid_elem;
	}

	/*
	 * ...but our caller also cares about how its searched-for tuple datum
	 * compares to the low_elem datum.  Must always set *set_elem_result with
	 * the result of that comparison specifically.
	 */
	if (low_elem != mid_elem)
		result = _bt_compare_array_skey(orderproc, tupdatum, tupnull,
										array->elem_values[low_elem], cur);

	*set_elem_result = result;

	return low_elem;
}

/*
 * _bt_start_array_keys() -- Initialize array keys at start of a scan
 *
 * Set up the cur_elem counters and fill in the first sk_argument value for
 * each array scankey.
 */
void
_bt_start_array_keys(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	int			i;

	Assert(so->numArrayKeys);
	Assert(so->qual_ok);

	for (i = 0; i < so->numArrayKeys; i++)
	{
		BTArrayKeyInfo *curArrayKey = &so->arrayKeys[i];
		ScanKey		skey = &so->keyData[curArrayKey->scan_key];

		Assert(curArrayKey->num_elems > 0);
		Assert(skey->sk_flags & SK_SEARCHARRAY);

		if (ScanDirectionIsBackward(dir))
			curArrayKey->cur_elem = curArrayKey->num_elems - 1;
		else
			curArrayKey->cur_elem = 0;
		skey->sk_argument = curArrayKey->elem_values[curArrayKey->cur_elem];
	}
}

/*
 * _bt_advance_array_keys_increment() -- Advance to next set of array elements
 *
 * Advances the array keys by a single increment in the current scan
 * direction.  When there are multiple array keys this can roll over from the
 * lowest order array to higher order arrays.
 *
 * Returns true if there is another set of values to consider, false if not.
 * On true result, the scankeys are initialized with the next set of values.
 * On false result, the scankeys stay the same, and the array keys are not
 * advanced (every array remains at its final element for scan direction).
 */
static bool
_bt_advance_array_keys_increment(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	/*
	 * We must advance the last array key most quickly, since it will
	 * correspond to the lowest-order index column among the available
	 * qualifications.  Rolling over like this is necessary to ensure correct
	 * ordering of output when there are multiple array keys.
	 */
	for (int i = so->numArrayKeys - 1; i >= 0; i--)
	{
		BTArrayKeyInfo *curArrayKey = &so->arrayKeys[i];
		ScanKey		skey = &so->keyData[curArrayKey->scan_key];
		int			cur_elem = curArrayKey->cur_elem;
		int			num_elems = curArrayKey->num_elems;
		bool		rolled = false;

		if (ScanDirectionIsForward(dir) && ++cur_elem >= num_elems)
		{
			cur_elem = 0;
			rolled = true;
		}
		else if (ScanDirectionIsBackward(dir) && --cur_elem < 0)
		{
			cur_elem = num_elems - 1;
			rolled = true;
		}

		curArrayKey->cur_elem = cur_elem;
		skey->sk_argument = curArrayKey->elem_values[cur_elem];
		if (!rolled)
			return true;

		/* Need to advance next array key, if any */
	}

	/*
	 * The array keys are now exhausted.
	 *
	 * There isn't actually a distinct state that represents array exhaustion,
	 * since index scans don't always end when btgettuple returns "false". The
	 * scan direction might be reversed, or the scan might yet have its last
	 * saved position restored.
	 *
	 * Restore the array keys to the state they were in immediately before we
	 * were called.  This ensures that the arrays can only ever ratchet in the
	 * scan's current direction.  Without this, scans would overlook matching
	 * tuples if and when the scan's direction was subsequently reversed.
	 */
	_bt_start_array_keys(scan, -dir);

	return false;
}

/*
 * _bt_rewind_nonrequired_arrays() -- Rewind non-required arrays
 *
 * Called when _bt_advance_array_keys decides to start a new primitive index
 * scan on the basis of the current scan position being before the position
 * that _bt_first is capable of repositioning the scan to by applying an
 * inequality operator required in the opposite-to-scan direction only.
 *
 * Although equality strategy scan keys (for both arrays and non-arrays alike)
 * are either marked required in both directions or in neither direction,
 * there is a sense in which non-required arrays behave like required arrays.
 * With a qual such as "WHERE a IN (100, 200) AND b >= 3 AND c IN (5, 6, 7)",
 * the scan key on "c" is non-required, but nevertheless enables positioning
 * the scan at the first tuple >= "(100, 3, 5)" on the leaf level during the
 * first descent of the tree by _bt_first.  Later on, there could also be a
 * second descent, that places the scan right before tuples >= "(200, 3, 5)".
 * _bt_first must never be allowed to build an insertion scan key whose "c"
 * entry is set to a value other than 5, the "c" array's first element/value.
 * (Actually, it's the first in the current scan direction.  This example uses
 * a forward scan.)
 *
 * Calling here resets the array scan key elements for the scan's non-required
 * arrays.  This is strictly necessary for correctness in a subset of cases
 * involving "required in opposite direction"-triggered primitive index scans.
 * Not all callers are at risk of _bt_first using a non-required array like
 * this, but advancement always resets the arrays, just to keep things simple.
 * Array advancement even makes sure to reset non-required arrays like this
 * during scans that have no inequalities.  Advancement won't ever need to
 * call here, though that's just because it is all handled indirectly instead.
 *
 * Note: _bt_verify_arrays_bt_first is called by an assertion to enforce that
 * everybody got this right.  Note that this only happens between each call to
 * _bt_first (never after the final _bt_first call).
 */
static void
_bt_rewind_nonrequired_arrays(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	int			arrayidx = 0;
	bool		arrays_advanced = false;

	for (int ikey = 0; ikey < so->numberOfKeys; ikey++)
	{
		ScanKey		cur = so->keyData + ikey;
		BTArrayKeyInfo *array = NULL;
		int			first_elem_dir;

		if (!(cur->sk_flags & SK_SEARCHARRAY) &&
			cur->sk_strategy != BTEqualStrategyNumber)
			continue;

		array = &so->arrayKeys[arrayidx++];
		Assert(array->scan_key == ikey);

		if ((cur->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)))
			continue;

		if (ScanDirectionIsForward(dir) || !array)
			first_elem_dir = 0;
		else
			first_elem_dir = array->num_elems - 1;

		if (array->cur_elem != first_elem_dir)
		{
			array->cur_elem = first_elem_dir;
			cur->sk_argument = array->elem_values[first_elem_dir];
			arrays_advanced = true;
		}
	}

	if (arrays_advanced)
		so->advanceDir = dir;
}

/*
 * _bt_rewind_array_keys() -- Handle array keys during btrestrpos
 *
 * Restore the array keys to the start of the key space for the current scan
 * direction as of the last time the arrays advanced.
 *
 * Once the scan reaches _bt_advance_array_keys, the arrays will advance up to
 * the key space of the actual tuples from the mark position's leaf page.
 */
void
_bt_rewind_array_keys(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	Assert(so->numArrayKeys);
	Assert(!ScanDirectionIsNoMovement(so->advanceDir));
	Assert(so->qual_ok && so->numArrayKeys);

	/*
	 * First reinitialize the array keys to the first elements for the scan
	 * direction at the time that the arrays last advanced
	 */
	_bt_start_array_keys(scan, so->advanceDir);

	/*
	 * Next invert the scan direction as of the last time the array keys
	 * advanced.
	 *
	 * This prevents _bt_steppage from fully trusting currPos.moreRight and
	 * currPos.moreLeft in cases where _bt_readpage/_bt_checkkeys don't get
	 * the opportunity to consider advancing the array keys as expected.
	 */
	if (ScanDirectionIsForward(so->advanceDir))
		so->advanceDir = BackwardScanDirection;
	else
		so->advanceDir = ForwardScanDirection;

	so->scanBehind = true;
	so->needPrimScan = false;
}

/*
 * _bt_tuple_before_array_skeys() -- determine if tuple advances array keys
 *
 * We always compare the tuple using the current array keys (which we assume
 * are already set in so->keyData[]).  readpagetup indicates if tuple is the
 * scan's current _bt_readpage-wise tuple.
 *
 * readpagetup callers must only call here when _bt_check_compare already set
 * continuescan=false.  We help these callers deal with _bt_check_compare's
 * inability to distinguishing between the < and > cases (it uses equality
 * operator scan keys, whereas we use 3-way ORDER procs).
 *
 * Returns true when caller passes a tuple that is < the current set of array
 * keys for the most significant non-equal column/scan key (or > for backwards
 * scans).  This happens to readpagetup callers when tuple is still before the
 * start of matches for the scan's current required array keys.
 *
 * Returns false when caller's tuple is >= the current array keys (or <=, in
 * the case of backwards scans).  This happens to readpagetup callers when the
 * scan has reached the point of needing its array keys advanced.
 *
 * As an optimization, readpagetup callers pass a _bt_check_compare-set sktrig
 * value to indicate which scan key triggered _bt_checkkeys to recheck with us
 * (!readpagetup callers must always pass sktrig=0).  This allows us to avoid
 * wastefully checking earlier scan keys that _bt_check_compare already found
 * to be satisfied by the current qual/set of array keys.  If sktrig indicates
 * a non-required array that _bt_check_compare just set continuescan=false for
 * (see _bt_check_compare for an explanation), then we always return false.
 *
 * !readpagetup callers optionally pass us *scanBehind, which tracks whether
 * any missing truncated attributes might have affected array advancement
 * (compared to what would happen if it was shown the first non-pivot tuple on
 * the page to the right of caller's finaltup/high key tuple instead).  It's
 * only possible that we'll set *scanBehind to true when caller passes us a
 * pivot tuple (with truncated attributes) that we return false for.
 */
static bool
_bt_tuple_before_array_skeys(IndexScanDesc scan, ScanDirection dir,
							 IndexTuple tuple, bool readpagetup, int sktrig,
							 bool *scanBehind)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	rel = scan->indexRelation;
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			ntupatts = BTreeTupleGetNAtts(tuple, rel);

	Assert(so->numArrayKeys);
	Assert(so->numberOfKeys);
	Assert(!so->needPrimScan);
	Assert(sktrig == 0 || readpagetup);
	Assert(!readpagetup || scanBehind == NULL);

	if (scanBehind)
		*scanBehind = false;

	for (; sktrig < so->numberOfKeys; sktrig++)
	{
		ScanKey		cur = so->keyData + sktrig;
		FmgrInfo   *orderproc;
		Datum		tupdatum;
		bool		tupnull;
		int32		result;

		/*
		 * Once we reach a non-required scan key, we're completely done.
		 *
		 * Note: we deliberately don't consider the scan direction here.
		 * _bt_advance_array_keys caller requires that we track *scanBehind
		 * without concern for scan direction.
		 */
		if ((cur->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) == 0)
		{
			Assert(!readpagetup || (cur->sk_strategy == BTEqualStrategyNumber &&
									(cur->sk_flags & SK_SEARCHARRAY)));
			return false;
		}

		/* readpagetup calls require one ORDER proc comparison (at most) */
		Assert(!readpagetup || cur == so->keyData + sktrig);

		if (cur->sk_attno > ntupatts)
		{
			Assert(!readpagetup);

			/*
			 * When we reach a high key's truncated attribute, assume that the
			 * tuple attribute's value is >= the scan's equality constraint
			 * scan keys (but set *scanBehind to let interested callers know
			 * that a truncated attribute might have affected our answer).
			 */
			if (scanBehind)
				*scanBehind = true;

			return false;
		}

		/*
		 * Inequality strategy scan keys (that are required in current scan
		 * direction) can only be evaluated by _bt_check_compare
		 */
		if (cur->sk_strategy != BTEqualStrategyNumber)
		{
			/*
			 * Give up right away when _bt_check_compare indicated that a
			 * required inequality scan key wasn't satisfied
			 */
			if (readpagetup)
				return false;

			/*
			 * Otherwise we can't give up.  There can't be any required
			 * equality strategy scan keys after this one, but we still need
			 * to maintain *scanBehind for any later required inequality keys.
			 */
			continue;
		}

		orderproc = &so->orderProcs[so->keyDataMap[sktrig]];
		tupdatum = index_getattr(tuple, cur->sk_attno, itupdesc, &tupnull);

		result = _bt_compare_array_skey(orderproc, tupdatum, tupnull,
										cur->sk_argument, cur);

		/*
		 * Does this comparison indicate that caller must _not_ advance the
		 * scan's arrays just yet?
		 */
		if ((ScanDirectionIsForward(dir) && result < 0) ||
			(ScanDirectionIsBackward(dir) && result > 0))
			return true;

		/*
		 * Does this comparison indicate that caller should now advance the
		 * scan's arrays?  (Must be if we get here during a readpagetup call.)
		 */
		if (readpagetup || result != 0)
		{
			Assert(result != 0);
			return false;
		}

		/*
		 * Inconclusive -- need to check later scan keys, too.
		 *
		 * This must be a finaltup precheck, or a call made from an assertion.
		 */
		Assert(result == 0);
		Assert(!readpagetup);
	}

	return false;
}

/*
 * _bt_start_prim_scan() -- start scheduled primitive index scan?
 *
 * Returns true if _bt_checkkeys scheduled another primitive index scan, just
 * as the last one ended.  Otherwise returns false, indicating that the array
 * keys are now fully exhausted.
 *
 * Only call here during scans with one or more equality type array scan keys,
 * after _bt_first or _bt_next return false.
 */
bool
_bt_start_prim_scan(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	Assert(so->numArrayKeys);
	Assert(so->advanceDir == dir || !so->qual_ok);

	/*
	 * Array keys are advanced within _bt_checkkeys when the scan reaches the
	 * leaf level (more precisely, they're advanced when the scan reaches the
	 * end of each distinct set of array elements).  This process avoids
	 * repeat access to leaf pages (across multiple primitive index scans) by
	 * advancing the scan's array keys when it allows the primitive index scan
	 * to find nearby matching tuples (or when it eliminates ranges of array
	 * key space that can't possibly be satisfied by any index tuple).
	 *
	 * _bt_checkkeys sets a simple flag variable to schedule another primitive
	 * index scan.  The flag tells us what to do.
	 *
	 * We cannot rely on _bt_first always reaching _bt_checkkeys.  There are
	 * various cases where that won't happen.  For example, if the index is
	 * completely empty, then _bt_first won't call _bt_readpage/_bt_checkkeys.
	 * We also don't expect a call to _bt_checkkeys during searches for a
	 * non-existent value that happens to be lower/higher than any existing
	 * value in the index.
	 *
	 * We don't require special handling for these cases -- we don't need to
	 * be explicitly instructed to _not_ perform another primitive index scan.
	 * It's up to code under the control of _bt_first to always set the flag
	 * when another primitive index scan will be required.
	 *
	 * This works correctly, even with the tricky cases listed above, which
	 * all involve access to leaf pages "near the boundaries of the key space"
	 * (whether it's from a leftmost/rightmost page, or an imaginary empty
	 * leaf root page).  If _bt_checkkeys cannot be reached by a primitive
	 * index scan for one set of array keys, then it also won't be reached for
	 * any later set ("later" in terms of the direction that we scan the index
	 * and advance the arrays).  The array keys won't have advanced in these
	 * cases, but that's the correct behavior (even _bt_advance_array_keys
	 * won't always advance the arrays at the point they become "exhausted").
	 */
	if (so->needPrimScan)
	{
		Assert(_bt_verify_arrays_bt_first(scan, dir));

		/* Flag was set -- must call _bt_first again */
		so->needPrimScan = false;
		so->scanBehind = false;
		if (scan->parallel_scan != NULL)
			_bt_parallel_next_primitive_scan(scan);

		return true;
	}

	/* The top-level index scan ran out of tuples in this scan direction */
	if (scan->parallel_scan != NULL)
		_bt_parallel_done(scan);

	return false;
}

/*
 * _bt_advance_array_keys() -- Advance array elements using a tuple
 *
 * The scan always gets a new qual as a consequence of calling here (except
 * when we determine that the top-level scan has run out of matching tuples).
 * All later _bt_check_compare calls also use the same new qual that was first
 * used here (at least until the next call here advances the keys once again).
 * It's convenient to structure _bt_check_compare rechecks of caller's tuple
 * (using the new qual) as one the steps of advancing the scan's array keys,
 * so this function works as a wrapper around _bt_check_compare.
 *
 * Like _bt_check_compare, we'll set pstate.continuescan on behalf of the
 * caller, and return a boolean indicating if caller's tuple satisfies the
 * scan's new qual.  But unlike _bt_check_compare, we set so->needPrimScan
 * when we set continuescan=false, indicating if a new primitive index scan
 * has been scheduled (otherwise, the top-level scan has run out of tuples in
 * the current scan direction).
 *
 * Caller must use _bt_tuple_before_array_skeys to determine if the current
 * place in the scan is >= the current array keys _before_ calling here.
 * We're responsible for ensuring that caller's tuple is <= the newly advanced
 * required array keys once we return.  We try to find an exact match, but
 * failing that we'll advance the array keys to whatever set of array elements
 * comes next in the key space for the current scan direction.  Required array
 * keys "ratchet forwards" (or backwards).  They can only advance as the scan
 * itself advances through the index/key space.
 *
 * (The rules are the same for backwards scans, except that the operators are
 * flipped: just replace the precondition's >= operator with a <=, and the
 * postcondition's <= operator with with a >=.  In other words, just swap the
 * precondition with the postcondition.)
 *
 * We also deal with "advancing" non-required arrays here.  Sometimes that'll
 * be the sole reason for calling here.  These calls are the only exception to
 * the general rule about always advancing the array keys.  (That rule only
 * applies when a required scan key was found to be unsatisfied.)
 *
 * Note that we deal with non-array required equality strategy scan keys as
 * degenerate single element arrays here.  Obviously, they can never really
 * advance in the way that real arrays can, but they must still affect how we
 * advance real array scan keys (exactly like true array equality scan keys).
 * We have to keep around a 3-way ORDER proc for these (using the "=" operator
 * won't do), since in general whether the tuple is < or > _any_ unsatisfied
 * required equality key influences how the scan's real arrays must advance.
 *
 * Note also that we may sometimes need to advance the array keys when the
 * existing array keys are already an exact match for every corresponding
 * value from caller's tuple.  This is how we deal with inequalities that are
 * required in the current scan direction.  They can advance the array keys
 * here, even though they don't influence the initial positioning strategy
 * within _bt_first (only inequalities required in the _opposite_ direction to
 * the scan influence _bt_first in this way).  When sktrig (which is an offset
 * to the unsatisfied scan key set by _bt_check_compare) is for a required
 * inequality scan key, we'll perform array key advancement.
 */
static bool
_bt_advance_array_keys(IndexScanDesc scan, BTReadPageState *pstate,
					   IndexTuple tuple, int sktrig)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Relation	rel = scan->indexRelation;
	ScanDirection dir = pstate->dir;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			arrayidx = 0,
				ntupatts = BTreeTupleGetNAtts(tuple, rel);
	bool		arrays_advanced = false,
				arrays_exhausted,
				beyond_end_advance = false,
				sktrig_required = false,
				has_required_opposite_direction_only = false,
				oppodir_inequality_sktrig = false,
				all_required_satisfied = true;

	/*
	 * Precondition array state assertions
	 */
	Assert(!so->needPrimScan && so->advanceDir == dir);
	Assert(_bt_verify_keys_with_arraykeys(scan));
	Assert(!_bt_tuple_before_array_skeys(scan, dir, tuple, false, 0, NULL));

	so->scanBehind = false;		/* reset */

	for (int ikey = 0; ikey < so->numberOfKeys; ikey++)
	{
		ScanKey		cur = so->keyData + ikey;
		FmgrInfo   *orderproc;
		BTArrayKeyInfo *array = NULL;
		Datum		tupdatum;
		bool		required = false,
					required_opposite_direction_only = false,
					tupnull;
		int32		result;
		int			set_elem = 0;

		if (cur->sk_strategy == BTEqualStrategyNumber)
		{
			/* Manage array state */
			if (cur->sk_flags & SK_SEARCHARRAY)
			{
				array = &so->arrayKeys[arrayidx++];
				Assert(array->scan_key == ikey);
			}
		}
		else
		{
			/*
			 * Are any inequalities required in the opposite direction only
			 * present here?
			 */
			if (((ScanDirectionIsForward(dir) &&
				  (cur->sk_flags & (SK_BT_REQBKWD))) ||
				 (ScanDirectionIsBackward(dir) &&
				  (cur->sk_flags & (SK_BT_REQFWD)))))
				has_required_opposite_direction_only =
					required_opposite_direction_only = true;
		}

		/* Optimization: skip over known-satisfied scan keys */
		if (ikey < sktrig)
			continue;

		if (cur->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD))
		{
			required = true;

			if (ikey == sktrig)
				sktrig_required = true;

			if (cur->sk_attno > ntupatts)
			{
				/* Set this just like _bt_tuple_before_array_skeys */
				Assert(sktrig < ikey);
				so->scanBehind = true;
			}
		}

		/*
		 * Handle a required non-array scan key that the initial call to
		 * _bt_check_compare indicated triggered array advancement, if any.
		 *
		 * The non-array scan key's strategy will be <, <=, or = during a
		 * forwards scan (or any one of =, >=, or > during a backwards scan).
		 * It follows that the corresponding tuple attribute's value must now
		 * be either > or >= the scan key value (for backwards scans it must
		 * be either < or <= that value).
		 *
		 * If this is a required equality strategy scan key, this is just an
		 * optimization; _bt_tuple_before_array_skeys already confirmed that
		 * this scan key places us ahead of caller's tuple.  There's no need
		 * to repeat that work now. (We only do comparisons of any required
		 * non-array equality scan keys that come after the triggering key.)
		 *
		 * If this is a required inequality strategy scan key, we _must_ rely
		 * on _bt_check_compare like this; we aren't capable of directly
		 * evaluating required inequality strategy scan keys here, on our own.
		 */
		if (ikey == sktrig && !array)
		{
			Assert(required && all_required_satisfied && !arrays_advanced);

			/* Use "beyond end" advancement.  See below for an explanation. */
			beyond_end_advance = true;
			all_required_satisfied = false;

			/*
			 * Set a flag that remembers that this was an inequality required
			 * in the opposite scan direction only, that nevertheless
			 * triggered the call here.
			 *
			 * This only happens when an inequality operator (which must be
			 * strict) encounters a group of NULLs that indicate the end of
			 * non-NULL values for tuples in the current scan direction.
			 */
			if (unlikely(required_opposite_direction_only))
				oppodir_inequality_sktrig = true;

			continue;
		}

		/*
		 * Nothing more for us to do with an inequality strategy scan key that
		 * wasn't the one that _bt_check_compare stopped on, though.
		 *
		 * Note: if our later call to _bt_check_compare (to recheck caller's
		 * tuple) sets continuescan=false due to finding this same inequality
		 * unsatisfied (possible when it's required in the scan direction), we
		 * deal with it via a recursive call.
		 */
		else if (cur->sk_strategy != BTEqualStrategyNumber)
			continue;

		/*
		 * Nothing for us to do with an equality strategy scan key that isn't
		 * marked required, either.
		 *
		 * Non-required array scan keys are the only exception.  They're a
		 * special case in that _bt_check_compare can set continuescan=false
		 * for them, just as it will given an unsatisfied required scan key.
		 * It's convenient to follow the same convention, since it results in
		 * our getting called to advance non-required arrays in the same way
		 * as required arrays (though we avoid stopping the scan for them).
		 */
		else if (!required && !array)
			continue;

		/*
		 * Here we perform steps for all array scan keys after a required
		 * array scan key whose binary search triggered "beyond end of array
		 * element" array advancement due to encountering a tuple attribute
		 * value > the closest matching array key (or < for backwards scans).
		 */
		if (beyond_end_advance)
		{
			int			final_elem_dir;

			if (ScanDirectionIsBackward(dir) || !array)
				final_elem_dir = 0;
			else
				final_elem_dir = array->num_elems - 1;

			if (array && array->cur_elem != final_elem_dir)
			{
				array->cur_elem = final_elem_dir;
				cur->sk_argument = array->elem_values[final_elem_dir];
				arrays_advanced = true;
			}

			continue;
		}

		/*
		 * Here we perform steps for all array scan keys after a required
		 * array scan key whose tuple attribute was < the closest matching
		 * array key when we dealt with it (or > for backwards scans).
		 *
		 * This earlier required array key already puts us ahead of caller's
		 * tuple in the key space (for the current scan direction).  We must
		 * make sure that subsequent lower-order array keys do not put us too
		 * far ahead (ahead of tuples that have yet to be seen by our caller).
		 * For example, when a tuple "(a, b) = (42, 5)" advances the array
		 * keys on "a" from 40 to 45, we must also set "b" to whatever the
		 * first array element for "b" is.  It would be wrong to allow "b" to
		 * be set based on the tuple value.
		 *
		 * Perform the same steps with truncated high key attributes.  You can
		 * think of this as a "binary search" for the element closest to the
		 * value -inf.  Again, the arrays must never get ahead of the scan.
		 */
		if (!all_required_satisfied || cur->sk_attno > ntupatts)
		{
			int			first_elem_dir;

			if (ScanDirectionIsForward(dir) || !array)
				first_elem_dir = 0;
			else
				first_elem_dir = array->num_elems - 1;

			if (array && array->cur_elem != first_elem_dir)
			{
				array->cur_elem = first_elem_dir;
				cur->sk_argument = array->elem_values[first_elem_dir];
				arrays_advanced = true;
			}

			continue;
		}

		/*
		 * Search in scankey's array for the corresponding tuple attribute
		 * value from caller's tuple
		 */
		orderproc = &so->orderProcs[so->keyDataMap[ikey]];
		tupdatum = index_getattr(tuple, cur->sk_attno, tupdesc, &tupnull);

		if (array)
		{
			bool		ratchets = (required && !arrays_advanced);

			/*
			 * Binary search for closest match that's available from the array
			 */
			set_elem = _bt_binsrch_array_skey(orderproc, ratchets, dir,
											  tupdatum, tupnull,
											  array, cur, &result);

			/*
			 * Required arrays only ever ratchet forwards (backwards).
			 *
			 * This condition makes it safe for binary searches to skip over
			 * array elements that the scan must already be ahead of by now.
			 * That is strictly an optimization.  Our assertion verifies that
			 * the condition holds, which doesn't depend on the optimization.
			 */
			Assert(!ratchets ||
				   ((ScanDirectionIsForward(dir) && set_elem >= array->cur_elem) ||
					(ScanDirectionIsBackward(dir) && set_elem <= array->cur_elem)));
			Assert(set_elem >= 0 && set_elem < array->num_elems);
		}
		else
		{
			Assert(required);

			/*
			 * This is a required non-array equality strategy scan key, which
			 * we'll treat as a degenerate single value array.
			 *
			 * This scan key's imaginary "array" can't really advance, but it
			 * can still roll over like any other array.  (Actually, this is
			 * no different to real single value arrays, which never advance
			 * without rolling over -- they can never truly advance, either.)
			 */
			result = _bt_compare_array_skey(orderproc, tupdatum, tupnull,
											cur->sk_argument, cur);
		}

		/*
		 * Consider "beyond end of array element" array advancement.
		 *
		 * When the tuple attribute value is > the closest matching array key
		 * (or < in the backwards scan case), we need to ratchet this array
		 * forward (backward) by one increment, so that caller's tuple ends up
		 * being < final array value instead (or > final array value instead).
		 * This process has to work for all of the arrays, not just this one:
		 * it must "carry" to higher-order arrays when the set_elem that we
		 * just found happens to be the final one for the scan's direction.
		 * Incrementing (decrementing) set_elem itself isn't good enough.
		 *
		 * Our approach is to provisionally use set_elem as if it was an exact
		 * match now, then set each later/less significant array to whatever
		 * its final element is.  Once outside the loop we'll then "increment
		 * this array's set_elem" by calling _bt_advance_array_keys_increment.
		 * That way the process rolls over to higher order arrays as needed.
		 *
		 * Under this scheme any required arrays only ever ratchet forwards
		 * (or backwards), and always do so to the maximum possible extent
		 * that we can know will be safe without seeing the scan's next tuple.
		 * We don't need any special handling for required scan keys that lack
		 * a real array to advance, nor for redundant scan keys that couldn't
		 * be eliminated by _bt_preprocess_keys.  It won't matter if some of
		 * our "true" array scan keys (or even all of them) are non-required.
		 */
		if (required &&
			((ScanDirectionIsForward(dir) && result > 0) ||
			 (ScanDirectionIsBackward(dir) && result < 0)))
			beyond_end_advance = true;

		if (result != 0)
		{
			/*
			 * Track whether caller's tuple satisfies our new post-advancement
			 * qual, though only in respect of its required scan keys.
			 *
			 * When it's a non-required array that doesn't match, we can give
			 * up early, without advancing the array (nor any later
			 * non-required arrays).  This often saves us an unnecessary
			 * recheck call to _bt_check_compare.
			 */
			Assert(all_required_satisfied);
			if (required)
				all_required_satisfied = false;
			else
				break;
		}

		/* Advance array keys, even when set_elem isn't an exact match */
		if (array && array->cur_elem != set_elem)
		{
			array->cur_elem = set_elem;
			cur->sk_argument = array->elem_values[set_elem];
			arrays_advanced = true;
		}
	}

	/*
	 * Consider if we need to advance the array keys incrementally to finish
	 * off "beyond end of array element" array advancement.  This is the only
	 * way that the array keys can be exhausted, which is how top-level index
	 * scans usually determine that they've run out of tuples to return.
	 */
	arrays_exhausted = false;
	if (beyond_end_advance)
	{
		Assert(!all_required_satisfied && sktrig_required);

		if (_bt_advance_array_keys_increment(scan, dir))
			arrays_advanced = true;
		else
			arrays_exhausted = true;
	}

	if (arrays_advanced)
	{
		if (sktrig_required)
		{
			/*
			 * One or more required array keys advanced, so invalidate state
			 * that tracks whether required-in-opposite-direction-only scan
			 * keys are already known to be satisfied
			 */
			pstate->firstmatch = false;

			/* Shouldn't have to invalidate 'prechecked', though */
			Assert(!pstate->prechecked);
		}
	}
	else
		Assert(arrays_exhausted || !sktrig_required);

	Assert(_bt_verify_keys_with_arraykeys(scan));
	if (arrays_exhausted)
	{
		Assert(sktrig_required);
		Assert(!all_required_satisfied);

		/*
		 * The top-level index scan ran out of tuples to return
		 */
		goto end_toplevel_scan;
	}

	/*
	 * Does caller's tuple now match the new qual?  Call _bt_check_compare a
	 * second time to find out (unless it's already clear that it can't).
	 */
	if (all_required_satisfied && arrays_advanced)
	{
		int			nsktrig = sktrig + 1;

		if (_bt_check_compare(dir, so, tuple, ntupatts, tupdesc,
							  false, false, false,
							  &pstate->continuescan, &nsktrig) &&
			!so->scanBehind)
		{
			/* This tuple satisfies the new qual */
			return true;
		}

		/*
		 * Consider "second pass" handling of required inequalities.
		 *
		 * It's possible that our _bt_check_compare call indicated that the
		 * scan should end due to some unsatisfied inequality that wasn't
		 * initially recognized as such by us.  Handle this by calling
		 * ourselves recursively, this time indicating that the trigger is the
		 * inequality that we missed first time around (and using a set of
		 * required array/equality keys that are now exact matches for tuple).
		 *
		 * We make a strong, general guarantee that every _bt_checkkeys call
		 * here will advance the array keys to the maximum possible extent
		 * that we can know to be safe based on caller's tuple alone.  If we
		 * didn't perform this step, then that guarantee wouldn't quite hold.
		 */
		if (unlikely(!pstate->continuescan))
		{
			bool		satisfied PG_USED_FOR_ASSERTS_ONLY;

			Assert(so->keyData[nsktrig].sk_strategy != BTEqualStrategyNumber);

			/*
			 * The tuple must use "beyond end" advancement during the
			 * recursive call, so we cannot possibly end up back here when
			 * recursing.  We'll consume a small, fixed amount of stack space.
			 */
			Assert(!beyond_end_advance);

			/* Advance the array keys a second time for same tuple */
			satisfied = _bt_advance_array_keys(scan, pstate, tuple, nsktrig);

			/* This tuple doesn't satisfy the inequality */
			Assert(!satisfied);
			return false;
		}

		/*
		 * Some non-required scan key (from new qual) still not satisfied.
		 *
		 * All scan keys required in the current scan direction must still be
		 * satisfied, though, so we can trust all_required_satisfied below.
		 *
		 * Note: it's still too early to tell if the current primitive index
		 * scan can continue (has_required_opposite_direction_only steps might
		 * still start a new primitive index scan instead).
		 */
	}

	/*
	 * Postcondition array state assertions (for still-unsatisfied tuples).
	 *
	 * Caller's tuple is now < the newly advanced array keys (or > when this
	 * is a backwards scan) when not all required scan keys from the new qual
	 * (including any required inequality keys) were found to be satisfied.
	 */
	Assert(_bt_tuple_before_array_skeys(scan, dir, tuple, false, 0, NULL) ==
		   !all_required_satisfied);

	/*
	 * When we were called just to deal with "advancing" non-required arrays,
	 * there's no way that we can need to start a new primitive index scan
	 * (and it would be wrong to allow it).  Continue ongoing primitive scan.
	 */
	if (!sktrig_required)
		goto continue_prim_scan;

	/*
	 * By here we have established that the scan's required arrays were
	 * advanced, and that they haven't become exhausted.
	 */
	Assert(arrays_advanced || !arrays_exhausted);

	/*
	 * We generally permit primitive index scans to continue onto the next
	 * sibling page when the page's finaltup satisfies all required scan keys
	 * at the point where we're between pages.
	 *
	 * If caller's tuple is also the page's finaltup, and we see that required
	 * scan keys still aren't satisfied, start a new primitive index scan.
	 */
	if (!all_required_satisfied && pstate->finaltup == tuple)
		goto new_prim_scan;

	/*
	 * Proactively check finaltup (don't wait until finaltup is reached by the
	 * scan) when it might well turn out to not be satisfied later on.
	 *
	 * This isn't quite equivalent to looking ahead to check if finaltup will
	 * also be satisfied by all required scan keys, since there isn't any real
	 * handling of inequalities in _bt_tuple_before_array_skeys.  It wouldn't
	 * make sense for us to evaluate inequalities when "looking ahead to
	 * finaltup", though.  Inequalities that are required in the current scan
	 * direction cannot affect how _bt_first repositions the top-level scan
	 * (unless the scan direction happens to change).
	 *
	 * Note: if so->scanBehind hasn't already been set for finaltup by us,
	 * it'll be set during this call to _bt_tuple_before_array_skeys.  Either
	 * way it'll be set correctly after this point.
	 */
	if (!all_required_satisfied && pstate->finaltup &&
		_bt_tuple_before_array_skeys(scan, dir, pstate->finaltup, false, 0,
									 &so->scanBehind))
		goto new_prim_scan;

	/*
	 * When we encounter a truncated finaltup high key attribute, we're
	 * optimistic about the chances of its corresponding required scan key
	 * being satisfied when we go on to check it against tuples from this
	 * page's right sibling leaf page.  We consider truncated attributes to be
	 * satisfied by required scan keys, which allows the primitive index scan
	 * to continue to the next leaf page.  We must set so->scanBehind to true
	 * to remember that the last page's finaltup had "satisfied" required scan
	 * keys for one or more truncated attribute values (scan keys required in
	 * _either_ scan direction).
	 *
	 * There is a chance that _bt_checkkeys (which checks so->scanBehind) will
	 * find that even the sibling leaf page's finaltup is < the new array
	 * keys.  When that happens, our optimistic policy will have incurred a
	 * single extra leaf page access that could have been avoided.
	 *
	 * A pessimistic policy would give backward scans a gratuitous advantage
	 * over forward scans.  We'd punish forward scans for applying more
	 * accurate information from the high key, rather than just using the
	 * final non-pivot tuple as finaltup, in the style of backward scans.
	 * Being pessimistic would also give some scans with non-required arrays a
	 * perverse advantage over similar scans that use required arrays instead.
	 *
	 * You can think of this as a speculative bet on what the scan is likely
	 * to find on the next page.  It's not much of a gamble, though, since the
	 * untruncated prefix of attributes must strictly satisfy the new qual
	 * (though it's okay if any non-required scan keys fail to be satisfied).
	 */
	if (so->scanBehind && has_required_opposite_direction_only)
	{
		/*
		 * However, we avoid this behavior whenever the scan involves a scan
		 * key required in the opposite direction to the scan only, along with
		 * a finaltup with at least one truncated attribute that's associated
		 * with a scan key marked required (required in either direction).
		 *
		 * _bt_check_compare simply won't stop the scan for a scan key that's
		 * marked required in the opposite scan direction only.  That leaves
		 * us without any reliable way of reconsidering any opposite-direction
		 * inequalities if it turns out that starting a new primitive index
		 * scan will allow _bt_first to skip ahead by a great many leaf pages
		 * (see next section for details of how that works).
		 */
		goto new_prim_scan;
	}

	/*
	 * Handle inequalities marked required in the opposite scan direction.
	 * They can also signal that we should start a new primitive index scan.
	 *
	 * It's possible that the scan is now positioned where "matching" tuples
	 * begin, and that caller's tuple satisfies all scan keys required in the
	 * current scan direction.  But if caller's tuple still doesn't satisfy
	 * other scan keys that are required in the opposite scan direction only
	 * (e.g., a required >= strategy scan key when scan direction is forward),
	 * it's still possible that there are many leaf pages before the page that
	 * _bt_first could skip straight to.  Groveling through all those pages
	 * will always give correct answers, but it can be very inefficient.  We
	 * must avoid needlessly scanning extra pages.
	 *
	 * Separately, it's possible that _bt_check_compare set continuescan=false
	 * for a scan key that's required in the opposite direction only.  This is
	 * a special case, that happens only when _bt_check_compare sees that the
	 * inequality encountered a NULL value.  This signals the end of non-NULL
	 * values in the current scan direction, which is reason enough to end the
	 * (primitive) scan.  If this happens at the start of a large group of
	 * NULL values, then we shouldn't expect to be called again until after
	 * the scan has already read indefinitely-many leaf pages full of tuples
	 * with NULL suffix values.  We need a separate test for this case so that
	 * we don't miss our only opportunity to skip over such a group of pages.
	 *
	 * Apply a test against finaltup to detect and recover from the problem:
	 * if even finaltup doesn't satisfy such an inequality, we just skip by
	 * starting a new primitive index scan.  When we skip, we know for sure
	 * that all of the tuples on the current page following caller's tuple are
	 * also before the _bt_first-wise start of tuples for our new qual.  That
	 * at least suggests many more skippable pages beyond the current page.
	 */
	if (has_required_opposite_direction_only && pstate->finaltup &&
		(all_required_satisfied || oppodir_inequality_sktrig))
	{
		int			nfinaltupatts = BTreeTupleGetNAtts(pstate->finaltup, rel);
		ScanDirection flipped;
		bool		continuescanflip;
		int			opsktrig;

		/*
		 * We're checking finaltup (which is usually not caller's tuple), so
		 * cannot reuse work from caller's earlier _bt_check_compare call.
		 *
		 * Flip the scan direction when calling _bt_check_compare this time,
		 * so that it will set continuescanflip=false when it encounters an
		 * inequality required in the opposite scan direction.
		 */
		Assert(!so->scanBehind);
		opsktrig = 0;
		flipped = -dir;
		_bt_check_compare(flipped, so, pstate->finaltup, nfinaltupatts,
						  tupdesc, false, false, false,
						  &continuescanflip, &opsktrig);

		/*
		 * If we ended up here due to the all_required_satisfied criteria,
		 * test opsktrig in a way that ensures that finaltup contains the same
		 * prefix of key columns as caller's tuple (a prefix that satisfies
		 * earlier required-in-current-direction scan keys).
		 *
		 * If we ended up here due to the oppodir_inequality_sktrig criteria,
		 * test opsktrig in a way that ensures that the same scan key that our
		 * caller found to be unsatisfied (by the scan's tuple) was also the
		 * one unsatisfied just now (by finaltup).  That way we'll only start
		 * a new primitive scan when we're sure that both tuples _don't_ share
		 * the same prefix of satisfied equality-constrained attribute values,
		 * and that finaltup has a non-NULL attribute value indicated by the
		 * unsatisfied scan key at offset opsktrig/sktrig.  (This depends on
		 * _bt_check_compare not caring about the direction that inequalities
		 * are required in whenever NULL attribute values are unsatisfied.  It
		 * only cares about the scan direction, and its relationship to
		 * whether NULLs are stored first or last relative to non-NULLs.)
		 */
		Assert(all_required_satisfied != oppodir_inequality_sktrig);
		if (unlikely(!continuescanflip &&
					 ((all_required_satisfied && opsktrig > sktrig) ||
					  (oppodir_inequality_sktrig && opsktrig == sktrig))))
		{
			Assert(so->keyData[opsktrig].sk_strategy != BTEqualStrategyNumber);

			/*
			 * Make sure that any non-required arrays are set to the first
			 * array element for the current scan direction
			 */
			_bt_rewind_nonrequired_arrays(scan, dir);

			goto new_prim_scan;
		}
	}

continue_prim_scan:

	/*
	 * Stick with the ongoing primitive index scan for now.
	 *
	 * It's possible that later tuples will also turn out to have values that
	 * are still < the now-current array keys (or > the current array keys).
	 * Our caller will handle this by performing what amounts to a linear
	 * search of the page, implemented by calling _bt_check_compare and then
	 * _bt_tuple_before_array_skeys for each tuple.
	 *
	 * This approach has various advantages over a binary search of the page.
	 * We expect that our caller will quickly discover the next tuple covered
	 * by the current array keys.  Repeated binary searches of the page (one
	 * binary search per array advancement) is unlikely to outperform one
	 * continuous linear search of the whole page.
	 */
	pstate->continuescan = true;	/* Override _bt_check_compare */
	so->needPrimScan = false;	/* _bt_readpage has more tuples to check */

	/* Caller's tuple doesn't match the new qual */
	return false;

new_prim_scan:

	/*
	 * End this primitive index scan, but scheduled another
	 */
	pstate->continuescan = false;	/* Tell _bt_readpage we're done... */
	so->needPrimScan = true;	/* ...but call _bt_first again */

	/* Caller's tuple doesn't match the new qual */
	return false;

end_toplevel_scan:

	/*
	 * End the current primitive index scan, but don't schedule another.
	 *
	 * This ends the entire top-level scan.
	 */
	pstate->continuescan = false;	/* Tell _bt_readpage we're done... */
	so->needPrimScan = false;	/* ...don't call _bt_first again, though */

	/* Caller's tuple doesn't match any qual */
	return false;
}

/*
 *	_bt_preprocess_keys() -- Preprocess scan keys
 *
 * The given search-type keys (taken from scan->keyData[])
 * are copied to so->keyData[] with possible transformation.
 * scan->numberOfKeys is the number of input keys, so->numberOfKeys gets
 * the number of output keys (possibly less, never greater).
 *
 * The output keys are marked with additional sk_flags bits beyond the
 * system-standard bits supplied by the caller.  The DESC and NULLS_FIRST
 * indoption bits for the relevant index attribute are copied into the flags.
 * Also, for a DESC column, we commute (flip) all the sk_strategy numbers
 * so that the index sorts in the desired direction.
 *
 * One key purpose of this routine is to discover which scan keys must be
 * satisfied to continue the scan.  It also attempts to eliminate redundant
 * keys and detect contradictory keys.  (If the index opfamily provides
 * incomplete sets of cross-type operators, we may fail to detect redundant
 * or contradictory keys, but we can survive that.)
 *
 * The output keys must be sorted by index attribute.  Presently we expect
 * (but verify) that the input keys are already so sorted --- this is done
 * by match_clauses_to_index() in indxpath.c.  Some reordering of the keys
 * within each attribute may be done as a byproduct of the processing here,
 * but no other code depends on that.  Note that index scans with array scan
 * keys depend on state (maintained here by us) that maps each of our input
 * scan keys to its corresponding output scan key.  This indirection allows
 * index scans to use an ikey offset-to-output-scankey to look up the cached
 * ORDER proc for the scankey.
 *
 * The output keys are marked with flags SK_BT_REQFWD and/or SK_BT_REQBKWD
 * if they must be satisfied in order to continue the scan forward or backward
 * respectively.  _bt_checkkeys uses these flags.  For example, if the quals
 * are "x = 1 AND y < 4 AND z < 5", then _bt_checkkeys will reject a tuple
 * (1,2,7), but we must continue the scan in case there are tuples (1,3,z).
 * But once we reach tuples like (1,4,z) we can stop scanning because no
 * later tuples could match.  This is reflected by marking the x and y keys,
 * but not the z key, with SK_BT_REQFWD.  In general, the keys for leading
 * attributes with "=" keys are marked both SK_BT_REQFWD and SK_BT_REQBKWD.
 * For the first attribute without an "=" key, any "<" and "<=" keys are
 * marked SK_BT_REQFWD while any ">" and ">=" keys are marked SK_BT_REQBKWD.
 * This can be seen to be correct by considering the above example.  Note
 * in particular that if there are no keys for a given attribute, the keys for
 * subsequent attributes can never be required; for instance "WHERE y = 4"
 * requires a full-index scan.
 *
 * If possible, redundant keys are eliminated: we keep only the tightest
 * >/>= bound and the tightest </<= bound, and if there's an = key then
 * that's the only one returned.  (So, we return either a single = key,
 * or one or two boundary-condition keys for each attr.)  However, if we
 * cannot compare two keys for lack of a suitable cross-type operator,
 * we cannot eliminate either.  If there are two such keys of the same
 * operator strategy, the second one is just pushed into the output array
 * without further processing here.  We may also emit both >/>= or both
 * </<= keys if we can't compare them.  The logic about required keys still
 * works if we don't eliminate redundant keys.
 *
 * Note that one reason we need direction-sensitive required-key flags is
 * precisely that we may not be able to eliminate redundant keys.  Suppose
 * we have "x > 4::int AND x > 10::bigint", and we are unable to determine
 * which key is more restrictive for lack of a suitable cross-type operator.
 * _bt_first will arbitrarily pick one of the keys to do the initial
 * positioning with.  If it picks x > 4, then the x > 10 condition will fail
 * until we reach index entries > 10; but we can't stop the scan just because
 * x > 10 is failing.  On the other hand, if we are scanning backwards, then
 * failure of either key is indeed enough to stop the scan.  (In general, when
 * inequality keys are present, the initial-positioning code only promises to
 * position before the first possible match, not exactly at the first match,
 * for a forward scan; or after the last match for a backward scan.)
 *
 * As a byproduct of this work, we can detect contradictory quals such
 * as "x = 1 AND x > 2".  If we see that, we return so->qual_ok = false,
 * indicating the scan need not be run at all since no tuples can match.
 * (In this case we do not bother completing the output key array!)
 * Again, missing cross-type operators might cause us to fail to prove the
 * quals contradictory when they really are, but the scan will work correctly.
 *
 * _bt_checkkeys needs to be able to perform in-place updates of the scan keys
 * output here by us.  This is the final step it performs in order to advance
 * the scan's array keys.  The rules for redundancy/contradictoriness work a
 * little differently when array-type scan keys are involved.  We need to
 * consider every possible set of array keys.  During scans with array keys,
 * only the first call here (per btrescan) will actually do any real work.
 * Later calls just assert that _bt_checkkeys set things up correctly.
 *
 * Row comparison keys are currently also treated without any smarts:
 * we just transfer them into the preprocessed array without any
 * editorialization.  We can treat them the same as an ordinary inequality
 * comparison on the row's first index column, for the purposes of the logic
 * about required keys.
 *
 * Note: the reason we have to copy the preprocessed scan keys into private
 * storage is that we are modifying the array based on comparisons of the key
 * argument values, which could change on a rescan.  Therefore we can't
 * overwrite the source data.
 */
void
_bt_preprocess_keys(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	int			numberOfKeys = scan->numberOfKeys;
	int16	   *indoption = scan->indexRelation->rd_indoption;
	int			new_numberOfKeys;
	int			numberOfEqualCols;
	ScanKey		inkeys;
	ScanKey		outkeys;
	int		   *keyDataMap = NULL;
	ScanKey		cur;
	ScanKeyAttr xform[BTMaxStrategyNumber];
	bool		test_result;
	int			i,
				j;
	AttrNumber	attno;
	ScanKey		arrayKeyData;	/* modified copy of scan->keyData */

	Assert(!so->needPrimScan);

	/*
	 * We're called at the start of each primitive index scan during top-level
	 * scans that use equality array keys.  We can reuse the scan keys that
	 * were output at the start of the scan's first primitive index scan.
	 * There is no need to perform exactly the same work more than once.
	 */
	if (so->numberOfKeys > 0)
	{
		/*
		 * An earlier call to _bt_advance_array_keys already set everything up
		 * for us.  Just assert that the scan's existing output scan keys are
		 * consistent with its current array elements.
		 */
		Assert(so->numArrayKeys && !ScanDirectionIsNoMovement(so->advanceDir));
		Assert(_bt_verify_keys_with_arraykeys(scan));
		return;
	}

	Assert(ScanDirectionIsNoMovement(so->advanceDir));

	/* initialize result variables */
	so->qual_ok = true;
	so->numberOfKeys = 0;

	if (numberOfKeys < 1)
		return;					/* done if qual-less scan */

	/* If any keys are SK_SEARCHARRAY type, set up array-key info */
	arrayKeyData = _bt_preprocess_array_keys(scan);
	if (!so->qual_ok)
	{
		/* unmatchable array, so give up */
		so->qual_ok = false;
		return;
	}

	/*
	 * Treat arrayKeyData as our input if _bt_preprocess_array_keys just
	 * allocated it, else just use scan->keyData.
	 */
	if (arrayKeyData != NULL)
	{
		/*
		 * Maintain a mapping from input scan keys to our final output scan
		 * keys.  This gives _bt_advance_array_keys a convenient way to look
		 * up each equality scan key's ORDER proc (including but not limited
		 * to scan keys used for arrays).  The ORDER proc array stores entries
		 * in the same order as corresponding scan keys appear in inkeys.
		 */
		inkeys = arrayKeyData;
		keyDataMap = so->keyDataMap;
	}
	else
		inkeys = scan->keyData;

	outkeys = so->keyData;
	cur = &inkeys[0];
	/* we check that input keys are correctly ordered */
	if (cur->sk_attno < 1)
		elog(ERROR, "btree index keys must be ordered by attribute");

	/* We can short-circuit most of the work if there's just one key */
	if (numberOfKeys == 1)
	{
		/* Apply indoption to scankey (might change sk_strategy!) */
		if (!_bt_fix_scankey_strategy(cur, indoption))
			so->qual_ok = false;
		memcpy(outkeys, cur, sizeof(ScanKeyData));
		so->numberOfKeys = 1;
		/* We can mark the qual as required if it's for first index col */
		if (cur->sk_attno == 1)
			_bt_mark_scankey_required(outkeys);
		if (keyDataMap)
			keyDataMap[0] = 0;

		return;
	}

	/*
	 * Otherwise, do the full set of pushups.
	 */
	new_numberOfKeys = 0;
	numberOfEqualCols = 0;

	/*
	 * Initialize for processing of keys for attr 1.
	 *
	 * xform[i] points to the currently best scan key of strategy type i+1; it
	 * is NULL if we haven't yet found such a key for this attr.
	 */
	attno = 1;
	memset(xform, 0, sizeof(xform));

	/*
	 * Loop iterates from 0 to numberOfKeys inclusive; we use the last pass to
	 * handle after-last-key processing.  Actual exit from the loop is at the
	 * "break" statement below.
	 */
	for (i = 0;; cur++, i++)
	{
		if (i < numberOfKeys)
		{
			/* Apply indoption to scankey (might change sk_strategy!) */
			if (!_bt_fix_scankey_strategy(cur, indoption))
			{
				/* NULL can't be matched, so give up */
				so->qual_ok = false;
				return;
			}
		}

		/*
		 * If we are at the end of the keys for a particular attr, finish up
		 * processing and emit the cleaned-up keys.
		 */
		if (i == numberOfKeys || cur->sk_attno != attno)
		{
			int			priorNumberOfEqualCols = numberOfEqualCols;

			/* check input keys are correctly ordered */
			if (i < numberOfKeys && cur->sk_attno < attno)
				elog(ERROR, "btree index keys must be ordered by attribute");

			/*
			 * If = has been specified, all other keys can be eliminated as
			 * redundant.  If we have a case like key = 1 AND key > 2, we can
			 * set qual_ok to false and abandon further processing.
			 *
			 * We also have to deal with the case of "key IS NULL", which is
			 * unsatisfiable in combination with any other index condition. By
			 * the time we get here, that's been classified as an equality
			 * check, and we've rejected any combination of it with a regular
			 * equality condition (including those used with array keys); but
			 * not with other types of conditions.
			 */
			if (xform[BTEqualStrategyNumber - 1].skey)
			{
				ScanKey		eq = xform[BTEqualStrategyNumber - 1].skey;

				for (j = BTMaxStrategyNumber; --j >= 0;)
				{
					ScanKey		chk = xform[j].skey;

					if (!chk || j == (BTEqualStrategyNumber - 1))
						continue;

					if (eq->sk_flags & SK_SEARCHNULL)
					{
						/* IS NULL is contradictory to anything else */
						so->qual_ok = false;
						return;
					}

					if (eq->sk_flags & SK_SEARCHARRAY)
					{
						/*
						 * Don't try to prove redundancy in the event of an
						 * inequality strategy scan key that looks like it
						 * might contradict a subset of the array elements
						 * from some equality scan key's array.  Just keep
						 * both keys.
						 *
						 * Ideally, we'd handle this by adding a preprocessing
						 * step that eliminates the subset of array elements
						 * that the inequality ipso facto rules out (and
						 * eliminates the inequality itself, too).  But that
						 * seems like a lot of code for such a small benefit
						 * (_bt_checkkeys is already capable of advancing the
						 * array keys by a great many elements in one step,
						 * without requiring too many cycles compared to
						 * sophisticated preprocessing).
						 */
					}
					else if (_bt_compare_scankey_args(scan, chk, eq, chk,
													  &test_result))
					{
						if (!test_result)
						{
							/* keys proven mutually contradictory */
							so->qual_ok = false;
							return;
						}
						/* else discard the redundant non-equality key */
						xform[j].skey = NULL;
						xform[j].ikey = -1;
					}
					/* else, cannot determine redundancy, keep both keys */
				}
				/* track number of attrs for which we have "=" keys */
				numberOfEqualCols++;
			}

			/* try to keep only one of <, <= */
			if (xform[BTLessStrategyNumber - 1].skey
				&& xform[BTLessEqualStrategyNumber - 1].skey)
			{
				ScanKey		lt = xform[BTLessStrategyNumber - 1].skey;
				ScanKey		le = xform[BTLessEqualStrategyNumber - 1].skey;

				if (_bt_compare_scankey_args(scan, le, lt, le,
											 &test_result))
				{
					if (test_result)
						xform[BTLessEqualStrategyNumber - 1].skey = NULL;
					else
						xform[BTLessStrategyNumber - 1].skey = NULL;
				}
			}

			/* try to keep only one of >, >= */
			if (xform[BTGreaterStrategyNumber - 1].skey
				&& xform[BTGreaterEqualStrategyNumber - 1].skey)
			{
				ScanKey		gt = xform[BTGreaterStrategyNumber - 1].skey;
				ScanKey		ge = xform[BTGreaterEqualStrategyNumber - 1].skey;

				if (_bt_compare_scankey_args(scan, ge, gt, ge,
											 &test_result))
				{
					if (test_result)
						xform[BTGreaterEqualStrategyNumber - 1].skey = NULL;
					else
						xform[BTGreaterStrategyNumber - 1].skey = NULL;
				}
			}

			/*
			 * Emit the cleaned-up keys into the outkeys[] array, and then
			 * mark them if they are required.  They are required (possibly
			 * only in one direction) if all attrs before this one had "=".
			 */
			for (j = BTMaxStrategyNumber; --j >= 0;)
			{
				if (xform[j].skey)
				{
					ScanKey		outkey = &outkeys[new_numberOfKeys++];

					memcpy(outkey, xform[j].skey, sizeof(ScanKeyData));
					if (keyDataMap)
						keyDataMap[new_numberOfKeys - 1] = xform[j].ikey;
					if (priorNumberOfEqualCols == attno - 1)
						_bt_mark_scankey_required(outkey);
				}
			}

			/*
			 * Exit loop here if done.
			 */
			if (i == numberOfKeys)
				break;

			/* Re-initialize for new attno */
			attno = cur->sk_attno;
			memset(xform, 0, sizeof(xform));
		}

		/* check strategy this key's operator corresponds to */
		j = cur->sk_strategy - 1;

		/*
		 * Is this an array scan key that _bt_preprocess_array_keys merged
		 * into an earlier array key against the same attribute?
		 */
		if (cur->sk_strategy == InvalidStrategy)
		{
			/*
			 * key is redundant for this primitive index scan (and will be
			 * redundant during all subsequent primitive index scans)
			 */
			Assert(cur->sk_flags & SK_SEARCHARRAY);

			continue;
		}

		/* if row comparison, push it directly to the output array */
		if (cur->sk_flags & SK_ROW_HEADER)
		{
			ScanKey		outkey = &outkeys[new_numberOfKeys++];

			memcpy(outkey, cur, sizeof(ScanKeyData));
			if (keyDataMap)
				keyDataMap[new_numberOfKeys - 1] = i;
			if (numberOfEqualCols == attno - 1)
				_bt_mark_scankey_required(outkey);

			/*
			 * We don't support RowCompare using equality; such a qual would
			 * mess up the numberOfEqualCols tracking.
			 */
			Assert(j != (BTEqualStrategyNumber - 1));
			continue;
		}

		/*
		 * have we seen a scan key for this same attribute and using this same
		 * operator strategy before now?
		 */
		if (xform[j].skey == NULL)
		{
			/* nope, so this scan key wins by default (at least for now) */
			xform[j].skey = cur;
			xform[j].ikey = i;
		}
		else
		{
			ScanKey		outkey;

			/*
			 * Seen one of these before, so keep only the more restrictive key
			 * if possible
			 */
			if (j == (BTEqualStrategyNumber - 1) &&
				((xform[j].skey->sk_flags & SK_SEARCHARRAY) ||
				 (cur->sk_flags & SK_SEARCHARRAY)) &&
				!(cur->sk_flags & SK_SEARCHNULL))
			{
				/*
				 * But don't discard the existing equality key if it's an
				 * array scan key.  We can't conclude that the key is truly
				 * redundant with an array.  The only exception is "key IS
				 * NULL" keys, which eliminate every possible array element
				 * (and so ipso facto make the whole qual contradictory).
				 *
				 * Note: redundant and contradictory array keys will have
				 * already been dealt with by _bt_merge_arrays in the most
				 * important cases.  Ideally, _bt_merge_arrays would also be
				 * able to handle all equality keys as "degenerate single
				 * value arrays", but for now we're better off leaving it up
				 * to _bt_checkkeys to advance the array keys.
				 *
				 * Note: another possible solution to this problem is to
				 * perform incremental array advancement here instead.  That
				 * doesn't seem particularly appealing, since it won't perform
				 * acceptably during scans that have an extremely large number
				 * of distinct array key combinations (typically due to the
				 * presence of multiple arrays, each containing merely a large
				 * number of distinct elements).
				 *
				 * Likely only redundant for a subset of array elements...
				 */
			}
			else if (!_bt_compare_scankey_args(scan, cur, cur, xform[j].skey,
											   &test_result))
			{
				/*
				 * Cannot determine redundancy because opfamily doesn't supply
				 * a complete set of cross-type operators...
				 */
			}
			else
			{
				/* Have all we need to determine redundancy */
				if (test_result)
				{
					Assert(!(xform[j].skey->sk_flags & SK_SEARCHARRAY) ||
						   xform[j].skey->sk_strategy != BTEqualStrategyNumber);

					/* New key is more restrictive, and so replaces old key */
					xform[j].skey = cur;
					xform[j].ikey = i;
					continue;
				}
				else if (j == (BTEqualStrategyNumber - 1))
				{
					/* key == a && key == b, but a != b */
					so->qual_ok = false;
					return;
				}
				/* else old key is more restrictive, keep it */
				continue;
			}

			/*
			 * ...so keep both keys.
			 *
			 * We can't determine which key is more restrictive (or we can't
			 * eliminate an array scan key).  Replace it in xform[j], and push
			 * the cur one directly to the output array, too.
			 */
			outkey = &outkeys[new_numberOfKeys++];

			memcpy(outkey, xform[j].skey, sizeof(ScanKeyData));
			if (keyDataMap)
				keyDataMap[new_numberOfKeys - 1] = xform[j].ikey;
			if (numberOfEqualCols == attno - 1)
				_bt_mark_scankey_required(outkey);
			xform[j].skey = cur;
			xform[j].ikey = i;
		}
	}

	/*
	 * When _bt_preprocess_array_keys performed array preprocessing, it set
	 * each array's array->scan_key to the array's arrayKeys[] entry offset.
	 *
	 * Now that we've output so->keyData[], and built a mapping from
	 * so->keyData[] (output scan keys) to scan->keyData[] (input scan keys),
	 * fix the array->scan_key references.  (This relies on the assumption
	 * that arrayKeys[] has essentially the same entries as scan->keyData[]).
	 */
	if (arrayKeyData)
	{
		int			arrayidx = 0;

		for (int output_ikey = 0;
			 output_ikey < new_numberOfKeys;
			 output_ikey++)
		{
			ScanKey		outkey = so->keyData + output_ikey;
			int			input_ikey = keyDataMap[output_ikey];

			if (!(outkey->sk_flags & SK_SEARCHARRAY) ||
				outkey->sk_strategy != BTEqualStrategyNumber)
				continue;

			for (; arrayidx < so->numArrayKeys; arrayidx++)
			{
				BTArrayKeyInfo *array = &so->arrayKeys[arrayidx];

				if (array->scan_key == input_ikey)
				{
					array->scan_key = output_ikey;
					break;
				}
			}
		}

		/* We could pfree(arrayKeyData) now, but not worth the cycles */
	}

	so->numberOfKeys = new_numberOfKeys;
}

#ifdef USE_ASSERT_CHECKING
/*
 * Verify that the scan's qual state matches what we expect at the point that
 * _bt_start_prim_scan is about to start a just-scheduled new primitive scan.
 *
 * We enforce a rule against non-required array scan keys: they must start out
 * with whatever element is the first for the scan's current scan direction.
 * See _bt_rewind_nonrequired_arrays comments for an explanation.
 */
static bool
_bt_verify_arrays_bt_first(IndexScanDesc scan, ScanDirection dir)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	int			arrayidx = 0;

	for (int ikey = 0; ikey < so->numberOfKeys; ikey++)
	{
		ScanKey		cur = so->keyData + ikey;
		BTArrayKeyInfo *array = NULL;
		int			first_elem_dir;

		if (!(cur->sk_flags & SK_SEARCHARRAY) ||
			cur->sk_strategy != BTEqualStrategyNumber)
			continue;

		array = &so->arrayKeys[arrayidx++];

		if (((cur->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) ||
			((cur->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)))
			continue;

		if (ScanDirectionIsForward(dir))
			first_elem_dir = 0;
		else
			first_elem_dir = array->num_elems - 1;

		if (array->cur_elem != first_elem_dir)
			return false;
	}

	return _bt_verify_keys_with_arraykeys(scan);
}

/*
 * Verify that the scan's "so->keyData[]" scan keys are in agreement with
 * its array key state
 */
static bool
_bt_verify_keys_with_arraykeys(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	int			last_proc_map = -1,
				last_sk_attno = 0,
				arrayidx = 0;

	if (!so->qual_ok)
		return false;

	for (int ikey = 0; ikey < so->numberOfKeys; ikey++)
	{
		ScanKey		cur = so->keyData + ikey;
		BTArrayKeyInfo *array;

		if (cur->sk_strategy != BTEqualStrategyNumber ||
			!(cur->sk_flags & SK_SEARCHARRAY))
			continue;

		array = &so->arrayKeys[arrayidx++];
		if (array->scan_key != ikey)
			return false;

		/*
		 * Verify that so->keyDataMap[] mappings are in order for
		 * SK_SEARCHARRAY equality strategy scan keys
		 */
		if (last_proc_map >= so->keyDataMap[ikey])
			return false;
		last_proc_map = so->keyDataMap[ikey];

		if (cur->sk_argument != array->elem_values[array->cur_elem])
			return false;
		if (last_sk_attno > cur->sk_attno)
			return false;
		last_sk_attno = cur->sk_attno;
	}

	if (arrayidx != so->numArrayKeys)
		return false;

	return true;
}
#endif

/*
 * Compare two scankey values using a specified operator.
 *
 * The test we want to perform is logically "leftarg op rightarg", where
 * leftarg and rightarg are the sk_argument values in those ScanKeys, and
 * the comparison operator is the one in the op ScanKey.  However, in
 * cross-data-type situations we may need to look up the correct operator in
 * the index's opfamily: it is the one having amopstrategy = op->sk_strategy
 * and amoplefttype/amoprighttype equal to the two argument datatypes.
 *
 * If the opfamily doesn't supply a complete set of cross-type operators we
 * may not be able to make the comparison.  If we can make the comparison
 * we store the operator result in *result and return true.  We return false
 * if the comparison could not be made.
 *
 * Note: op always points at the same ScanKey as either leftarg or rightarg.
 * Since we don't scribble on the scankeys, this aliasing should cause no
 * trouble.
 *
 * Note: this routine needs to be insensitive to any DESC option applied
 * to the index column.  For example, "x < 4" is a tighter constraint than
 * "x < 5" regardless of which way the index is sorted.
 */
static bool
_bt_compare_scankey_args(IndexScanDesc scan, ScanKey op,
						 ScanKey leftarg, ScanKey rightarg,
						 bool *result)
{
	Relation	rel = scan->indexRelation;
	Oid			lefttype,
				righttype,
				optype,
				opcintype,
				cmp_op;
	StrategyNumber strat;

	/*
	 * First, deal with cases where one or both args are NULL.  This should
	 * only happen when the scankeys represent IS NULL/NOT NULL conditions.
	 */
	if ((leftarg->sk_flags | rightarg->sk_flags) & SK_ISNULL)
	{
		bool		leftnull,
					rightnull;

		if (leftarg->sk_flags & SK_ISNULL)
		{
			Assert(leftarg->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL));
			leftnull = true;
		}
		else
			leftnull = false;
		if (rightarg->sk_flags & SK_ISNULL)
		{
			Assert(rightarg->sk_flags & (SK_SEARCHNULL | SK_SEARCHNOTNULL));
			rightnull = true;
		}
		else
			rightnull = false;

		/*
		 * We treat NULL as either greater than or less than all other values.
		 * Since true > false, the tests below work correctly for NULLS LAST
		 * logic.  If the index is NULLS FIRST, we need to flip the strategy.
		 */
		strat = op->sk_strategy;
		if (op->sk_flags & SK_BT_NULLS_FIRST)
			strat = BTCommuteStrategyNumber(strat);

		switch (strat)
		{
			case BTLessStrategyNumber:
				*result = (leftnull < rightnull);
				break;
			case BTLessEqualStrategyNumber:
				*result = (leftnull <= rightnull);
				break;
			case BTEqualStrategyNumber:
				*result = (leftnull == rightnull);
				break;
			case BTGreaterEqualStrategyNumber:
				*result = (leftnull >= rightnull);
				break;
			case BTGreaterStrategyNumber:
				*result = (leftnull > rightnull);
				break;
			default:
				elog(ERROR, "unrecognized StrategyNumber: %d", (int) strat);
				*result = false;	/* keep compiler quiet */
				break;
		}
		return true;
	}

	/*
	 * The opfamily we need to worry about is identified by the index column.
	 */
	Assert(leftarg->sk_attno == rightarg->sk_attno);

	opcintype = rel->rd_opcintype[leftarg->sk_attno - 1];

	/*
	 * Determine the actual datatypes of the ScanKey arguments.  We have to
	 * support the convention that sk_subtype == InvalidOid means the opclass
	 * input type; this is a hack to simplify life for ScanKeyInit().
	 */
	lefttype = leftarg->sk_subtype;
	if (lefttype == InvalidOid)
		lefttype = opcintype;
	righttype = rightarg->sk_subtype;
	if (righttype == InvalidOid)
		righttype = opcintype;
	optype = op->sk_subtype;
	if (optype == InvalidOid)
		optype = opcintype;

	/*
	 * If leftarg and rightarg match the types expected for the "op" scankey,
	 * we can use its already-looked-up comparison function.
	 */
	if (lefttype == opcintype && righttype == optype)
	{
		*result = DatumGetBool(FunctionCall2Coll(&op->sk_func,
												 op->sk_collation,
												 leftarg->sk_argument,
												 rightarg->sk_argument));
		return true;
	}

	/*
	 * Otherwise, we need to go to the syscache to find the appropriate
	 * operator.  (This cannot result in infinite recursion, since no
	 * indexscan initiated by syscache lookup will use cross-data-type
	 * operators.)
	 *
	 * If the sk_strategy was flipped by _bt_fix_scankey_strategy, we have to
	 * un-flip it to get the correct opfamily member.
	 */
	strat = op->sk_strategy;
	if (op->sk_flags & SK_BT_DESC)
		strat = BTCommuteStrategyNumber(strat);

	cmp_op = get_opfamily_member(rel->rd_opfamily[leftarg->sk_attno - 1],
								 lefttype,
								 righttype,
								 strat);
	if (OidIsValid(cmp_op))
	{
		RegProcedure cmp_proc = get_opcode(cmp_op);

		if (RegProcedureIsValid(cmp_proc))
		{
			*result = DatumGetBool(OidFunctionCall2Coll(cmp_proc,
														op->sk_collation,
														leftarg->sk_argument,
														rightarg->sk_argument));
			return true;
		}
	}

	/* Can't make the comparison */
	*result = false;			/* suppress compiler warnings */
	return false;
}

/*
 * Adjust a scankey's strategy and flags setting as needed for indoptions.
 *
 * We copy the appropriate indoption value into the scankey sk_flags
 * (shifting to avoid clobbering system-defined flag bits).  Also, if
 * the DESC option is set, commute (flip) the operator strategy number.
 *
 * A secondary purpose is to check for IS NULL/NOT NULL scankeys and set up
 * the strategy field correctly for them.
 *
 * Lastly, for ordinary scankeys (not IS NULL/NOT NULL), we check for a
 * NULL comparison value.  Since all btree operators are assumed strict,
 * a NULL means that the qual cannot be satisfied.  We return true if the
 * comparison value isn't NULL, or false if the scan should be abandoned.
 *
 * This function is applied to the *input* scankey structure; therefore
 * on a rescan we will be looking at already-processed scankeys.  Hence
 * we have to be careful not to re-commute the strategy if we already did it.
 * It's a bit ugly to modify the caller's copy of the scankey but in practice
 * there shouldn't be any problem, since the index's indoptions are certainly
 * not going to change while the scankey survives.
 */
static bool
_bt_fix_scankey_strategy(ScanKey skey, int16 *indoption)
{
	int			addflags;

	addflags = indoption[skey->sk_attno - 1] << SK_BT_INDOPTION_SHIFT;

	/*
	 * We treat all btree operators as strict (even if they're not so marked
	 * in pg_proc). This means that it is impossible for an operator condition
	 * with a NULL comparison constant to succeed, and we can reject it right
	 * away.
	 *
	 * However, we now also support "x IS NULL" clauses as search conditions,
	 * so in that case keep going. The planner has not filled in any
	 * particular strategy in this case, so set it to BTEqualStrategyNumber
	 * --- we can treat IS NULL as an equality operator for purposes of search
	 * strategy.
	 *
	 * Likewise, "x IS NOT NULL" is supported.  We treat that as either "less
	 * than NULL" in a NULLS LAST index, or "greater than NULL" in a NULLS
	 * FIRST index.
	 *
	 * Note: someday we might have to fill in sk_collation from the index
	 * column's collation.  At the moment this is a non-issue because we'll
	 * never actually call the comparison operator on a NULL.
	 */
	if (skey->sk_flags & SK_ISNULL)
	{
		/* SK_ISNULL shouldn't be set in a row header scankey */
		Assert(!(skey->sk_flags & SK_ROW_HEADER));

		/* Set indoption flags in scankey (might be done already) */
		skey->sk_flags |= addflags;

		/* Set correct strategy for IS NULL or NOT NULL search */
		if (skey->sk_flags & SK_SEARCHNULL)
		{
			skey->sk_strategy = BTEqualStrategyNumber;
			skey->sk_subtype = InvalidOid;
			skey->sk_collation = InvalidOid;
		}
		else if (skey->sk_flags & SK_SEARCHNOTNULL)
		{
			if (skey->sk_flags & SK_BT_NULLS_FIRST)
				skey->sk_strategy = BTGreaterStrategyNumber;
			else
				skey->sk_strategy = BTLessStrategyNumber;
			skey->sk_subtype = InvalidOid;
			skey->sk_collation = InvalidOid;
		}
		else
		{
			/* regular qual, so it cannot be satisfied */
			return false;
		}

		/* Needn't do the rest */
		return true;
	}

	/* Adjust strategy for DESC, if we didn't already */
	if ((addflags & SK_BT_DESC) && !(skey->sk_flags & SK_BT_DESC))
		skey->sk_strategy = BTCommuteStrategyNumber(skey->sk_strategy);
	skey->sk_flags |= addflags;

	/* If it's a row header, fix row member flags and strategies similarly */
	if (skey->sk_flags & SK_ROW_HEADER)
	{
		ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);

		for (;;)
		{
			Assert(subkey->sk_flags & SK_ROW_MEMBER);
			addflags = indoption[subkey->sk_attno - 1] << SK_BT_INDOPTION_SHIFT;
			if ((addflags & SK_BT_DESC) && !(subkey->sk_flags & SK_BT_DESC))
				subkey->sk_strategy = BTCommuteStrategyNumber(subkey->sk_strategy);
			subkey->sk_flags |= addflags;
			if (subkey->sk_flags & SK_ROW_END)
				break;
			subkey++;
		}
	}

	return true;
}

/*
 * Mark a scankey as "required to continue the scan".
 *
 * Depending on the operator type, the key may be required for both scan
 * directions or just one.  Also, if the key is a row comparison header,
 * we have to mark its first subsidiary ScanKey as required.  (Subsequent
 * subsidiary ScanKeys are normally for lower-order columns, and thus
 * cannot be required, since they're after the first non-equality scankey.)
 *
 * Note: when we set required-key flag bits in a subsidiary scankey, we are
 * scribbling on a data structure belonging to the index AM's caller, not on
 * our private copy.  This should be OK because the marking will not change
 * from scan to scan within a query, and so we'd just re-mark the same way
 * anyway on a rescan.  Something to keep an eye on though.
 */
static void
_bt_mark_scankey_required(ScanKey skey)
{
	int			addflags;

	switch (skey->sk_strategy)
	{
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			addflags = SK_BT_REQFWD;
			break;
		case BTEqualStrategyNumber:
			addflags = SK_BT_REQFWD | SK_BT_REQBKWD;
			break;
		case BTGreaterEqualStrategyNumber:
		case BTGreaterStrategyNumber:
			addflags = SK_BT_REQBKWD;
			break;
		default:
			elog(ERROR, "unrecognized StrategyNumber: %d",
				 (int) skey->sk_strategy);
			addflags = 0;		/* keep compiler quiet */
			break;
	}

	skey->sk_flags |= addflags;

	if (skey->sk_flags & SK_ROW_HEADER)
	{
		ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);

		/* First subkey should be same column/operator as the header */
		Assert(subkey->sk_flags & SK_ROW_MEMBER);
		Assert(subkey->sk_attno == skey->sk_attno);
		Assert(subkey->sk_strategy == skey->sk_strategy);
		subkey->sk_flags |= addflags;
	}
}

/*
 * Test whether an indextuple satisfies all the scankey conditions.
 *
 * Return true if so, false if not.  If the tuple fails to pass the qual,
 * we also determine whether there's any need to continue the scan beyond
 * this tuple, and set pstate.continuescan accordingly.  See comments for
 * _bt_preprocess_keys(), above, about how this is done.
 *
 * Forward scan callers can pass a high key tuple in the hopes of having
 * us set *continuescan to false, and avoiding an unnecessary visit to
 * the page to the right.
 *
 * Advances the scan's array keys when necessary for arrayKeys=true callers.
 * Caller can avoid all array related side-effects when calling just to do a
 * page continuescan precheck -- pass arrayKeys=false for that.  Scans without
 * any arrays keys must always pass arrayKeys=false.
 *
 * Also stops and starts primitive index scans for arrayKeys=true callers.
 * Scans with array keys are required to set up page state that helps us with
 * this.  The page's finaltup tuple (the page high key for a forward scan, or
 * the page's first non-pivot tuple for a backward scan) must be set in
 * pstate.finaltup ahead of the first call here for the page (or possibly the
 * first call after an initial continuescan-setting page precheck call).  Set
 * this to NULL for rightmost page (or the leftmost page for backwards scans).
 *
 * scan: index scan descriptor (containing a search-type scankey)
 * pstate: page level input and output parameters
 * arrayKeys: should we advance the scan's array keys if necessary?
 * tuple: index tuple to test
 * tupnatts: number of attributes in tupnatts (high key may be truncated)
 */
bool
_bt_checkkeys(IndexScanDesc scan, BTReadPageState *pstate, bool arrayKeys,
			  IndexTuple tuple, int tupnatts)
{
	TupleDesc	tupdesc = RelationGetDescr(scan->indexRelation);
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	ScanDirection dir = pstate->dir;
	int			ikey = 0;
	bool		res;

	Assert(BTreeTupleGetNAtts(tuple, scan->indexRelation) == tupnatts);
	Assert(!arrayKeys || (so->advanceDir == dir && so->arrayKeys));
	Assert(!so->scanBehind || (arrayKeys && ScanDirectionIsForward(dir)));
	Assert(!so->needPrimScan);

	res = _bt_check_compare(dir, so, tuple, tupnatts, tupdesc,
							arrayKeys, pstate->prechecked, pstate->firstmatch,
							&pstate->continuescan, &ikey);

#ifdef USE_ASSERT_CHECKING
	if (pstate->prechecked || pstate->firstmatch)
	{
		bool		dcontinuescan;
		int			dikey = 0;

		Assert(res == _bt_check_compare(dir, so, tuple, tupnatts, tupdesc,
										arrayKeys, false, false,
										&dcontinuescan, &dikey));
		Assert(dcontinuescan == pstate->continuescan && ikey == dikey);
	}
#endif

	/*
	 * Only one _bt_check_compare call is required in the common case where
	 * there are no equality strategy array scan keys.  Otherwise we can only
	 * accept _bt_check_compare's answer unreservedly when it didn't set
	 * pstate.continuescan=false.
	 */
	if (!arrayKeys || pstate->continuescan)
		return res;

	/*
	 * _bt_check_compare call set continuescan=false in the presence of
	 * equality type array keys.  This could mean that the tuple is just past
	 * the end of matches for the current array keys.
	 *
	 * It's also possible that the scan is still _before_ the _start_ of
	 * tuples matching the current set of array keys.  Check for that first.
	 */
	if (_bt_tuple_before_array_skeys(scan, dir, tuple, true, ikey, NULL))
	{
		/*
		 * Tuple is still before the start of matches according the the scan's
		 * required array keys (according to _all_ of its required equality
		 * strategy keys, actually).
		 *
		 * Note: we will end up here repeatedly given a group of tuples > the
		 * previous array keys and < the now-current keys (though only when
		 * _bt_advance_array_keys determined that key space relevant to the
		 * scan covers some of the page's remaining unscanned tuples).
		 *
		 * _bt_advance_array_keys occasionally sets so->scanBehind to signal
		 * that the scan's current position/tuples might be significantly
		 * behind (multiple pages behind) its current array keys.  When this
		 * happens, we check the page finaltup ourselves.  We'll start a new
		 * primitive index scan on our own if it turns out that the scan isn't
		 * now on a page that has at least some tuples covered by the key
		 * space of the arrays.
		 *
		 * This scheme allows _bt_advance_array_keys to optimistically assume
		 * that the scan will find array key matches for any truncated
		 * finaltup attributes once the scan reaches the right sibling page
		 * (only the untruncated prefix have to match the scan's array keys).
		 */
		Assert(!so->scanBehind ||
			   so->keyData[ikey].sk_strategy == BTEqualStrategyNumber);
		if (unlikely(so->scanBehind) && pstate->finaltup &&
			_bt_tuple_before_array_skeys(scan, dir, pstate->finaltup, false,
										 0, NULL))
		{
			/* Cut our losses -- start a new primitive index scan now */
			pstate->continuescan = false;
			so->needPrimScan = true;
		}
		else
		{
			/* Override _bt_check_compare, continue primitive scan */
			pstate->continuescan = true;
		}

		/* This indextuple doesn't match the current qual, in any case */
		return false;
	}

	/*
	 * Caller's tuple is >= the current set of array keys and other equality
	 * constraint scan keys (or <= if this is a backwards scan).  It's now
	 * clear that we _must_ advance any required array keys in lockstep with
	 * the scan (unless the required array keys become exhausted instead, or
	 * unless the ikey trigger corresponds to a non-required array scan key).
	 *
	 * Note: we might advance the required arrays when all existing keys are
	 * already equal to the values from the tuple at this point.  See comments
	 * above _bt_advance_array_keys about inequality driven array advancement.
	 */
	return _bt_advance_array_keys(scan, pstate, tuple, ikey);
}

/*
 * Test whether an indextuple satisfies current scan condition.
 *
 * Return true if so, false if not.  If not, also sets *continuescan to false
 * when it's also not possible for any later tuples to pass the current qual
 * (with the scan's current set of array keys, in the current scan direction),
 * in addition to setting *ikey to the so->keyData[] subscript/offset for the
 * unsatisfied scan key (needed when caller must consider advancing the scan's
 * array keys).
 *
 * This is a subroutine for _bt_checkkeys.  It is written with the assumption
 * that reaching the end of each distinct set of array keys ends the ongoing
 * primitive index scan.  It is up to our caller to override that initial
 * determination when it makes more sense to advance the array keys and
 * continue with further tuples from the same leaf page.
 *
 * Note: we set *continuescan to false for arrayKeys=true callers in the event
 * of an unsatisfied non-required array equality scan key, despite the fact
 * that it's never safe to end the current primitive index scan when that
 * happens.  Caller will still need to consider "advancing" the array keys
 * (which isn't all that different to what happens to truly required arrays).
 * Caller _must_ unset continuescan once non-required arrays have advanced.
 * Callers that pass arrayKeys=false won't get this behavior, which is useful
 * when the focus is on whether the scan's required scan keys are satisfied.
 */
static bool
_bt_check_compare(ScanDirection dir, BTScanOpaque so,
				  IndexTuple tuple, int tupnatts, TupleDesc tupdesc,
				  bool arrayKeys, bool prechecked, bool firstmatch,
				  bool *continuescan, int *ikey)
{
	*continuescan = true;		/* default assumption */

	for (; *ikey < so->numberOfKeys; (*ikey)++)
	{
		ScanKey		key = so->keyData + *ikey;
		Datum		datum;
		bool		isNull;
		bool		requiredSameDir = false,
					requiredOppositeDirOnly = false;

		/*
		 * Check if the key is required in the current scan direction, in the
		 * opposite scan direction _only_, or in neither direction
		 */
		if (((key->sk_flags & SK_BT_REQFWD) && ScanDirectionIsForward(dir)) ||
			((key->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsBackward(dir)))
			requiredSameDir = true;
		else if (((key->sk_flags & SK_BT_REQFWD) && ScanDirectionIsBackward(dir)) ||
				 ((key->sk_flags & SK_BT_REQBKWD) && ScanDirectionIsForward(dir)))
			requiredOppositeDirOnly = true;

		/*
		 * If the caller told us the *continuescan flag is known to be true
		 * for the last item on the page, then we know the keys required for
		 * the current direction scan should be matched.  Otherwise, the
		 * *continuescan flag would be set for the current item and
		 * subsequently the last item on the page accordingly.
		 *
		 * If the key is required for the opposite direction scan, we can skip
		 * the check if the caller tells us there was already at least one
		 * matching item on the page. Also, we require the *continuescan flag
		 * to be true for the last item on the page to know there are no
		 * NULLs.
		 *
		 * Both cases above work except for the row keys, where NULLs could be
		 * found in the middle of matching values.
		 */
		if (prechecked &&
			(requiredSameDir || (requiredOppositeDirOnly && firstmatch)) &&
			!(key->sk_flags & SK_ROW_HEADER))
			continue;

		if (key->sk_attno > tupnatts)
		{
			/*
			 * This attribute is truncated (must be high key).  The value for
			 * this attribute in the first non-pivot tuple on the page to the
			 * right could be any possible value.  Assume that truncated
			 * attribute passes the qual.
			 */
			Assert(BTreeTupleIsPivot(tuple));
			continue;
		}

		/* row-comparison keys need special processing */
		if (key->sk_flags & SK_ROW_HEADER)
		{
			if (_bt_check_rowcompare(key, tuple, tupnatts, tupdesc, dir,
									 continuescan))
				continue;
			return false;
		}

		datum = index_getattr(tuple,
							  key->sk_attno,
							  tupdesc,
							  &isNull);

		if (key->sk_flags & SK_ISNULL)
		{
			/* Handle IS NULL/NOT NULL tests */
			if (key->sk_flags & SK_SEARCHNULL)
			{
				if (isNull)
					continue;	/* tuple satisfies this qual */
			}
			else
			{
				Assert(key->sk_flags & SK_SEARCHNOTNULL);
				if (!isNull)
					continue;	/* tuple satisfies this qual */
			}

			/*
			 * Tuple fails this qual.  If it's a required qual for the current
			 * scan direction, then we can conclude no further tuples will
			 * pass, either.
			 */
			if (requiredSameDir)
				*continuescan = false;

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		if (isNull)
		{
			if (key->sk_flags & SK_BT_NULLS_FIRST)
			{
				/*
				 * Since NULLs are sorted before non-NULLs, we know we have
				 * reached the lower limit of the range of values for this
				 * index attr.  On a backward scan, we can stop if this qual
				 * is one of the "must match" subset.  We can stop regardless
				 * of whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a forward scan, however, we must keep going, because we may
				 * have initially positioned to the start of the index.
				 * (_bt_advance_array_keys also relies on this behavior during
				 * forward scans.)
				 */
				if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) &&
					ScanDirectionIsBackward(dir))
					*continuescan = false;
			}
			else
			{
				/*
				 * Since NULLs are sorted after non-NULLs, we know we have
				 * reached the upper limit of the range of values for this
				 * index attr.  On a forward scan, we can stop if this qual is
				 * one of the "must match" subset.  We can stop regardless of
				 * whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a backward scan, however, we must keep going, because we
				 * may have initially positioned to the end of the index.
				 * (_bt_advance_array_keys also relies on this behavior during
				 * backward scans.)
				 */
				if ((key->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) &&
					ScanDirectionIsForward(dir))
					*continuescan = false;
			}

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		/*
		 * Apply the key-checking function, though only if we must.
		 *
		 * When a key is required in the opposite-of-scan direction _only_,
		 * then it must already be satisfied if firstmatch=true indicates that
		 * an earlier tuple from this same page satisfied it earlier on.
		 */
		if (!(requiredOppositeDirOnly && firstmatch) &&
			!DatumGetBool(FunctionCall2Coll(&key->sk_func, key->sk_collation,
											datum, key->sk_argument)))
		{
			/*
			 * Tuple fails this qual.  If it's a required qual for the current
			 * scan direction, then we can conclude no further tuples will
			 * pass, either.
			 *
			 * Note: because we stop the scan as soon as any required equality
			 * qual fails, it is critical that equality quals be used for the
			 * initial positioning in _bt_first() when they are available. See
			 * comments in _bt_first().
			 */
			if (requiredSameDir)
				*continuescan = false;

			/*
			 * Also set continuescan=false for non-required equality-type
			 * array keys that don't pass (during arrayKeys=true calls)
			 */
			if (arrayKeys && (key->sk_flags & SK_SEARCHARRAY) &&
				key->sk_strategy == BTEqualStrategyNumber)
				*continuescan = false;

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}
	}

	/* If we get here, the tuple passes all index quals. */
	return true;
}

/*
 * Test whether an indextuple satisfies a row-comparison scan condition.
 *
 * Return true if so, false if not.  If not, also clear *continuescan if
 * it's not possible for any future tuples in the current scan direction
 * to pass the qual.
 *
 * This is a subroutine for _bt_checkkeys/_bt_check_compare.
 */
static bool
_bt_check_rowcompare(ScanKey skey, IndexTuple tuple, int tupnatts,
					 TupleDesc tupdesc, ScanDirection dir, bool *continuescan)
{
	ScanKey		subkey = (ScanKey) DatumGetPointer(skey->sk_argument);
	int32		cmpresult = 0;
	bool		result;

	/* First subkey should be same as the header says */
	Assert(subkey->sk_attno == skey->sk_attno);

	/* Loop over columns of the row condition */
	for (;;)
	{
		Datum		datum;
		bool		isNull;

		Assert(subkey->sk_flags & SK_ROW_MEMBER);

		if (subkey->sk_attno > tupnatts)
		{
			/*
			 * This attribute is truncated (must be high key).  The value for
			 * this attribute in the first non-pivot tuple on the page to the
			 * right could be any possible value.  Assume that truncated
			 * attribute passes the qual.
			 */
			Assert(BTreeTupleIsPivot(tuple));
			cmpresult = 0;
			if (subkey->sk_flags & SK_ROW_END)
				break;
			subkey++;
			continue;
		}

		datum = index_getattr(tuple,
							  subkey->sk_attno,
							  tupdesc,
							  &isNull);

		if (isNull)
		{
			if (subkey->sk_flags & SK_BT_NULLS_FIRST)
			{
				/*
				 * Since NULLs are sorted before non-NULLs, we know we have
				 * reached the lower limit of the range of values for this
				 * index attr.  On a backward scan, we can stop if this qual
				 * is one of the "must match" subset.  We can stop regardless
				 * of whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a forward scan, however, we must keep going, because we may
				 * have initially positioned to the start of the index.
				 * (_bt_advance_array_keys also relies on this behavior during
				 * forward scans.)
				 */
				if ((subkey->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) &&
					ScanDirectionIsBackward(dir))
					*continuescan = false;
			}
			else
			{
				/*
				 * Since NULLs are sorted after non-NULLs, we know we have
				 * reached the upper limit of the range of values for this
				 * index attr.  On a forward scan, we can stop if this qual is
				 * one of the "must match" subset.  We can stop regardless of
				 * whether the qual is > or <, so long as it's required,
				 * because it's not possible for any future tuples to pass. On
				 * a backward scan, however, we must keep going, because we
				 * may have initially positioned to the end of the index.
				 * (_bt_advance_array_keys also relies on this behavior during
				 * backward scans.)
				 */
				if ((subkey->sk_flags & (SK_BT_REQFWD | SK_BT_REQBKWD)) &&
					ScanDirectionIsForward(dir))
					*continuescan = false;
			}

			/*
			 * In any case, this indextuple doesn't match the qual.
			 */
			return false;
		}

		if (subkey->sk_flags & SK_ISNULL)
		{
			/*
			 * Unlike the simple-scankey case, this isn't a disallowed case.
			 * But it can never match.  If all the earlier row comparison
			 * columns are required for the scan direction, we can stop the
			 * scan, because there can't be another tuple that will succeed.
			 */
			if (subkey != (ScanKey) DatumGetPointer(skey->sk_argument))
				subkey--;
			if ((subkey->sk_flags & SK_BT_REQFWD) &&
				ScanDirectionIsForward(dir))
				*continuescan = false;
			else if ((subkey->sk_flags & SK_BT_REQBKWD) &&
					 ScanDirectionIsBackward(dir))
				*continuescan = false;
			return false;
		}

		/* Perform the test --- three-way comparison not bool operator */
		cmpresult = DatumGetInt32(FunctionCall2Coll(&subkey->sk_func,
													subkey->sk_collation,
													datum,
													subkey->sk_argument));

		if (subkey->sk_flags & SK_BT_DESC)
			INVERT_COMPARE_RESULT(cmpresult);

		/* Done comparing if unequal, else advance to next column */
		if (cmpresult != 0)
			break;

		if (subkey->sk_flags & SK_ROW_END)
			break;
		subkey++;
	}

	/*
	 * At this point cmpresult indicates the overall result of the row
	 * comparison, and subkey points to the deciding column (or the last
	 * column if the result is "=").
	 */
	switch (subkey->sk_strategy)
	{
			/* EQ and NE cases aren't allowed here */
		case BTLessStrategyNumber:
			result = (cmpresult < 0);
			break;
		case BTLessEqualStrategyNumber:
			result = (cmpresult <= 0);
			break;
		case BTGreaterEqualStrategyNumber:
			result = (cmpresult >= 0);
			break;
		case BTGreaterStrategyNumber:
			result = (cmpresult > 0);
			break;
		default:
			elog(ERROR, "unrecognized RowCompareType: %d",
				 (int) subkey->sk_strategy);
			result = 0;			/* keep compiler quiet */
			break;
	}

	if (!result)
	{
		/*
		 * Tuple fails this qual.  If it's a required qual for the current
		 * scan direction, then we can conclude no further tuples will pass,
		 * either.  Note we have to look at the deciding column, not
		 * necessarily the first or last column of the row condition.
		 */
		if ((subkey->sk_flags & SK_BT_REQFWD) &&
			ScanDirectionIsForward(dir))
			*continuescan = false;
		else if ((subkey->sk_flags & SK_BT_REQBKWD) &&
				 ScanDirectionIsBackward(dir))
			*continuescan = false;
	}

	return result;
}

/*
 * _bt_killitems - set LP_DEAD state for items an indexscan caller has
 * told us were killed
 *
 * scan->opaque, referenced locally through so, contains information about the
 * current page and killed tuples thereon (generally, this should only be
 * called if so->numKilled > 0).
 *
 * The caller does not have a lock on the page and may or may not have the
 * page pinned in a buffer.  Note that read-lock is sufficient for setting
 * LP_DEAD status (which is only a hint).
 *
 * We match items by heap TID before assuming they are the right ones to
 * delete.  We cope with cases where items have moved right due to insertions.
 * If an item has moved off the current page due to a split, we'll fail to
 * find it and do nothing (this is not an error case --- we assume the item
 * will eventually get marked in a future indexscan).
 *
 * Note that if we hold a pin on the target page continuously from initially
 * reading the items until applying this function, VACUUM cannot have deleted
 * any items from the page, and so there is no need to search left from the
 * recorded offset.  (This observation also guarantees that the item is still
 * the right one to delete, which might otherwise be questionable since heap
 * TIDs can get recycled.)	This holds true even if the page has been modified
 * by inserts and page splits, so there is no need to consult the LSN.
 *
 * If the pin was released after reading the page, then we re-read it.  If it
 * has been modified since we read it (as determined by the LSN), we dare not
 * flag any entries because it is possible that the old entry was vacuumed
 * away and the TID was re-used by a completely different heap tuple.
 */
void
_bt_killitems(IndexScanDesc scan)
{
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	Page		page;
	BTPageOpaque opaque;
	OffsetNumber minoff;
	OffsetNumber maxoff;
	int			i;
	int			numKilled = so->numKilled;
	bool		killedsomething = false;
	bool		droppedpin PG_USED_FOR_ASSERTS_ONLY;

	Assert(BTScanPosIsValid(so->currPos));

	/*
	 * Always reset the scan state, so we don't look for same items on other
	 * pages.
	 */
	so->numKilled = 0;

	if (BTScanPosIsPinned(so->currPos))
	{
		/*
		 * We have held the pin on this page since we read the index tuples,
		 * so all we need to do is lock it.  The pin will have prevented
		 * re-use of any TID on the page, so there is no need to check the
		 * LSN.
		 */
		droppedpin = false;
		_bt_lockbuf(scan->indexRelation, so->currPos.buf, BT_READ);

		page = BufferGetPage(so->currPos.buf);
	}
	else
	{
		Buffer		buf;

		droppedpin = true;
		/* Attempt to re-read the buffer, getting pin and lock. */
		buf = _bt_getbuf(scan->indexRelation, so->currPos.currPage, BT_READ);

		page = BufferGetPage(buf);
		if (BufferGetLSNAtomic(buf) == so->currPos.lsn)
			so->currPos.buf = buf;
		else
		{
			/* Modified while not pinned means hinting is not safe. */
			_bt_relbuf(scan->indexRelation, buf);
			return;
		}
	}

	opaque = BTPageGetOpaque(page);
	minoff = P_FIRSTDATAKEY(opaque);
	maxoff = PageGetMaxOffsetNumber(page);

	for (i = 0; i < numKilled; i++)
	{
		int			itemIndex = so->killedItems[i];
		BTScanPosItem *kitem = &so->currPos.items[itemIndex];
		OffsetNumber offnum = kitem->indexOffset;

		Assert(itemIndex >= so->currPos.firstItem &&
			   itemIndex <= so->currPos.lastItem);
		if (offnum < minoff)
			continue;			/* pure paranoia */
		while (offnum <= maxoff)
		{
			ItemId		iid = PageGetItemId(page, offnum);
			IndexTuple	ituple = (IndexTuple) PageGetItem(page, iid);
			bool		killtuple = false;

			if (BTreeTupleIsPosting(ituple))
			{
				int			pi = i + 1;
				int			nposting = BTreeTupleGetNPosting(ituple);
				int			j;

				/*
				 * We rely on the convention that heap TIDs in the scanpos
				 * items array are stored in ascending heap TID order for a
				 * group of TIDs that originally came from a posting list
				 * tuple.  This convention even applies during backwards
				 * scans, where returning the TIDs in descending order might
				 * seem more natural.  This is about effectiveness, not
				 * correctness.
				 *
				 * Note that the page may have been modified in almost any way
				 * since we first read it (in the !droppedpin case), so it's
				 * possible that this posting list tuple wasn't a posting list
				 * tuple when we first encountered its heap TIDs.
				 */
				for (j = 0; j < nposting; j++)
				{
					ItemPointer item = BTreeTupleGetPostingN(ituple, j);

					if (!ItemPointerEquals(item, &kitem->heapTid))
						break;	/* out of posting list loop */

					/*
					 * kitem must have matching offnum when heap TIDs match,
					 * though only in the common case where the page can't
					 * have been concurrently modified
					 */
					Assert(kitem->indexOffset == offnum || !droppedpin);

					/*
					 * Read-ahead to later kitems here.
					 *
					 * We rely on the assumption that not advancing kitem here
					 * will prevent us from considering the posting list tuple
					 * fully dead by not matching its next heap TID in next
					 * loop iteration.
					 *
					 * If, on the other hand, this is the final heap TID in
					 * the posting list tuple, then tuple gets killed
					 * regardless (i.e. we handle the case where the last
					 * kitem is also the last heap TID in the last index tuple
					 * correctly -- posting tuple still gets killed).
					 */
					if (pi < numKilled)
						kitem = &so->currPos.items[so->killedItems[pi++]];
				}

				/*
				 * Don't bother advancing the outermost loop's int iterator to
				 * avoid processing killed items that relate to the same
				 * offnum/posting list tuple.  This micro-optimization hardly
				 * seems worth it.  (Further iterations of the outermost loop
				 * will fail to match on this same posting list's first heap
				 * TID instead, so we'll advance to the next offnum/index
				 * tuple pretty quickly.)
				 */
				if (j == nposting)
					killtuple = true;
			}
			else if (ItemPointerEquals(&ituple->t_tid, &kitem->heapTid))
				killtuple = true;

			/*
			 * Mark index item as dead, if it isn't already.  Since this
			 * happens while holding a buffer lock possibly in shared mode,
			 * it's possible that multiple processes attempt to do this
			 * simultaneously, leading to multiple full-page images being sent
			 * to WAL (if wal_log_hints or data checksums are enabled), which
			 * is undesirable.
			 */
			if (killtuple && !ItemIdIsDead(iid))
			{
				/* found the item/all posting list items */
				ItemIdMarkDead(iid);
				killedsomething = true;
				break;			/* out of inner search loop */
			}
			offnum = OffsetNumberNext(offnum);
		}
	}

	/*
	 * Since this can be redone later if needed, mark as dirty hint.
	 *
	 * Whenever we mark anything LP_DEAD, we also set the page's
	 * BTP_HAS_GARBAGE flag, which is likewise just a hint.  (Note that we
	 * only rely on the page-level flag in !heapkeyspace indexes.)
	 */
	if (killedsomething)
	{
		opaque->btpo_flags |= BTP_HAS_GARBAGE;
		MarkBufferDirtyHint(so->currPos.buf, true);
	}

	_bt_unlockbuf(scan->indexRelation, so->currPos.buf);
}


/*
 * The following routines manage a shared-memory area in which we track
 * assignment of "vacuum cycle IDs" to currently-active btree vacuuming
 * operations.  There is a single counter which increments each time we
 * start a vacuum to assign it a cycle ID.  Since multiple vacuums could
 * be active concurrently, we have to track the cycle ID for each active
 * vacuum; this requires at most MaxBackends entries (usually far fewer).
 * We assume at most one vacuum can be active for a given index.
 *
 * Access to the shared memory area is controlled by BtreeVacuumLock.
 * In principle we could use a separate lmgr locktag for each index,
 * but a single LWLock is much cheaper, and given the short time that
 * the lock is ever held, the concurrency hit should be minimal.
 */

typedef struct BTOneVacInfo
{
	LockRelId	relid;			/* global identifier of an index */
	BTCycleId	cycleid;		/* cycle ID for its active VACUUM */
} BTOneVacInfo;

typedef struct BTVacInfo
{
	BTCycleId	cycle_ctr;		/* cycle ID most recently assigned */
	int			num_vacuums;	/* number of currently active VACUUMs */
	int			max_vacuums;	/* allocated length of vacuums[] array */
	BTOneVacInfo vacuums[FLEXIBLE_ARRAY_MEMBER];
} BTVacInfo;

static BTVacInfo *btvacinfo;


/*
 * _bt_vacuum_cycleid --- get the active vacuum cycle ID for an index,
 *		or zero if there is no active VACUUM
 *
 * Note: for correct interlocking, the caller must already hold pin and
 * exclusive lock on each buffer it will store the cycle ID into.  This
 * ensures that even if a VACUUM starts immediately afterwards, it cannot
 * process those pages until the page split is complete.
 */
BTCycleId
_bt_vacuum_cycleid(Relation rel)
{
	BTCycleId	result = 0;
	int			i;

	/* Share lock is enough since this is a read-only operation */
	LWLockAcquire(BtreeVacuumLock, LW_SHARED);

	for (i = 0; i < btvacinfo->num_vacuums; i++)
	{
		BTOneVacInfo *vac = &btvacinfo->vacuums[i];

		if (vac->relid.relId == rel->rd_lockInfo.lockRelId.relId &&
			vac->relid.dbId == rel->rd_lockInfo.lockRelId.dbId)
		{
			result = vac->cycleid;
			break;
		}
	}

	LWLockRelease(BtreeVacuumLock);
	return result;
}

/*
 * _bt_start_vacuum --- assign a cycle ID to a just-starting VACUUM operation
 *
 * Note: the caller must guarantee that it will eventually call
 * _bt_end_vacuum, else we'll permanently leak an array slot.  To ensure
 * that this happens even in elog(FATAL) scenarios, the appropriate coding
 * is not just a PG_TRY, but
 *		PG_ENSURE_ERROR_CLEANUP(_bt_end_vacuum_callback, PointerGetDatum(rel))
 */
BTCycleId
_bt_start_vacuum(Relation rel)
{
	BTCycleId	result;
	int			i;
	BTOneVacInfo *vac;

	LWLockAcquire(BtreeVacuumLock, LW_EXCLUSIVE);

	/*
	 * Assign the next cycle ID, being careful to avoid zero as well as the
	 * reserved high values.
	 */
	result = ++(btvacinfo->cycle_ctr);
	if (result == 0 || result > MAX_BT_CYCLE_ID)
		result = btvacinfo->cycle_ctr = 1;

	/* Let's just make sure there's no entry already for this index */
	for (i = 0; i < btvacinfo->num_vacuums; i++)
	{
		vac = &btvacinfo->vacuums[i];
		if (vac->relid.relId == rel->rd_lockInfo.lockRelId.relId &&
			vac->relid.dbId == rel->rd_lockInfo.lockRelId.dbId)
		{
			/*
			 * Unlike most places in the backend, we have to explicitly
			 * release our LWLock before throwing an error.  This is because
			 * we expect _bt_end_vacuum() to be called before transaction
			 * abort cleanup can run to release LWLocks.
			 */
			LWLockRelease(BtreeVacuumLock);
			elog(ERROR, "multiple active vacuums for index \"%s\"",
				 RelationGetRelationName(rel));
		}
	}

	/* OK, add an entry */
	if (btvacinfo->num_vacuums >= btvacinfo->max_vacuums)
	{
		LWLockRelease(BtreeVacuumLock);
		elog(ERROR, "out of btvacinfo slots");
	}
	vac = &btvacinfo->vacuums[btvacinfo->num_vacuums];
	vac->relid = rel->rd_lockInfo.lockRelId;
	vac->cycleid = result;
	btvacinfo->num_vacuums++;

	LWLockRelease(BtreeVacuumLock);
	return result;
}

/*
 * _bt_end_vacuum --- mark a btree VACUUM operation as done
 *
 * Note: this is deliberately coded not to complain if no entry is found;
 * this allows the caller to put PG_TRY around the start_vacuum operation.
 */
void
_bt_end_vacuum(Relation rel)
{
	int			i;

	LWLockAcquire(BtreeVacuumLock, LW_EXCLUSIVE);

	/* Find the array entry */
	for (i = 0; i < btvacinfo->num_vacuums; i++)
	{
		BTOneVacInfo *vac = &btvacinfo->vacuums[i];

		if (vac->relid.relId == rel->rd_lockInfo.lockRelId.relId &&
			vac->relid.dbId == rel->rd_lockInfo.lockRelId.dbId)
		{
			/* Remove it by shifting down the last entry */
			*vac = btvacinfo->vacuums[btvacinfo->num_vacuums - 1];
			btvacinfo->num_vacuums--;
			break;
		}
	}

	LWLockRelease(BtreeVacuumLock);
}

/*
 * _bt_end_vacuum wrapped as an on_shmem_exit callback function
 */
void
_bt_end_vacuum_callback(int code, Datum arg)
{
	_bt_end_vacuum((Relation) DatumGetPointer(arg));
}

/*
 * BTreeShmemSize --- report amount of shared memory space needed
 */
Size
BTreeShmemSize(void)
{
	Size		size;

	size = offsetof(BTVacInfo, vacuums);
	size = add_size(size, mul_size(MaxBackends, sizeof(BTOneVacInfo)));
	return size;
}

/*
 * BTreeShmemInit --- initialize this module's shared memory
 */
void
BTreeShmemInit(void)
{
	bool		found;

	btvacinfo = (BTVacInfo *) ShmemInitStruct("BTree Vacuum State",
											  BTreeShmemSize(),
											  &found);

	if (!IsUnderPostmaster)
	{
		/* Initialize shared memory area */
		Assert(!found);

		/*
		 * It doesn't really matter what the cycle counter starts at, but
		 * having it always start the same doesn't seem good.  Seed with
		 * low-order bits of time() instead.
		 */
		btvacinfo->cycle_ctr = (BTCycleId) time(NULL);

		btvacinfo->num_vacuums = 0;
		btvacinfo->max_vacuums = MaxBackends;
	}
	else
		Assert(found);
}

bytea *
btoptions(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"fillfactor", RELOPT_TYPE_INT, offsetof(BTOptions, fillfactor)},
		{"vacuum_cleanup_index_scale_factor", RELOPT_TYPE_REAL,
		offsetof(BTOptions, vacuum_cleanup_index_scale_factor)},
		{"deduplicate_items", RELOPT_TYPE_BOOL,
		offsetof(BTOptions, deduplicate_items)}
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  RELOPT_KIND_BTREE,
									  sizeof(BTOptions),
									  tab, lengthof(tab));
}

/*
 *	btproperty() -- Check boolean properties of indexes.
 *
 * This is optional, but handling AMPROP_RETURNABLE here saves opening the rel
 * to call btcanreturn.
 */
bool
btproperty(Oid index_oid, int attno,
		   IndexAMProperty prop, const char *propname,
		   bool *res, bool *isnull)
{
	switch (prop)
	{
		case AMPROP_RETURNABLE:
			/* answer only for columns, not AM or whole index */
			if (attno == 0)
				return false;
			/* otherwise, btree can always return data */
			*res = true;
			return true;

		default:
			return false;		/* punt to generic code */
	}
}

/*
 *	btbuildphasename() -- Return name of index build phase.
 */
char *
btbuildphasename(int64 phasenum)
{
	switch (phasenum)
	{
		case PROGRESS_CREATEIDX_SUBPHASE_INITIALIZE:
			return "initializing";
		case PROGRESS_BTREE_PHASE_INDEXBUILD_TABLESCAN:
			return "scanning table";
		case PROGRESS_BTREE_PHASE_PERFORMSORT_1:
			return "sorting live tuples";
		case PROGRESS_BTREE_PHASE_PERFORMSORT_2:
			return "sorting dead tuples";
		case PROGRESS_BTREE_PHASE_LEAF_LOAD:
			return "loading tuples in tree";
		default:
			return NULL;
	}
}

/*
 *	_bt_truncate() -- create tuple without unneeded suffix attributes.
 *
 * Returns truncated pivot index tuple allocated in caller's memory context,
 * with key attributes copied from caller's firstright argument.  If rel is
 * an INCLUDE index, non-key attributes will definitely be truncated away,
 * since they're not part of the key space.  More aggressive suffix
 * truncation can take place when it's clear that the returned tuple does not
 * need one or more suffix key attributes.  We only need to keep firstright
 * attributes up to and including the first non-lastleft-equal attribute.
 * Caller's insertion scankey is used to compare the tuples; the scankey's
 * argument values are not considered here.
 *
 * Note that returned tuple's t_tid offset will hold the number of attributes
 * present, so the original item pointer offset is not represented.  Caller
 * should only change truncated tuple's downlink.  Note also that truncated
 * key attributes are treated as containing "minus infinity" values by
 * _bt_compare().
 *
 * In the worst case (when a heap TID must be appended to distinguish lastleft
 * from firstright), the size of the returned tuple is the size of firstright
 * plus the size of an additional MAXALIGN()'d item pointer.  This guarantee
 * is important, since callers need to stay under the 1/3 of a page
 * restriction on tuple size.  If this routine is ever taught to truncate
 * within an attribute/datum, it will need to avoid returning an enlarged
 * tuple to caller when truncation + TOAST compression ends up enlarging the
 * final datum.
 */
IndexTuple
_bt_truncate(Relation rel, IndexTuple lastleft, IndexTuple firstright,
			 BTScanInsert itup_key)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int16		nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	int			keepnatts;
	IndexTuple	pivot;
	IndexTuple	tidpivot;
	ItemPointer pivotheaptid;
	Size		newsize;

	/*
	 * We should only ever truncate non-pivot tuples from leaf pages.  It's
	 * never okay to truncate when splitting an internal page.
	 */
	Assert(!BTreeTupleIsPivot(lastleft) && !BTreeTupleIsPivot(firstright));

	/* Determine how many attributes must be kept in truncated tuple */
	keepnatts = _bt_keep_natts(rel, lastleft, firstright, itup_key);

#ifdef DEBUG_NO_TRUNCATE
	/* Force truncation to be ineffective for testing purposes */
	keepnatts = nkeyatts + 1;
#endif

	pivot = index_truncate_tuple(itupdesc, firstright,
								 Min(keepnatts, nkeyatts));

	if (BTreeTupleIsPosting(pivot))
	{
		/*
		 * index_truncate_tuple() just returns a straight copy of firstright
		 * when it has no attributes to truncate.  When that happens, we may
		 * need to truncate away a posting list here instead.
		 */
		Assert(keepnatts == nkeyatts || keepnatts == nkeyatts + 1);
		Assert(IndexRelationGetNumberOfAttributes(rel) == nkeyatts);
		pivot->t_info &= ~INDEX_SIZE_MASK;
		pivot->t_info |= MAXALIGN(BTreeTupleGetPostingOffset(firstright));
	}

	/*
	 * If there is a distinguishing key attribute within pivot tuple, we're
	 * done
	 */
	if (keepnatts <= nkeyatts)
	{
		BTreeTupleSetNAtts(pivot, keepnatts, false);
		return pivot;
	}

	/*
	 * We have to store a heap TID in the new pivot tuple, since no non-TID
	 * key attribute value in firstright distinguishes the right side of the
	 * split from the left side.  nbtree conceptualizes this case as an
	 * inability to truncate away any key attributes, since heap TID is
	 * treated as just another key attribute (despite lacking a pg_attribute
	 * entry).
	 *
	 * Use enlarged space that holds a copy of pivot.  We need the extra space
	 * to store a heap TID at the end (using the special pivot tuple
	 * representation).  Note that the original pivot already has firstright's
	 * possible posting list/non-key attribute values removed at this point.
	 */
	newsize = MAXALIGN(IndexTupleSize(pivot)) + MAXALIGN(sizeof(ItemPointerData));
	tidpivot = palloc0(newsize);
	memcpy(tidpivot, pivot, MAXALIGN(IndexTupleSize(pivot)));
	/* Cannot leak memory here */
	pfree(pivot);

	/*
	 * Store all of firstright's key attribute values plus a tiebreaker heap
	 * TID value in enlarged pivot tuple
	 */
	tidpivot->t_info &= ~INDEX_SIZE_MASK;
	tidpivot->t_info |= newsize;
	BTreeTupleSetNAtts(tidpivot, nkeyatts, true);
	pivotheaptid = BTreeTupleGetHeapTID(tidpivot);

	/*
	 * Lehman & Yao use lastleft as the leaf high key in all cases, but don't
	 * consider suffix truncation.  It seems like a good idea to follow that
	 * example in cases where no truncation takes place -- use lastleft's heap
	 * TID.  (This is also the closest value to negative infinity that's
	 * legally usable.)
	 */
	ItemPointerCopy(BTreeTupleGetMaxHeapTID(lastleft), pivotheaptid);

	/*
	 * We're done.  Assert() that heap TID invariants hold before returning.
	 *
	 * Lehman and Yao require that the downlink to the right page, which is to
	 * be inserted into the parent page in the second phase of a page split be
	 * a strict lower bound on items on the right page, and a non-strict upper
	 * bound for items on the left page.  Assert that heap TIDs follow these
	 * invariants, since a heap TID value is apparently needed as a
	 * tiebreaker.
	 */
#ifndef DEBUG_NO_TRUNCATE
	Assert(ItemPointerCompare(BTreeTupleGetMaxHeapTID(lastleft),
							  BTreeTupleGetHeapTID(firstright)) < 0);
	Assert(ItemPointerCompare(pivotheaptid,
							  BTreeTupleGetHeapTID(lastleft)) >= 0);
	Assert(ItemPointerCompare(pivotheaptid,
							  BTreeTupleGetHeapTID(firstright)) < 0);
#else

	/*
	 * Those invariants aren't guaranteed to hold for lastleft + firstright
	 * heap TID attribute values when they're considered here only because
	 * DEBUG_NO_TRUNCATE is defined (a heap TID is probably not actually
	 * needed as a tiebreaker).  DEBUG_NO_TRUNCATE must therefore use a heap
	 * TID value that always works as a strict lower bound for items to the
	 * right.  In particular, it must avoid using firstright's leading key
	 * attribute values along with lastleft's heap TID value when lastleft's
	 * TID happens to be greater than firstright's TID.
	 */
	ItemPointerCopy(BTreeTupleGetHeapTID(firstright), pivotheaptid);

	/*
	 * Pivot heap TID should never be fully equal to firstright.  Note that
	 * the pivot heap TID will still end up equal to lastleft's heap TID when
	 * that's the only usable value.
	 */
	ItemPointerSetOffsetNumber(pivotheaptid,
							   OffsetNumberPrev(ItemPointerGetOffsetNumber(pivotheaptid)));
	Assert(ItemPointerCompare(pivotheaptid,
							  BTreeTupleGetHeapTID(firstright)) < 0);
#endif

	return tidpivot;
}

/*
 * _bt_keep_natts - how many key attributes to keep when truncating.
 *
 * Caller provides two tuples that enclose a split point.  Caller's insertion
 * scankey is used to compare the tuples; the scankey's argument values are
 * not considered here.
 *
 * This can return a number of attributes that is one greater than the
 * number of key attributes for the index relation.  This indicates that the
 * caller must use a heap TID as a unique-ifier in new pivot tuple.
 */
static int
_bt_keep_natts(Relation rel, IndexTuple lastleft, IndexTuple firstright,
			   BTScanInsert itup_key)
{
	int			nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			keepnatts;
	ScanKey		scankey;

	/*
	 * _bt_compare() treats truncated key attributes as having the value minus
	 * infinity, which would break searches within !heapkeyspace indexes.  We
	 * must still truncate away non-key attribute values, though.
	 */
	if (!itup_key->heapkeyspace)
		return nkeyatts;

	scankey = itup_key->scankeys;
	keepnatts = 1;
	for (int attnum = 1; attnum <= nkeyatts; attnum++, scankey++)
	{
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;

		datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
		datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);

		if (isNull1 != isNull2)
			break;

		if (!isNull1 &&
			DatumGetInt32(FunctionCall2Coll(&scankey->sk_func,
											scankey->sk_collation,
											datum1,
											datum2)) != 0)
			break;

		keepnatts++;
	}

	/*
	 * Assert that _bt_keep_natts_fast() agrees with us in passing.  This is
	 * expected in an allequalimage index.
	 */
	Assert(!itup_key->allequalimage ||
		   keepnatts == _bt_keep_natts_fast(rel, lastleft, firstright));

	return keepnatts;
}

/*
 * _bt_keep_natts_fast - fast bitwise variant of _bt_keep_natts.
 *
 * This is exported so that a candidate split point can have its effect on
 * suffix truncation inexpensively evaluated ahead of time when finding a
 * split location.  A naive bitwise approach to datum comparisons is used to
 * save cycles.
 *
 * The approach taken here usually provides the same answer as _bt_keep_natts
 * will (for the same pair of tuples from a heapkeyspace index), since the
 * majority of btree opclasses can never indicate that two datums are equal
 * unless they're bitwise equal after detoasting.  When an index only has
 * "equal image" columns, routine is guaranteed to give the same result as
 * _bt_keep_natts would.
 *
 * Callers can rely on the fact that attributes considered equal here are
 * definitely also equal according to _bt_keep_natts, even when the index uses
 * an opclass or collation that is not "allequalimage"/deduplication-safe.
 * This weaker guarantee is good enough for nbtsplitloc.c caller, since false
 * negatives generally only have the effect of making leaf page splits use a
 * more balanced split point.
 */
int
_bt_keep_natts_fast(Relation rel, IndexTuple lastleft, IndexTuple firstright)
{
	TupleDesc	itupdesc = RelationGetDescr(rel);
	int			keysz = IndexRelationGetNumberOfKeyAttributes(rel);
	int			keepnatts;

	keepnatts = 1;
	for (int attnum = 1; attnum <= keysz; attnum++)
	{
		Datum		datum1,
					datum2;
		bool		isNull1,
					isNull2;
		Form_pg_attribute att;

		datum1 = index_getattr(lastleft, attnum, itupdesc, &isNull1);
		datum2 = index_getattr(firstright, attnum, itupdesc, &isNull2);
		att = TupleDescAttr(itupdesc, attnum - 1);

		if (isNull1 != isNull2)
			break;

		if (!isNull1 &&
			!datum_image_eq(datum1, datum2, att->attbyval, att->attlen))
			break;

		keepnatts++;
	}

	return keepnatts;
}

/*
 *  _bt_check_natts() -- Verify tuple has expected number of attributes.
 *
 * Returns value indicating if the expected number of attributes were found
 * for a particular offset on page.  This can be used as a general purpose
 * sanity check.
 *
 * Testing a tuple directly with BTreeTupleGetNAtts() should generally be
 * preferred to calling here.  That's usually more convenient, and is always
 * more explicit.  Call here instead when offnum's tuple may be a negative
 * infinity tuple that uses the pre-v11 on-disk representation, or when a low
 * context check is appropriate.  This routine is as strict as possible about
 * what is expected on each version of btree.
 */
bool
_bt_check_natts(Relation rel, bool heapkeyspace, Page page, OffsetNumber offnum)
{
	int16		natts = IndexRelationGetNumberOfAttributes(rel);
	int16		nkeyatts = IndexRelationGetNumberOfKeyAttributes(rel);
	BTPageOpaque opaque = BTPageGetOpaque(page);
	IndexTuple	itup;
	int			tupnatts;

	/*
	 * We cannot reliably test a deleted or half-dead page, since they have
	 * dummy high keys
	 */
	if (P_IGNORE(opaque))
		return true;

	Assert(offnum >= FirstOffsetNumber &&
		   offnum <= PageGetMaxOffsetNumber(page));

	itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, offnum));
	tupnatts = BTreeTupleGetNAtts(itup, rel);

	/* !heapkeyspace indexes do not support deduplication */
	if (!heapkeyspace && BTreeTupleIsPosting(itup))
		return false;

	/* Posting list tuples should never have "pivot heap TID" bit set */
	if (BTreeTupleIsPosting(itup) &&
		(ItemPointerGetOffsetNumberNoCheck(&itup->t_tid) &
		 BT_PIVOT_HEAP_TID_ATTR) != 0)
		return false;

	/* INCLUDE indexes do not support deduplication */
	if (natts != nkeyatts && BTreeTupleIsPosting(itup))
		return false;

	if (P_ISLEAF(opaque))
	{
		if (offnum >= P_FIRSTDATAKEY(opaque))
		{
			/*
			 * Non-pivot tuple should never be explicitly marked as a pivot
			 * tuple
			 */
			if (BTreeTupleIsPivot(itup))
				return false;

			/*
			 * Leaf tuples that are not the page high key (non-pivot tuples)
			 * should never be truncated.  (Note that tupnatts must have been
			 * inferred, even with a posting list tuple, because only pivot
			 * tuples store tupnatts directly.)
			 */
			return tupnatts == natts;
		}
		else
		{
			/*
			 * Rightmost page doesn't contain a page high key, so tuple was
			 * checked above as ordinary leaf tuple
			 */
			Assert(!P_RIGHTMOST(opaque));

			/*
			 * !heapkeyspace high key tuple contains only key attributes. Note
			 * that tupnatts will only have been explicitly represented in
			 * !heapkeyspace indexes that happen to have non-key attributes.
			 */
			if (!heapkeyspace)
				return tupnatts == nkeyatts;

			/* Use generic heapkeyspace pivot tuple handling */
		}
	}
	else						/* !P_ISLEAF(opaque) */
	{
		if (offnum == P_FIRSTDATAKEY(opaque))
		{
			/*
			 * The first tuple on any internal page (possibly the first after
			 * its high key) is its negative infinity tuple.  Negative
			 * infinity tuples are always truncated to zero attributes.  They
			 * are a particular kind of pivot tuple.
			 */
			if (heapkeyspace)
				return tupnatts == 0;

			/*
			 * The number of attributes won't be explicitly represented if the
			 * negative infinity tuple was generated during a page split that
			 * occurred with a version of Postgres before v11.  There must be
			 * a problem when there is an explicit representation that is
			 * non-zero, or when there is no explicit representation and the
			 * tuple is evidently not a pre-pg_upgrade tuple.
			 *
			 * Prior to v11, downlinks always had P_HIKEY as their offset.
			 * Accept that as an alternative indication of a valid
			 * !heapkeyspace negative infinity tuple.
			 */
			return tupnatts == 0 ||
				ItemPointerGetOffsetNumber(&(itup->t_tid)) == P_HIKEY;
		}
		else
		{
			/*
			 * !heapkeyspace downlink tuple with separator key contains only
			 * key attributes.  Note that tupnatts will only have been
			 * explicitly represented in !heapkeyspace indexes that happen to
			 * have non-key attributes.
			 */
			if (!heapkeyspace)
				return tupnatts == nkeyatts;

			/* Use generic heapkeyspace pivot tuple handling */
		}
	}

	/* Handle heapkeyspace pivot tuples (excluding minus infinity items) */
	Assert(heapkeyspace);

	/*
	 * Explicit representation of the number of attributes is mandatory with
	 * heapkeyspace index pivot tuples, regardless of whether or not there are
	 * non-key attributes.
	 */
	if (!BTreeTupleIsPivot(itup))
		return false;

	/* Pivot tuple should not use posting list representation (redundant) */
	if (BTreeTupleIsPosting(itup))
		return false;

	/*
	 * Heap TID is a tiebreaker key attribute, so it cannot be untruncated
	 * when any other key attribute is truncated
	 */
	if (BTreeTupleGetHeapTID(itup) != NULL && tupnatts != nkeyatts)
		return false;

	/*
	 * Pivot tuple must have at least one untruncated key attribute (minus
	 * infinity pivot tuples are the only exception).  Pivot tuples can never
	 * represent that there is a value present for a key attribute that
	 * exceeds pg_index.indnkeyatts for the index.
	 */
	return tupnatts > 0 && tupnatts <= nkeyatts;
}

/*
 *
 *  _bt_check_third_page() -- check whether tuple fits on a btree page at all.
 *
 * We actually need to be able to fit three items on every page, so restrict
 * any one item to 1/3 the per-page available space.  Note that itemsz should
 * not include the ItemId overhead.
 *
 * It might be useful to apply TOAST methods rather than throw an error here.
 * Using out of line storage would break assumptions made by suffix truncation
 * and by contrib/amcheck, though.
 */
void
_bt_check_third_page(Relation rel, Relation heap, bool needheaptidspace,
					 Page page, IndexTuple newtup)
{
	Size		itemsz;
	BTPageOpaque opaque;

	itemsz = MAXALIGN(IndexTupleSize(newtup));

	/* Double check item size against limit */
	if (itemsz <= BTMaxItemSize(page))
		return;

	/*
	 * Tuple is probably too large to fit on page, but it's possible that the
	 * index uses version 2 or version 3, or that page is an internal page, in
	 * which case a slightly higher limit applies.
	 */
	if (!needheaptidspace && itemsz <= BTMaxItemSizeNoHeapTid(page))
		return;

	/*
	 * Internal page insertions cannot fail here, because that would mean that
	 * an earlier leaf level insertion that should have failed didn't
	 */
	opaque = BTPageGetOpaque(page);
	if (!P_ISLEAF(opaque))
		elog(ERROR, "cannot insert oversized tuple of size %zu on internal page of index \"%s\"",
			 itemsz, RelationGetRelationName(rel));

	ereport(ERROR,
			(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
			 errmsg("index row size %zu exceeds btree version %u maximum %zu for index \"%s\"",
					itemsz,
					needheaptidspace ? BTREE_VERSION : BTREE_NOVAC_VERSION,
					needheaptidspace ? BTMaxItemSize(page) :
					BTMaxItemSizeNoHeapTid(page),
					RelationGetRelationName(rel)),
			 errdetail("Index row references tuple (%u,%u) in relation \"%s\".",
					   ItemPointerGetBlockNumber(BTreeTupleGetHeapTID(newtup)),
					   ItemPointerGetOffsetNumber(BTreeTupleGetHeapTID(newtup)),
					   RelationGetRelationName(heap)),
			 errhint("Values larger than 1/3 of a buffer page cannot be indexed.\n"
					 "Consider a function index of an MD5 hash of the value, "
					 "or use full text indexing."),
			 errtableconstraint(heap, RelationGetRelationName(rel))));
}

/*
 * Are all attributes in rel "equality is image equality" attributes?
 *
 * We use each attribute's BTEQUALIMAGE_PROC opclass procedure.  If any
 * opclass either lacks a BTEQUALIMAGE_PROC procedure or returns false, we
 * return false; otherwise we return true.
 *
 * Returned boolean value is stored in index metapage during index builds.
 * Deduplication can only be used when we return true.
 */
bool
_bt_allequalimage(Relation rel, bool debugmessage)
{
	bool		allequalimage = true;

	/* INCLUDE indexes can never support deduplication */
	if (IndexRelationGetNumberOfAttributes(rel) !=
		IndexRelationGetNumberOfKeyAttributes(rel))
		return false;

	for (int i = 0; i < IndexRelationGetNumberOfKeyAttributes(rel); i++)
	{
		Oid			opfamily = rel->rd_opfamily[i];
		Oid			opcintype = rel->rd_opcintype[i];
		Oid			collation = rel->rd_indcollation[i];
		Oid			equalimageproc;

		equalimageproc = get_opfamily_proc(opfamily, opcintype, opcintype,
										   BTEQUALIMAGE_PROC);

		/*
		 * If there is no BTEQUALIMAGE_PROC then deduplication is assumed to
		 * be unsafe.  Otherwise, actually call proc and see what it says.
		 */
		if (!OidIsValid(equalimageproc) ||
			!DatumGetBool(OidFunctionCall1Coll(equalimageproc, collation,
											   ObjectIdGetDatum(opcintype))))
		{
			allequalimage = false;
			break;
		}
	}

	if (debugmessage)
	{
		if (allequalimage)
			elog(DEBUG1, "index \"%s\" can safely use deduplication",
				 RelationGetRelationName(rel));
		else
			elog(DEBUG1, "index \"%s\" cannot use deduplication",
				 RelationGetRelationName(rel));
	}

	return allequalimage;
}
