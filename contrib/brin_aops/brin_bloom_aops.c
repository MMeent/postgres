/*
 * contrib/btree_gin/btree_gin.c
 */
#include "postgres.h"

#include "access/brin_internal.h"
#include "access/brin_tuple.h"
#include "access/genam.h"
#include "access/stratnum.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_type.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/fmgroids.h"

#define BloomEqualStrategyNumber		1
#define BloomContainsStrategyNumber		2
#define BloomContainedStrategyNumber	3
#define BloomOverlapsStrategyNumber		4

/*
 * Additional SQL level support functions. We only have one, which is
 * used to calculate hash of the input value.
 *
 * Procedure numbers must not use values reserved for BRIN itself; see
 * brin_internal.h.
 */
#define		BLOOM_MAX_PROCNUMS		1	/* maximum support procs we need */
#define		PROCNUM_HASH			11	/* required */

/*
 * Subtract this from procnum to obtain index in BloomOpaque arrays
 * (Must be equal to minimum of private procnums).
 */
#define		PROCNUM_BASE			11

/*
 * Storage type for BRIN's reloptions.
 */
typedef struct BloomOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	double		nDistinctPerRange;	/* number of distinct values per range */
	double		falsePositiveRate;	/* false positive for bloom filter */
} BloomOptions;

/*
 * The current min value (16) is somewhat arbitrary, but it's based
 * on the fact that the filter header is ~20B alone, which is about
 * the same as the filter bitmap for 16 distinct items with 1% false
 * positive rate. So by allowing lower values we'd not gain much. In
 * any case, the min should not be larger than MaxHeapTuplesPerPage
 * (~290), which is the theoretical maximum for single-page ranges.
 */
#define		BLOOM_MIN_NDISTINCT_PER_RANGE		16

/*
 * Used to determine number of distinct items, based on the number of rows
 * in a page range. The 10% is somewhat similar to what estimate_num_groups
 * does, so we use the same factor here.
 */
#define		BLOOM_DEFAULT_NDISTINCT_PER_RANGE	-0.1	/* 10% of values */

/*
 * Allowed range and default value for the false positive range. The exact
 * values are somewhat arbitrary, but were chosen considering the various
 * parameters (size of filter vs. page size, etc.).
 *
 * The lower the false-positive rate, the more accurate the filter is, but
 * it also gets larger - at some point this eliminates the main advantage
 * of BRIN indexes, which is the tiny size. At 0.01% the index is about
 * 10% of the table (assuming 290 distinct values per 8kB page).
 *
 * On the other hand, as the false-positive rate increases, larger part of
 * the table has to be scanned due to mismatches - at 25% we're probably
 * close to sequential scan being cheaper.
 */
#define		BLOOM_MIN_FALSE_POSITIVE_RATE	0.0001	/* 0.01% fp rate */
#define		BLOOM_MAX_FALSE_POSITIVE_RATE	0.25	/* 25% fp rate */
#define		BLOOM_DEFAULT_FALSE_POSITIVE_RATE	0.01	/* 1% fp rate */

#define BloomGetNDistinctPerRange(opts) \
	((opts) && (((BloomOptions *) (opts))->nDistinctPerRange != 0) ? \
	 (((BloomOptions *) (opts))->nDistinctPerRange) : \
	 BLOOM_DEFAULT_NDISTINCT_PER_RANGE)

#define BloomGetFalsePositiveRate(opts) \
	((opts) && (((BloomOptions *) (opts))->falsePositiveRate != 0.0) ? \
	 (((BloomOptions *) (opts))->falsePositiveRate) : \
	 BLOOM_DEFAULT_FALSE_POSITIVE_RATE)

/*
 * And estimate of the largest bloom we can fit onto a page. This is not
 * a perfect guarantee, for a couple of reasons. For example, the row may
 * be larger because the index has multiple columns.
 */
#define BloomMaxFilterSize \
	MAXALIGN_DOWN(BLCKSZ - \
				  (MAXALIGN(SizeOfPageHeaderData + \
							sizeof(ItemIdData)) + \
				   MAXALIGN(sizeof(BrinSpecialSpace)) + \
				   SizeOfBrinTuple))

/*
 * Seeds used to calculate two hash functions h1 and h2, which are then used
 * to generate k hashes using the (h1 + i * h2) scheme.
 */
#define BLOOM_SEED_1	0x71d924af
#define BLOOM_SEED_2	0xba48b314

/*
 * Bloom Filter
 *
 * Represents a bloom filter, built on hashes of the indexed values. That is,
 * we compute a uint32 hash of the value, and then store this hash into the
 * bloom filter (and compute additional hashes on it).
 *
 * XXX We could implement "sparse" bloom filters, keeping only the bytes that
 * are not entirely 0. But while indexes don't support TOAST, the varlena can
 * still be compressed. So this seems unnecessary, because the compression
 * should do the same job.
 *
 * XXX We can also watch the number of bits set in the bloom filter, and then
 * stop using it (and not store the bitmap, to save space) when the false
 * positive rate gets too high. But even if the false positive rate exceeds the
 * desired value, it still can eliminate some page ranges.
 */
typedef struct BloomFilter
{
	/* varlena header (do not touch directly!) */
	int32		vl_len_;

	/* space for various flags (unused for now) */
	uint16		flags;

	/* fields for the HASHED phase */
	uint8		nhashes;		/* number of hash functions */
	uint32		nbits;			/* number of bits in the bitmap (size) */
	uint32		nbits_set;		/* number of bits set to 1 */

	/* data of the bloom filter */
	char		data[FLEXIBLE_ARRAY_MEMBER];
} BloomFilter;

#define BAOP_BL_HAS_EMPTY	0x01
#define BAOP_BL_HAS_NULL	0x02

typedef struct BAOpsBloomOpaque
{
	/*
	 * XXX At this point we only need a single proc (to compute the hash), but
	 * let's keep the array just like inclusion and bloom opclasses, for
	 * consistency. We may need additional procs in the future.
	 */
	FmgrInfo	extra_procinfos[BLOOM_MAX_PROCNUMS];
	bool		extra_proc_missing[BLOOM_MAX_PROCNUMS];
} BloomOpaque;

static FmgrInfo *bloom_get_procinfo(BrinDesc *bdesc, uint16 attno,
									uint16 procnum);

PG_FUNCTION_INFO_V1(brin_aops_bloom_opcinfo);
PG_FUNCTION_INFO_V1(brin_aops_bloom_add_value);
PG_FUNCTION_INFO_V1(brin_aops_bloom_consistent);
PG_FUNCTION_INFO_V1(brin_aops_bloom_union);
PG_FUNCTION_INFO_V1(brin_aops_bloom_options);

PG_FUNCTION_INFO_V1(brin_aops_bloom_storage_in);
PG_FUNCTION_INFO_V1(brin_aops_bloom_storage_out);
PG_FUNCTION_INFO_V1(brin_aops_bloom_storage_send);
PG_FUNCTION_INFO_V1(brin_aops_bloom_storage_recv);

typedef struct ArrayMinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} ArrayMinmaxOpaque;

static FmgrInfo *baop_bloom_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
												  Oid subtype, uint16 strategynum);

Datum
brin_aops_bloom_storage_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "brin_aops_bloom_storage")));
}

Datum
brin_aops_bloom_storage_out(PG_FUNCTION_ARGS)
{
	BloomFilter *filter;
	StringInfoData str;

	filter = (BloomFilter *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));
	initStringInfo(&str);
	appendStringInfoChar(&str, '{');

	appendStringInfo(&str, "mode: hashed  flags: %x  nhashes: %u  nbits: %u  nbits_set: %u",
					 filter->flags, filter->nhashes, filter->nbits, filter->nbits_set);

	appendStringInfoChar(&str, '}');

	PG_RETURN_CSTRING(str.data);
}

Datum
brin_aops_bloom_storage_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}

Datum
brin_aops_bloom_storage_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot accept a value of type %s", "brin_aops_bloom_storage")));
}

Datum
brin_aops_bloom_opcinfo(PG_FUNCTION_ARGS)
{
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 *
	 * bloom indexes only store the filter as a single BYTEA column
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(2)) +
					 sizeof(BloomOpaque));
	result->oi_nstored = 2;
	result->oi_regular_nulls = true;
	result->oi_opaque = (BloomOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(2));
	result->oi_typcache[0] = lookup_type_cache(BOOLOID, 0);
	result->oi_typcache[1] = lookup_type_cache(PG_BRIN_BLOOM_SUMMARYOID, 0);


	PG_RETURN_POINTER(result);
}

/*
 * Examine the given index tuple (which contains partial status of a certain
 * page range) by comparing it to the given value that comes from another heap
 * tuple.  If the new value is outside the min/max range specified by the
 * existing tuple values, update the index tuple and return true.  Otherwise,
 * return false and do not modify in this case.
 */
Datum
brin_aops_bloom_add_value(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	bool		isnull PG_USED_FOR_ASSERTS_ONLY = PG_GETARG_DATUM(3);
	Oid			element_type;
	bool		updated = false;
	ArrayType  *array;
	ArrayMetaState *my_extra;
	ArrayIterator array_iterator;
	Datum		value;
	bool		value_isnull;
	AttrNumber	attno;
	Form_pg_attribute attr;
	FmgrInfo   *cmpFn;
	Datum		compar;
	Oid			colloid = PG_GET_COLLATION();


	if (isnull)
	{
		if (column->bv_allnulls || column->bv_hasnulls)
			PG_RETURN_BOOL(false);

		column->bv_hasnulls = true;
		PG_RETURN_BOOL(true);
	}

	/* prepare array operations */
	array = PG_GETARG_ARRAYTYPE_P(2);
	element_type = ARR_ELEMTYPE(array);
	Assert(element_type == TupleDescAttr(bdesc->bd_tupdesc, column->bv_attno)->atttypid);

	if (ARR_SIZE(array) == 0)
	{
		if (column->bv_values[BAOP_MM_ATT_ANY_EMPTY] != true)
			updated = true;
		column->bv_values[BAOP_MM_ATT_ANY_EMPTY] = true;
		PG_RETURN_BOOL(updated);
	}

	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		TypeCacheEntry *typentry;

		get_typlenbyvalalign(element_type,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);

		typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify an equality operator for type %s",
							   format_type_be(element_type))));

		my_extra->element_type = element_type;
		fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
	}

	/* prepare bloom operations */
	attno = column->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &value_isnull))
	{
		/*
		 * If the recorded entry is null, store the new value (which we know to be
		 * not null) as both minimum and maximum, and we're done.
		 */
		if (!value_isnull)
		{
			column->bv_values[BAOP_MM_ATT_MIN] = datumCopy(value, attr->attbyval, attr->attlen);
			column->bv_values[BAOP_MM_ATT_MAX] = datumCopy(value, attr->attbyval, attr->attlen);
			column->bv_values[BAOP_MM_ATT_ANY_NULLS] = false;
			column->bv_values[3] = false;
			updated = true;
			continue;
		}

		/*
		 * If the entry is null, record that it's null, and continue
		 */
		if (value_isnull)
		{
			if (!DatumGetBool(column->bv_values[2]))
				updated = true;
			column->bv_values[2] = BoolGetDatum(true);
			continue;
		}

		/*
		 * Otherwise, need to compare the new value with the existing boundaries
		 * and update them accordingly.  First check if it's less than the
		 * existing minimum.
		 */
		cmpFn = baop_bloom_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												  BTLessStrategyNumber);
		compar = FunctionCall2Coll(cmpFn, colloid, value, column->bv_values[0]);
		if (DatumGetBool(compar))
		{
			if (!attr->attbyval)
				pfree(DatumGetPointer(column->bv_values[0]));
			column->bv_values[0] = datumCopy(value, attr->attbyval, attr->attlen);
			updated = true;
		}

		cmpFn = baop_bloom_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												  BTGreaterStrategyNumber);
		compar = FunctionCall2Coll(cmpFn, colloid, value, column->bv_values[1]);
		if (DatumGetBool(compar))
		{
			if (!attr->attbyval)
				pfree(DatumGetPointer(column->bv_values[1]));
			column->bv_values[1] = datumCopy(value, attr->attbyval, attr->attlen);
			updated = true;
		}
	}
	PG_RETURN_BOOL(updated);
}

Datum
brin_aops_bloom_consistent(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	ScanKey		key = (ScanKey) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION(),
				subtype;
	AttrNumber	attno;
	ArrayType  *array;
	Oid			element_type;
	ArrayMetaState *my_extra;
	bool		matches = true;
	Datum		value;
	bool		value_isnull;
	ArrayIterator array_iterator;

	/* This opclass uses the old signature with only three arguments. */
	Assert(PG_NARGS() == 3);
	/* Should not be dealing with all-NULL ranges. */
	Assert(!column->bv_allnulls);

	attno = key->sk_attno;
	subtype = key->sk_subtype;

	array = DatumGetArrayTypeP(key->sk_argument);
	element_type = ARR_ELEMTYPE(array);

	Assert(element_type == TupleDescAttr(bdesc->bd_tupdesc, column->bv_attno)->atttypid);

	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		my_extra->element_type = ~element_type;
	}

	if (my_extra->element_type != element_type)
	{
		TypeCacheEntry *typentry;

		get_typlenbyvalalign(element_type,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);

		typentry = lookup_type_cache(element_type, TYPECACHE_EQ_OPR_FINFO);

		if (!OidIsValid(typentry->eq_opr_finfo.fn_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify an equality operator for type %s",
							   format_type_be(element_type))));

		my_extra->element_type = element_type;
		fmgr_info_cxt(typentry->eq_opr_finfo.fn_oid, &my_extra->proc,
					  fcinfo->flinfo->fn_mcxt);
	}

	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &value_isnull))
	{
		switch (key->sk_strategy) {

		}
	}

	PG_RETURN_DATUM(matches);
}

Datum
brin_aops_bloom_union(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION();
	AttrNumber	attno;
	Form_pg_attribute attr;
	FmgrInfo   *finfo;
	bool		needsadj;

	Assert(col_a->bv_attno == col_b->bv_attno);
	Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/* Adjust minimum, if B's min is less than A's min */
	finfo = baop_bloom_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											  BTLessStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[0],
								 col_a->bv_values[0]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[0]));
		col_a->bv_values[0] = datumCopy(col_b->bv_values[0],
										attr->attbyval, attr->attlen);
	}

	/* Adjust maximum, if B's max is greater than A's max */
	finfo = baop_bloom_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											  BTGreaterStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, col_b->bv_values[1],
								 col_a->bv_values[1]);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(col_a->bv_values[1]));
		col_a->bv_values[1] = datumCopy(col_b->bv_values[1],
										attr->attbyval, attr->attlen);
	}

	if (DatumGetBool(col_a->bv_values[BAOP_MM_ATT_ANY_NULLS]) !=
		DatumGetBool(col_b->bv_values[BAOP_MM_ATT_ANY_NULLS]))
		col_a->bv_values[BAOP_MM_ATT_ANY_NULLS] = BoolGetDatum(true);

	if (DatumGetBool(col_a->bv_values[BAOP_MM_ATT_ANY_EMPTY]) !=
		DatumGetBool(col_b->bv_values[BAOP_MM_ATT_ANY_EMPTY]))
		col_a->bv_values[BAOP_MM_ATT_ANY_EMPTY] = BoolGetDatum(true);

	PG_RETURN_VOID();
}



/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors inclusion_get_strategy_procinfo; see notes
 * there.  If changes are made here, see that function too.
 */
static FmgrInfo *
baop_bloom_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype,
								 uint16 strategynum)
{
	BAOpsMinmaxOpaque *opaque;

	Assert(strategynum >= 1 &&
		   strategynum <= BTMaxStrategyNumber);

	opaque = (BAOpsMinmaxOpaque *) bdesc->bd_info[attno - 1]->oi_opaque;

	/*
	 * We cache the procedures for the previous subtype in the opaque struct,
	 * to avoid repetitive syscache lookups.  If the subtype changed,
	 * invalidate all the cached entries.
	 */
	if (opaque->cached_subtype != subtype)
	{
		uint16		i;

		for (i = 1; i <= BTMaxStrategyNumber; i++)
			opaque->strategy_procinfos[i - 1].fn_oid = InvalidOid;
		opaque->cached_subtype = subtype;
	}

	if (opaque->strategy_procinfos[strategynum - 1].fn_oid == InvalidOid)
	{
		Form_pg_attribute attr;
		HeapTuple	tuple;
		Oid			opfamily,
			oprid;

		opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
		attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
		tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(subtype),
								Int16GetDatum(strategynum));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, subtype, opfamily);

		oprid = DatumGetObjectId(SysCacheGetAttrNotNull(AMOPSTRATEGY, tuple,
														Anum_pg_amop_amopopr));
		ReleaseSysCache(tuple);
		Assert(RegProcedureIsValid(oprid));

		fmgr_info_cxt(get_opcode(oprid),
					  &opaque->strategy_procinfos[strategynum - 1],
					  bdesc->bd_context);
	}

	return &opaque->strategy_procinfos[strategynum - 1];
}
