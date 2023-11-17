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
#include "brin_aops.h"
#include "access/tupmacs.h"
#include "catalog/pg_am_d.h"
#include "commands/defrem.h"

PG_MODULE_MAGIC;

typedef struct BAOpsMinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} BAOpsMinmaxOpaque;

#define BAOP_MM_ATT_MIN 0
#define BAOP_MM_ATT_MAX 1
#define BAOP_MM_ATT_ANY_NULLS 2
#define BAOP_MM_ATT_ANY_EMPTY 3

#define BAOP_MM_NATTS (BAOP_MM_ATT_ANY_EMPTY + 1)

PG_FUNCTION_INFO_V1(brin_aops_minmax_opcinfo);
PG_FUNCTION_INFO_V1(brin_aops_minmax_add_value);
PG_FUNCTION_INFO_V1(brin_aops_minmax_consistent);
PG_FUNCTION_INFO_V1(brin_aops_minmax_union);

PG_FUNCTION_INFO_V1(brin_aops_minmax_storage_in);
PG_FUNCTION_INFO_V1(brin_aops_minmax_storage_out);
PG_FUNCTION_INFO_V1(brin_aops_minmax_storage_send);
PG_FUNCTION_INFO_V1(brin_aops_minmax_storage_recv);

typedef struct ArrayMinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} ArrayMinmaxOpaque;

typedef struct SerializedArrayMinMax
{
	/* varlena header (do not touch directly!) */
	int32		vl_len_;

	/* type of values stored in the data array */
	Oid			typid;
	int8		flags;
	char		data[FLEXIBLE_ARRAY_MEMBER];
} SerializedArrayMinMax;

#define AOPS_MINMAX_HAS_NULLS		0x1
#define AOPS_MINMAX_HAS_EMPTY		0x2
#define AOPS_MINMAX_VALID_MINMAX	0x4

typedef struct ArrayMinMax
{
	/* Cache information that we need quite often. */
	Oid			typid;

	bool		any_nulls;
	bool		any_empty;
	bool		minmax_valid;
	Datum		min;
	Datum		max;
} ArrayMinMax;

static SerializedArrayMinMax *baop_minmax_serialize(ArrayMinMax *range);
static ArrayMinMax *baop_minmax_deserialize(SerializedArrayMinMax *serialized);
static ArrayMinMax *baop_minmax_init();


static FmgrInfo *baop_minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
												   Oid subtype, uint16 strategynum);

Datum
brin_aops_minmax_opcinfo(PG_FUNCTION_ARGS)
{
	BrinOpcInfo *result;
	baop_initialize_cache();

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(1)) +
					 sizeof(ArrayMinmaxOpaque));
	result->oi_nstored = 1;
	result->oi_regular_nulls = true;
	result->oi_opaque = (ArrayMinmaxOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(1));
	result->oi_typcache[0] =
		lookup_type_cache(BRIN_AOPS_CONSTANTS->minmax_storage_oid, 0);

	PG_RETURN_POINTER(result);
}
static void
baop_minmax_serialize_tup(BrinDesc *bdesc, Datum src, Datum *dst)
{
	ArrayMinMax	*minmax = (ArrayMinMax *) DatumGetPointer(src);
	SerializedArrayMinMax *s;

	s = baop_minmax_serialize(minmax);
	dst[0] = PointerGetDatum(s);
}

/*
 * Examine the given index tuple (which contains partial status of a certain
 * page range) by comparing it to the given value that comes from another heap
 * tuple.  If the new value is outside the min/max range specified by the
 * existing tuple values, update the index tuple and return true.  Otherwise,
 * return false and do not modify in this case.
 */
Datum
brin_aops_minmax_add_value(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	bool		isnull PG_USED_FOR_ASSERTS_ONLY = PG_GETARG_DATUM(3);
	Oid			element_type;
	bool		modified = false;
	ArrayMinMax *minMax;
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

	attno = column->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);
	minMax = (ArrayMinMax *) DatumGetPointer(column->bv_mem_value);

	if (column->bv_allnulls)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(column->bv_context);

		minMax = baop_minmax_init();
		minMax->typid = attr->atttypid;


		MemoryContextSwitchTo(oldctx);

		column->bv_allnulls = false;
		modified = true;

		column->bv_mem_value = PointerGetDatum(minMax);
		column->bv_serialize = baop_minmax_serialize_tup;
	}
	else if (!minMax)
	{
		MemoryContext oldctx = MemoryContextSwitchTo(column->bv_context);
		SerializedArrayMinMax *serialized;


		serialized = (SerializedArrayMinMax *) PG_DETOAST_DATUM(column->bv_values[0]);

		minMax = baop_minmax_deserialize(serialized);

		column->bv_mem_value = PointerGetDatum(minMax);
		column->bv_serialize = baop_minmax_serialize_tup;

		MemoryContextSwitchTo(oldctx);
	}

	/* prepare array operations */
	array = PG_GETARG_ARRAYTYPE_P(2);
	element_type = ARR_ELEMTYPE(array);
	Assert(element_type == TupleDescAttr(bdesc->bd_tupdesc, column->bv_attno)->atttypid);

	if (ARR_SIZE(array) == 0)
	{
		if (!minMax->any_empty)
			modified = true;
		minMax->any_empty = true;
		PG_RETURN_BOOL(modified);
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

	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &value_isnull))
	{
		/*
		 * If the recorded entry is null, store the new value (which we know to be
		 * not null) as both minimum and maximum, and we're done.
		 */
		if (!value_isnull && !minMax->minmax_valid)
		{
			minMax->min = datumCopy(value, attr->attbyval, attr->attlen);
			minMax->max = datumCopy(value, attr->attbyval, attr->attlen);
			minMax->minmax_valid = true;
			modified = true;
			continue;
		}

		/*
		 * If the entry is null, record that it's null, and continue
		 */
		if (value_isnull)
		{
			if (!minMax->any_nulls)
				modified = true;
			minMax->any_nulls = true;
			continue;
		}

		/*
		 * Otherwise, need to compare the new value with the existing boundaries
		 * and update them accordingly.  First check if it's less than the
		 * existing minimum.
		 */
		cmpFn = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												  BTLessStrategyNumber);
		compar = FunctionCall2Coll(cmpFn, colloid, value, minMax->min);
		if (DatumGetBool(compar))
		{
			if (!attr->attbyval)
				pfree(DatumGetPointer(minMax->min));
			minMax->min = datumCopy(value, attr->attbyval, attr->attlen);
			modified = true;
		}

		cmpFn = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												  BTGreaterStrategyNumber);
		compar = FunctionCall2Coll(cmpFn, colloid, value, minMax->max);
		if (DatumGetBool(compar))
		{
			if (!attr->attbyval)
				pfree(DatumGetPointer(minMax->max));
			minMax->max = datumCopy(value, attr->attbyval, attr->attlen);
			modified = true;
		}
	}
	array_free_iterator(array_iterator);

	PG_RETURN_BOOL(modified);
}

Datum
brin_aops_minmax_consistent(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *column = (BrinValues *) PG_GETARG_POINTER(1);
	ScanKey	   *keys = (ScanKey *) PG_GETARG_POINTER(2);
	int			nkeys = PG_GETARG_INT32(3);
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
	SerializedArrayMinMax *serialized;
	ArrayMinMax *minMax;
	FmgrInfo *finfo;

	serialized = (SerializedArrayMinMax *) PG_DETOAST_DATUM(column->bv_values[0]);
	minMax = baop_minmax_deserialize(serialized);

	/* This opclass uses the old signature with only three arguments. */
	Assert(PG_NARGS() == 3);
	/* Should not be dealing with all-NULL ranges. */
	Assert(!column->bv_allnulls);

	for (int i = 0; i < nkeys; i++)
	{
		ScanKey key = keys[i];

		attno = key->sk_attno;
		subtype = key->sk_subtype;

		array = DatumGetArrayTypeP(key->sk_argument);
		/* some fast exit strategies */
		switch (key->sk_strategy)
		{
			case AOPS_MINMAX_STRATEGY_CONTAINS:
				/* An empty array is considered to be @> any size of array */
				if (ARR_DIMS(array) == 0)
					continue;
				break;
			case AOPS_MINMAX_STRATEGY_CONTAINED:
				/*
				 * An empty array can only contain empty arrays.
				 */
				if (ARR_DIMS(array) == 0)
				{
					if (minMax->any_empty)
						continue;
					else
						PG_RETURN_BOOL(false);
				}
				break;
			case AOPS_MINMAX_STRATEGY_OVERLAP:
				/* zero-sized arrays don't overlap with anything */
				if (ARR_DIMS(array) == 0)
					PG_RETURN_BOOL(false);

				/* overlap requires some valid minmax values */
				if (!minMax->minmax_valid)
					PG_RETURN_BOOL(false);
				break;
			case AOPS_MINMAX_STRATEGY_LE:
				/*
				 * An empty array is equal to itself, and is smaller than any
				 * array with values.
				 */
				if (minMax->any_empty && ARR_NDIM(array) >= 0)
					continue;
				break;
			case AOPS_MINMAX_STRATEGY_LT:
				/* an empty array is smaller than any non-empty array */
				if (minMax->any_empty && ARR_NDIM(array) > 0)
					continue;
				break;
			case AOPS_MINMAX_STRATEGY_EQ:
				/* empty array = empty array */
				if (minMax->any_empty && ARR_NDIM(array) == 0)
					continue;
				break;
			case AOPS_MINMAX_STRATEGY_GE:
				/* empty array = empty array */
				if (minMax->any_empty && ARR_NDIM(array) == 0)
					continue;
				/* [null, null, ...] compares larger than any other array */
				if (minMax->any_nulls)
					continue;
				break;
			case AOPS_MINMAX_STRATEGY_GT:
				/* [null, null, ...] compares larger than any other array */
				if (minMax->any_nulls)
					continue;
				break;
			default:
				break;
		}

		/* There are only empty arrays, or arrays with only NULL values */
		if (!minMax->minmax_valid)
		{
			Assert(minMax->any_empty || minMax->any_nulls);
			switch (key->sk_strategy)
			{
				/*
				 * LT, LE, EQ:
				 * We've already checked for empty arrays above, and this is not
				 * all_nulls, so all arrays contain only null values, which is
				 * always larger.
				 */
				case AOPS_MINMAX_STRATEGY_LT:
				case AOPS_MINMAX_STRATEGY_LE:
				case AOPS_MINMAX_STRATEGY_EQ:
					PG_RETURN_BOOL(false);
					/*
					 * We've already checked for any_nulls above, and for GE's empty
					 * array = empty array, so we don't have any cases left where this
					 * condition can be satisfied.
					 */
				case AOPS_MINMAX_STRATEGY_GE:
				case AOPS_MINMAX_STRATEGY_GT:
					PG_RETURN_BOOL(false);
					/*
					 * overlaps, contains, contained
					 * No array that contains only nulls is contained by a non-empty
					 * array, nor do they overlap. We've already checked for empty arrays
					 * above, so this is a hard exit.
					 */
				case AOPS_MINMAX_STRATEGY_CONTAINS:
				case AOPS_MINMAX_STRATEGY_CONTAINED:
				case AOPS_MINMAX_STRATEGY_OVERLAP:
					PG_RETURN_BOOL(false);
				default:
					pg_unreachable();
			}
		}

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
			if (value_isnull)
			{
				switch (key->sk_strategy)
				{
				case AOPS_MINMAX_STRATEGY_CONTAINS:
				case AOPS_MINMAX_STRATEGY_CONTAINED:
				{
					/* Arrays with nulls do not have overlap with other arrays */
					array_free_iterator(array_iterator);
					PG_RETURN_BOOL(false);
				}
				case AOPS_MINMAX_STRATEGY_OVERLAP:
					continue; /* not a match here, but later ones might have one */
				case AOPS_MINMAX_STRATEGY_EQ:
					if (minMax->any_nulls)
						continue;
					else
					{
						array_free_iterator(array_iterator);
						PG_RETURN_BOOL(false);
					}
				}
				continue;
			}

#define CHECK(strat, left, right) do { \
						finfo = baop_minmax_get_strategy_procinfo(bdesc, attno, subtype, strat); \
						Assert(finfo != NULL); \
						matches = FunctionCall2Coll(finfo, colloid, left, right);		\
					} while(0)

			/*
			 * Scan array value isn't null. Check if the value matches the
			 * summary.
			 */
			switch (key->sk_strategy)
			{
			case AOPS_MINMAX_STRATEGY_LT:
			case AOPS_MINMAX_STRATEGY_LE:
				CHECK(BTGreaterStrategyNumber, value, minMax->min);
				if (matches)
					goto key_done;
				CHECK(BTGreaterEqualStrategyNumber, value, minMax->min);
				if (!matches)
					goto key_done;
				break;
			case AOPS_MINMAX_STRATEGY_EQ:
				CHECK(BTLessEqualStrategyNumber, value, minMax->max);
				if (!matches)
					goto key_done;
				CHECK(BTGreaterEqualStrategyNumber, value, minMax->min);
				if (!matches)
					goto key_done;
				break;
			case AOPS_MINMAX_STRATEGY_GE:
			case AOPS_MINMAX_STRATEGY_GT:
				CHECK(BTLessStrategyNumber, value, minMax->max);
				if (matches)
					goto key_done;
				CHECK(BTLessEqualStrategyNumber, value, minMax->max);
				if (!matches)
					goto key_done;
				break;
			case AOPS_MINMAX_STRATEGY_CONTAINS:
				CHECK(BTLessEqualStrategyNumber, value, minMax->min);
				if (!matches)
					goto key_done;
				CHECK(BTGreaterEqualStrategyNumber, value, minMax->max);
				if (!matches)
					goto key_done;
				break;
			case AOPS_MINMAX_STRATEGY_CONTAINED:
			case AOPS_MINMAX_STRATEGY_OVERLAP:
				CHECK(BTLessEqualStrategyNumber, value, minMax->min);
				if (matches)
				{
					CHECK(BTGreaterEqualStrategyNumber, value, minMax->max);
					if (matches)
						goto key_done;
				}
				break;
			}
		}
		matches = true;

key_done:
		array_free_iterator(array_iterator);

		if (matches)
			continue;
		else
			break;
	}

	PG_RETURN_BOOL(matches);
}

Datum
brin_aops_minmax_union(PG_FUNCTION_ARGS)
{
	BrinDesc   *bdesc = (BrinDesc *) PG_GETARG_POINTER(0);
	BrinValues *col_a = (BrinValues *) PG_GETARG_POINTER(1);
	BrinValues *col_b = (BrinValues *) PG_GETARG_POINTER(2);
	Oid			colloid = PG_GET_COLLATION();
	AttrNumber	attno;
	Form_pg_attribute attr;
	FmgrInfo   *finfo;
	bool		needsadj;
	ArrayMinMax *minMaxA;
	ArrayMinMax *minMaxB;

	Assert(col_a->bv_attno == col_b->bv_attno);
	Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	minMaxA = (ArrayMinMax *) DatumGetPointer(col_a->bv_mem_value);
	minMaxB = (ArrayMinMax *) DatumGetPointer(col_b->bv_mem_value);

	/* Adjust minimum, if B's min is less than A's min */
	finfo = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											  BTLessStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid,
								 minMaxA->min,
								 minMaxB->min);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(minMaxA->min));
		minMaxA->min = datumCopy(minMaxB->min,
								 attr->attbyval, attr->attlen);
	}

	/* Adjust maximum, if B's max is greater than A's max */
	finfo = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
											  BTGreaterStrategyNumber);
	needsadj = FunctionCall2Coll(finfo, colloid, minMaxB->max,
								 minMaxA->max);
	if (needsadj)
	{
		if (!attr->attbyval)
			pfree(DatumGetPointer(minMaxA->max));
		minMaxA->max = datumCopy(minMaxB->max,
								 attr->attbyval, attr->attlen);
	}

	if (minMaxA->any_nulls != minMaxB->any_nulls)
		minMaxA->any_nulls = true;

	if (minMaxA->any_empty != minMaxB->any_empty)
		minMaxA->any_empty = true;

	PG_RETURN_VOID();
}

/*
 * Cache and return the procedure for the given strategy.
 *
 * Note: this function mirrors inclusion_get_strategy_procinfo; see notes
 * there.  If changes are made here, see that function too.
 */
static FmgrInfo *
baop_minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno, Oid subtype,
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

		attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

		opfamily = bdesc->bd_index->rd_opfamily[attno - 1];
		tuple = SearchSysCache4(AMOPSTRATEGY,
								ObjectIdGetDatum(opfamily),
								ObjectIdGetDatum(attr->atttypid),
								ObjectIdGetDatum(attr->atttypid),
								Int16GetDatum(strategynum));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
				 strategynum, attr->atttypid, attr->atttypid, opfamily);

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

static ArrayMinMax *baop_minmax_init()
{
	return palloc0(sizeof(ArrayMinMax));
}

static ArrayMinMax *
baop_minmax_deserialize(SerializedArrayMinMax *serialized)
{
	ArrayMinMax *minmax;

	minmax = baop_minmax_init();
	minmax->any_empty = ((serialized->flags & AOPS_MINMAX_HAS_EMPTY) != 0);
	minmax->any_nulls = ((serialized->flags & AOPS_MINMAX_HAS_NULLS) != 0);
	minmax->typid = serialized->typid;

	if (serialized->flags & AOPS_MINMAX_VALID_MINMAX)
	{
		char   *ptr = serialized->data;
		int		typbyval = get_typbyval(serialized->typid);
		int		typlen = get_typlen(serialized->typid);

		if (typbyval)
		{
			Datum		v = 0;
			memcpy(&v, ptr, typlen);
			minmax->min = fetch_att(&v, true, typlen);
			memcpy(&v, ptr + typlen, typlen);
			minmax->max = fetch_att(&v, true, typlen);
		}
		else
		{
			minmax->min = datumCopy(PointerGetDatum(ptr), false, typlen);
			minmax->max = datumCopy(PointerGetDatum(ptr + att_addlength_pointer(0, typlen, ptr)),
									false, typlen);
		}
		minmax->minmax_valid = true;
	}
	else
	{
		minmax->max = (Datum) NULL;
		minmax->min = (Datum) NULL;
		minmax->minmax_valid = false;
	}

	return minmax;
}

static SerializedArrayMinMax *
baop_minmax_serialize(ArrayMinMax *minMax)
{
	Size		len = offsetof(SerializedArrayMinMax, data);
	bool		typbyval = get_typbyval(minMax->typid);
	int			typlen = get_typlen(minMax->typid);
	SerializedArrayMinMax *serialized;

	if (minMax->minmax_valid)
	{
		if (typlen == -1)
		{
			len += VARSIZE_ANY(minMax->min);
			len += VARSIZE_ANY(minMax->max);
		}
		else if (typlen == -2)
		{
			len += strlen(DatumGetCString(minMax->min)) + 1;
			len += strlen(DatumGetCString(minMax->max)) + 1;
		}
		else
		{
			Assert(typlen > 0);
			len += typlen * 2;
		}
	}

	serialized = palloc0(len);
	SET_VARSIZE(serialized, len);

	serialized->typid = minMax->typid;
	serialized->flags |= minMax->any_nulls ? AOPS_MINMAX_HAS_NULLS : 0;
	serialized->flags |= minMax->any_empty ? AOPS_MINMAX_HAS_EMPTY : 0;

	if (minMax->minmax_valid)
	{
		char	   *ptr = serialized->data;
		serialized->flags |= AOPS_MINMAX_VALID_MINMAX;

		if (typbyval)
		{
			Datum	tmp;
			store_att_byval(&tmp, minMax->min, typlen);
			memcpy(ptr, &tmp, typlen);
			ptr += typlen;
			store_att_byval(&tmp, minMax->max, typlen);
			memcpy(ptr, &tmp, typlen);
			ptr += typlen;
		}
		else if (typlen > 0)
		{
			memcpy(ptr, DatumGetPointer(minMax->min), typlen);
			ptr += typlen;
			memcpy(ptr, DatumGetPointer(minMax->max), typlen);
			ptr += typlen;
		}
		else if (typlen == -1)
		{
			int		tmp = VARSIZE_ANY(DatumGetPointer(minMax->min));
			memcpy(ptr, DatumGetPointer(minMax->min), tmp);
			ptr += tmp;
			tmp = VARSIZE_ANY(DatumGetPointer(minMax->max));
			memcpy(ptr, DatumGetPointer(minMax->max), tmp);
			ptr += tmp;
		}
		else if (typlen == -2)
		{
			int		tmp = strlen(DatumGetCString(minMax->min)) + 1;
			memcpy(ptr, DatumGetCString(minMax->min), tmp);
			ptr += tmp;
			tmp = strlen(DatumGetCString(minMax->max)) + 1;
			memcpy(ptr, DatumGetCString(minMax->max), tmp);
			ptr += tmp;
		}
		else
		{
			Assert(false);
		}
	}
	Assert(ptr == ((char *) serialized) + len);

	return serialized;
}


Datum
brin_aops_minmax_storage_in(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot accept a value of type %s", "brin_aops_minmax_storage")));
}

Datum
brin_aops_minmax_storage_out(PG_FUNCTION_ARGS)
{
	StringInfoData str;
	SerializedArrayMinMax *serialized;

	serialized = (SerializedArrayMinMax *) PG_DETOAST_DATUM_PACKED(PG_GETARG_DATUM(0));

	initStringInfo(&str);
	appendStringInfoChar(&str, '{');

	appendStringInfo(&str, "typoid: %u  flags: %x",
					 serialized->typid, serialized->flags);
	appendStringInfoChar(&str, '}');

	PG_RETURN_CSTRING(str.data);
}

Datum
brin_aops_minmax_storage_send(PG_FUNCTION_ARGS)
{
	return byteasend(fcinfo);
}

Datum
brin_aops_minmax_storage_recv(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("cannot accept a value of type %s", "brin_aops_minmax_storage")));
}
