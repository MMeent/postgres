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

typedef struct ArrayMinmaxOpaque
{
	Oid			cached_subtype;
	FmgrInfo	strategy_procinfos[BTMaxStrategyNumber];
} ArrayMinmaxOpaque;

static FmgrInfo *baop_minmax_get_strategy_procinfo(BrinDesc *bdesc, uint16 attno,
												   Oid subtype, uint16 strategynum);

Datum
brin_aops_minmax_opcinfo(PG_FUNCTION_ARGS) {
	Oid			typoid = PG_GETARG_OID(0);
	BrinOpcInfo *result;

	/*
	 * opaque->strategy_procinfos is initialized lazily; here it is set to
	 * all-uninitialized by palloc0 which sets fn_oid to InvalidOid.
	 */

	result = palloc0(MAXALIGN(SizeofBrinOpcInfo(BAOP_MM_NATTS)) +
					 sizeof(ArrayMinmaxOpaque));
	result->oi_nstored = 3;
	result->oi_regular_nulls = true;
	result->oi_opaque = (ArrayMinmaxOpaque *)
		MAXALIGN((char *) result + SizeofBrinOpcInfo(BAOP_MM_NATTS));
	result->oi_typcache[BAOP_MM_ATT_MIN] =
		result->oi_typcache[BAOP_MM_ATT_MAX] =
		lookup_type_cache(typoid, 0);
	/*
	 * Apart from min/max, we also store whether the arrays contained
	 * any nulls, and whether there were any empty arrays.
	 */
	result->oi_typcache[BAOP_MM_ATT_ANY_NULLS] =
		result->oi_typcache[BAOP_MM_ATT_ANY_EMPTY] =
		lookup_type_cache(BOOLOID, 0);
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
brin_aops_minmax_add_value(PG_FUNCTION_ARGS)
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

	/* prepare minmax operations */
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
		cmpFn = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
												  BTLessStrategyNumber);
		compar = FunctionCall2Coll(cmpFn, colloid, value, column->bv_values[0]);
		if (DatumGetBool(compar))
		{
			if (!attr->attbyval)
				pfree(DatumGetPointer(column->bv_values[0]));
			column->bv_values[0] = datumCopy(value, attr->attbyval, attr->attlen);
			updated = true;
		}

		cmpFn = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
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
brin_aops_minmax_consistent(PG_FUNCTION_ARGS)
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

	}

	PG_RETURN_DATUM(matches);
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

	Assert(col_a->bv_attno == col_b->bv_attno);
	Assert(!col_a->bv_allnulls && !col_b->bv_allnulls);

	attno = col_a->bv_attno;
	attr = TupleDescAttr(bdesc->bd_tupdesc, attno - 1);

	/* Adjust minimum, if B's min is less than A's min */
	finfo = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
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
	finfo = baop_minmax_get_strategy_procinfo(bdesc, attno, attr->atttypid,
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
