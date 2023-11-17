#include "postgres.h"


#include "catalog/pg_type_d.h"
#include "commands/extension.h"
#include "fmgr.h"
#include "utils/catcache.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "brin_aops.h"

brin_aops_constants *BRIN_AOPS_CONSTANTS = NULL;

static brin_aops_constants *baop_install_constants();


static Oid
TypenameNspGetTypid(const char *typname, Oid nsp_oid)
{
	return GetSysCacheOid2(TYPENAMENSP,
						   Anum_pg_type_oid,
						   PointerGetDatum(typname),
						   ObjectIdGetDatum(nsp_oid));
}

void
baop_initialize_cache()
{
	if (!BRIN_AOPS_CONSTANTS)
		BRIN_AOPS_CONSTANTS = baop_install_constants();
	Assert(PointerIsValid(BRIN_AOPS_CONSTANTS));
}

/* Inspiration: getPostgisConstants() in postgis/lwgeom */
static brin_aops_constants *
baop_install_constants()
{
	Oid nsp_oid;
	Oid ext_oid;
	brin_aops_constants *constants;

	ext_oid = get_extension_oid("brin_aops", false);

	if (ext_oid != InvalidOid)
		nsp_oid = get_extension_schema(ext_oid);
	else
		return NULL;

	constants = MemoryContextAlloc(CacheMemoryContext, sizeof(brin_aops_constants));

	constants->minmax_storage_oid =
		TypenameNspGetTypid("brin_aops_minmax_storage", nsp_oid);
	constants->bloom_storage_oid =
		TypenameNspGetTypid("brin_aops_bloom_storage", nsp_oid);

	constants->valid = true;

	return constants;
}