#ifndef _BRIN_AOPS_H_
#define _BRIN_AOPS_H_

#define AOPS_MINMAX_STRATEGY_LT			1
#define AOPS_MINMAX_STRATEGY_LE			2
#define AOPS_MINMAX_STRATEGY_EQ			3
#define AOPS_MINMAX_STRATEGY_GE			4
#define AOPS_MINMAX_STRATEGY_GT			5
#define AOPS_MINMAX_STRATEGY_CONTAINS	6
#define AOPS_MINMAX_STRATEGY_CONTAINED	7
#define AOPS_MINMAX_STRATEGY_OVERLAP	8

typedef struct brin_aops_constants {
	bool		valid;
	Oid			install_nsp_oid;
	Oid			minmax_storage_oid;
	Oid			bloom_storage_oid;
} brin_aops_constants;

extern brin_aops_constants *BRIN_AOPS_CONSTANTS;
extern void baop_initialize_cache(void);

#endif /* _BRIN_AOPS_H_ */
