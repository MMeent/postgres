/*-------------------------------------------------------------------------
 *
 * nbtree_specialize.h
 *	  header file for postgres btree access method implementation.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/nbtree_specialize.h
 *
 *-------------------------------------------------------------------------
 *
 * Specialize key-accessing functions and the hot code around those. The file
 * that's marked for specialization is either
 * - NBTS_HEADER, a quoted header inclusion path; or
 * - NBTS_FILE, the include path of the to-be-specialized code relative to
 *		the access/nbtree_spec.h file.
 *
 * Specialized files define their functions like usual, with an additional
 * requirement that they #define their function name at the top, with
 * #define funcname NBTS_FUNCTION(funcname).
 */

#if defined(NBT_FILE)
#define NBT_SPECIALIZE_FILE NBT_FILE
#else
#error "No specializable file defined"
#endif

/* how do we make names? */
#define NBTS_MAKE_PREFIX(a) CppConcat(a,_)
#define NBTS_MAKE_NAME_(a,b) CppConcat(a,b)
#define NBTS_MAKE_NAME(a,b) NBTS_MAKE_NAME_(NBTS_MAKE_PREFIX(a),b)


/*
 * Protections against multiple inclusions - the definition of this macro is
 * different for files included by this file as template, versus those that
 * include this file for the definitions, so we redefine these macros at top and
 * bottom.
 */
#ifdef NBTS_FUNCTION
#undef NBTS_FUNCTION
#endif
#define NBTS_FUNCTION(name) NBTS_MAKE_NAME(name, NBTS_TYPE)

/* In a specialized file, specialization contexts are meaningless */
#ifdef nbts_prep_ctx
#undef nbts_prep_ctx
#endif

/*
 * Specialization 1: CACHED
 *
 * Multiple key columns, optimized access for attcacheoff -cacheable offsets.
 */
#define NBTS_TYPE NBTS_TYPE_CACHED

#include NBT_SPECIALIZE_FILE

#undef NBTS_TYPE

/*
 * We're done with templating, so restore or create the macros which can be
 * used in non-template sources.
 */
#define nbts_prep_ctx(rel) NBTS_MAKE_CTX(rel)

/*
 * from here on all NBTS_FUNCTIONs are from specialized function names that
 * are being called. Change the result of those macros from a direct call
 * call to a conditional call to the right place, depending on the correct
 * context.
 */
#undef NBTS_FUNCTION
#define NBTS_FUNCTION(name) NBTS_SPECIALIZE_NAME(name)

/* cleanup unwanted definitions from file inclusion */
#undef NBT_SPECIALIZE_FILE
#ifdef NBT_HEADERS
#undef NBT_HEADERS
#endif
#ifdef NBT_FILE
#undef NBT_FILE
#endif
