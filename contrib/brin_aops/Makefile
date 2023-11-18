# contrib/brin_aops/Makefile

MODULE_big = brin_aops
OBJS = \
	$(WIN32RES) \
	brin_aops.o \
	brin_minmax_aops.o

EXTENSION = brin_aops
DATA = brin_aops--1.0.sql
PGFILEDESC = "brin_aops - Array operators for BRIN"

REGRESS = install_brin_aops brin_aops

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/brin_aops
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
