# Copyright (c) 2022-2024, PostgreSQL Global Development Group

backend_sources += files(
  'bitmapset.c',
  'extensible.c',
  'list.c',
  'makefuncs.c',
  'multibitmapset.c',
  'ndio_binary.c',
  'ndio_json.c',
  'ndio_text.c',
  'nodeFuncs.c',
  'params.c',
  'print.c',
  'read.c',
  'tidbitmap.c',
  'value.c',
)

# these include .c files generated in ../../include/nodes, seems nicer to not
# add that as an include path for the whole backend
nodefunc_sources = files(
  'copyfuncs.c',
  'equalfuncs.c',
  'ndio.c',
  'nodedesc.c',
  'queryjumblefuncs.c',
  'outfuncs.c',
  'readfuncs.c',
)
nodefuncs = static_library('nodefuncs',
  nodefunc_sources,
  dependencies: [backend_code],
  include_directories: include_directories('../../include/nodes'),
  kwargs: internal_lib_args,
)
backend_link_with += nodefuncs
