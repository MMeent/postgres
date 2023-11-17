# Copyright (c) 2023, PostgreSQL Global Development Group

brin_aops_sources = files(
  'brin_aops.c',
  # 'brin_bloom_aops.c',
  'brin_minmax_aops.c',
)

if host_system == 'windows'
  brin_aops_sources += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'brin_aops',
    '--FILEDESC', 'brin_aops - Array operator classes for BRIN',])
endif

brin_aops = shared_module('brin_aops',
  brin_aops_sources,
  kwargs: contrib_mod_args,
)
contrib_targets += brin_aops

install_data(
  'brin_aops.control',
  'brin_aops--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'brin_aops',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'install_brin_aops',
    ],
  },
}
