-- Tests for Bitmapsets
CREATE EXTENSION test_bitmapset;

-- bms_make_singleton()
SELECT test_bms_make_singleton(-1);
SELECT test_bms_make_singleton(42) AS result;
SELECT test_bms_make_singleton(0) AS result;
SELECT test_bms_make_singleton(1000) AS result;
-- Test module check
SELECT test_bms_make_singleton(NULL) AS result;

-- bms_add_member()
SELECT test_bms_add_member('(b 1)', -1); -- error
SELECT test_bms_add_member('(b)', -10); -- error
SELECT test_bms_add_member('(b)', 10) AS result;
SELECT test_bms_add_member('(b 5)', 10) AS result;
-- sort check
SELECT test_bms_add_member('(b 10)', 5) AS result;
-- idempotent change
SELECT test_bms_add_member('(b 10)', 10) AS result;
-- Test module check
SELECT test_bms_add_member('(b)', NULL) AS result;

-- bms_replace_members()
SELECT test_bms_replace_members(NULL, '(b 3 2 1)') AS result;
SELECT test_bms_replace_members('(b 3 2 1)', NULL) AS result;
SELECT test_bms_replace_members('(b 3 2 1)', '(b 6 5 3)') AS result;
SELECT test_bms_replace_members('(b 3 2 1)', '(b 5 3)') AS result;
SELECT test_bms_replace_members('(b 2 1)', '(b 7 5 3)') AS result;
-- Force repalloc() with larger set
SELECT test_bms_replace_members('(b 5 4 3 2 1)', '(b 600 500)') AS result;

-- bms_del_member()
SELECT test_bms_del_member('(b)', -20); -- error
SELECT test_bms_del_member('(b)', 10) AS result;
SELECT test_bms_del_member('(b 10)', 10) AS result;
SELECT test_bms_del_member('(b 10)', 5) AS result;
SELECT test_bms_del_member('(b 3 2 1)', 2) AS result;
-- Reallocation check
SELECT test_bms_del_member(test_bms_del_member('(b 64 63 32 31 0)', 32), 63) AS result;
-- Word boundary
SELECT test_bms_del_member(test_bms_add_range('(b)', 30, 34), 32) AS result;
-- Force word count changes
SELECT test_bms_del_member('(b 200 1)', 200) AS result;
SELECT test_bms_del_member('(b 200 100 50 1)', 200) AS result;
SELECT test_bms_del_member('(b 200 100 50 1)', 100) AS result;
-- Test module checks
SELECT test_bms_del_member('(b 5)', NULL) AS result;

-- bms_del_members()
SELECT test_bms_del_members('(b)', '(b 10)') AS result;
SELECT test_bms_del_members('(b 10)', '(b 10)') AS result;
SELECT test_bms_del_members('(b 10)', '(b 5)') AS result;
SELECT test_bms_del_members('(b 3 2 1)', '(b 2)') AS result;
SELECT test_bms_del_members('(b 100 5)', '(b 100)') AS result;
SELECT test_bms_del_members('(b 200 100 5)', '(b 200)') AS result;
-- Force word count changes
SELECT test_bms_del_members('(b 300 200 100 2 1)', '(b 2 1)') AS result;
SELECT test_bms_del_members('(b 300 200 100 2 1)', '(b 300 200)') AS result;
-- NULL inputs
SELECT test_bms_del_members('(b 5)', NULL) AS result;
SELECT test_bms_del_members(NULL, '(b 5)') AS result;

-- bms_join()
SELECT test_bms_join('(b 5 3 1)', NULL) AS result;
SELECT test_bms_join(NULL, '(b 6 4 2)') AS result;
SELECT test_bms_join('(b 5 3 1)', '(b 6 4 2)') AS result;
SELECT test_bms_join('(b 5 3 1)', '(b 5 4 1)') AS result;
-- Force word count changes
SELECT test_bms_join('(b 5)', '(b 100)') AS result;
SELECT test_bms_join('(b 2 1)', '(b 300 200 100)') AS result;
-- NULL inputs
SELECT test_bms_join('(b 5)', NULL) AS result;
SELECT test_bms_join(NULL, '(b 5)') AS result;
SELECT test_bms_join(NULL, NULL) AS result;

-- bms_union()
-- Overlapping sets
SELECT test_bms_union('(b 5 3 1)', '(b 7 5 3)') AS result;
-- Union with NULL
SELECT test_bms_union('(b 5 3 1)', '(b)') AS result;
-- Union of empty with empty
SELECT test_bms_union('(b)', '(b)') AS result;
-- Overlapping ranges
SELECT test_bms_union(
         test_bms_add_range('(b)', 0, 15),
         test_bms_add_range('(b)', 10, 20)
       ) AS result;
-- Union with varying word counts
SELECT test_bms_union('(b 2 1)', '(b 300 100)') AS result;
SELECT test_bms_union('(b 300 100)', '(b 2 1)') AS result;
-- NULL inputs
SELECT test_bms_union('(b 5)', NULL) AS result;
SELECT test_bms_union(NULL, '(b 5)') AS result;
SELECT test_bms_union(NULL, NULL) AS result;

-- bms_intersect()
-- Overlapping sets
SELECT test_bms_intersect('(b 5 3 1)', '(b 7 5 3)') AS result;
-- Disjoint sets
SELECT test_bms_intersect('(b 5 3 1)', '(b 6 4 2)') AS result;
-- Intersect with empty
SELECT test_bms_intersect('(b 5 3 1)', '(b)') AS result;
-- Intersect with varying word counts
SELECT test_bms_intersect('(b 300 1)', '(b 5 4 3 2 1)') AS result;
SELECT test_bms_intersect('(b 5 4 3 2 1)', '(b 300 1)') AS result;
-- NULL inputs
SELECT test_bms_intersect('(b 5)', NULL) AS result;
SELECT test_bms_intersect(NULL, '(b 5)') AS result;
SELECT test_bms_intersect(NULL, NULL) AS result;

-- bms_int_members()
-- Overlapping sets
SELECT test_bms_int_members('(b 5 3 1)', '(b 7 5 3)') AS result;
-- Disjoint sets
SELECT test_bms_int_members('(b 5 3 1)', '(b 6 4 2)') AS result;
-- Intersect with empty
SELECT test_bms_int_members('(b 5 3 1)', '(b)') AS result;
-- Multiple members
SELECT test_bms_int_members('(b 64 63 32 31 0)', '(b 65 64 32 31)') AS result;
-- NULL inputs
SELECT test_bms_int_members('(b 5)', NULL) AS result;
SELECT test_bms_int_members(NULL, '(b 5)') AS result;
SELECT test_bms_int_members(NULL, NULL) AS result;

-- bms_difference()
-- Overlapping sets
SELECT test_bms_difference('(b 5 3 1)', '(b 7 5 3)') AS result;
-- Disjoint sets
SELECT test_bms_difference('(b 5 3 1)', '(b 6 4 2)') AS result;
-- Identical sets
SELECT test_bms_difference('(b 5 3 1)', '(b 5 3 1)') AS result;
-- Subtraction to empty
SELECT test_bms_difference('(b 42)', '(b 42)') AS result;
-- Subtraction edge case
SELECT test_bms_difference(
         test_bms_add_range('(b)', 0, 100),
         test_bms_add_range('(b)', 50, 150)
       ) AS result;
-- Difference with different word counts
SELECT test_bms_difference('(b 100 5)', '(b 5)') AS result;
SELECT test_bms_difference('(b 200 100 2 1)', '(b 2 1)') AS result;
-- NULL inputs
SELECT test_bms_difference('(b 5)', NULL) AS result;
SELECT test_bms_difference(NULL, '(b 5)') AS result;
SELECT test_bms_difference(NULL, NULL) AS result;

-- bms_offset_members()
-- Ensure overflow detection works
SELECT test_bms_offset_members('(b 1)', 2147483647);
SELECT test_bms_offset_members('(b 2)', 2147483646);

-- Ensure members are all offset as expected
SELECT test_bms_offset_members('(b 5 3 1)', 1);
SELECT test_bms_offset_members('(b 5 3 1)', 0);
SELECT test_bms_offset_members('(b 5 3 1)', -1);
SELECT test_bms_offset_members('(b 64 63 32 31)', 1);
SELECT test_bms_offset_members('(b 64 63 32 31)', 0);
SELECT test_bms_offset_members('(b 64 63 32 31)', -1);
SELECT test_bms_offset_members('(b 3 2 1)', 64);
SELECT test_bms_offset_members('(b 67 66 65)', -64);

-- Ensure members going below zero silently fall off the set
SELECT test_bms_offset_members('(b 10 2 1 0)', -2);
SELECT test_bms_offset_members('(b 3 2 1)', -10);
SELECT test_bms_offset_members('(b 0)', -2147483648);

-- bms_is_member()
SELECT test_bms_is_member('(b)', -5); -- error
SELECT test_bms_is_member('(b 5 3 1)', 1) AS result;
SELECT test_bms_is_member('(b 5 3 1)', 2) AS result;
SELECT test_bms_is_member('(b 5 3 1)', 3) AS result;
SELECT test_bms_is_member('(b)', 1) AS result;
-- Test module check
SELECT test_bms_is_member('(b 5)', NULL) AS result;

-- bms_member_index()
SELECT test_bms_member_index(NULL, 1) AS result;
SELECT test_bms_member_index('(b 5 3 1)', 2) AS result;
SELECT test_bms_member_index('(b 5 3 1)', 1) AS result;
SELECT test_bms_member_index('(b 5 3 1)', 3) AS result;
-- Member index with various word positions
SELECT test_bms_member_index('(b 200 100)', 100) AS result;
SELECT test_bms_member_index('(b 200 100)', 200) AS result;
SELECT test_bms_member_index('(b 200 100 50 1)', 200) AS result;
-- Test module check
SELECT test_bms_member_index('(b 5 3 1)', NULL) AS result;

-- bms_num_members()
SELECT test_bms_num_members('(b)') AS result;
SELECT test_bms_num_members('(b 5 3 1)') AS result;
SELECT test_bms_num_members('(b 10 8 6 4 2)') AS result;

-- test_bms_equal()
SELECT test_bms_equal('(b)', '(b)') AS result;
SELECT test_bms_equal('(b)', '(b 5 3 1)') AS result;
SELECT test_bms_equal('(b 5 3 1)', '(b)') AS result;
SELECT test_bms_equal('(b 5 3 1)', '(b 5 3 1)') AS result;
SELECT test_bms_equal('(b 5 3 1)', '(b 6 4 2)') AS result;
-- Equal with different word counts
SELECT test_bms_equal('(b 5)', '(b 100)') AS result;
SELECT test_bms_equal('(b 10 5)', '(b 300 200 100)') AS result;
-- NULL inputs
SELECT test_bms_equal('(b 5)', NULL) AS result;
SELECT test_bms_equal(NULL, '(b 5)') AS result;
SELECT test_bms_equal(NULL, NULL) AS result;

-- bms_compare()
SELECT test_bms_compare('(b)', '(b)') AS result;
SELECT test_bms_compare('(b)', '(b 3 1)') AS result;
SELECT test_bms_compare('(b 3 1)', '(b)') AS result;
SELECT test_bms_compare('(b 3 1)', '(b 3 1)') AS result;
SELECT test_bms_compare('(b 3 1)', '(b 5 3 1)') AS result;
SELECT test_bms_compare('(b 5 3 1)', '(b 3 1)') AS result;
SELECT test_bms_compare(
         test_bms_add_range('(b)', 0, 63),
         test_bms_add_range('(b)', 0, 64)
       ) AS result;
-- NULL inputs
SELECT test_bms_compare('(b 5)', NULL) AS result;
SELECT test_bms_compare(NULL, '(b 5)') AS result;
SELECT test_bms_compare(NULL, NULL) AS result;

-- bms_add_range()
SELECT test_bms_add_range('(b)', -5, 10); -- error
SELECT test_bms_add_range('(b)', 5, 7) AS result;
SELECT test_bms_add_range('(b)', 5, 5) AS result;
SELECT test_bms_add_range('(b 10 1)', 5, 7) AS result;
-- Word boundary of 31
SELECT test_bms_add_range('(b)', 30, 34) AS result;
-- Word boundary of 63
SELECT test_bms_add_range('(b)', 62, 66) AS result;
-- Large range
SELECT length(test_bms_add_range('(b)', 0, 1000)) AS result;
-- Force reallocations
SELECT length(test_bms_add_range('(b)', 0, 200)) AS result;
SELECT length(test_bms_add_range('(b)', 1000, 1100)) AS result;
-- Force word count expansion
SELECT test_bms_add_range('(b 5)', 100, 105) AS result;
SELECT length(test_bms_add_range('(b 2 1)', 200, 250)) AS result;
-- Test module checks
SELECT test_bms_add_range('(b 5)', 5, NULL) AS result;
SELECT test_bms_add_range('(b 5)', NULL, 10) AS result;
SELECT test_bms_add_range('(b 5)', NULL, NULL) AS result;
-- NULL inputs
SELECT test_bms_add_range(NULL, 5, 10) AS result;
SELECT test_bms_add_range(NULL, 10, 5) AS result;

-- bms_membership()
SELECT test_bms_membership('(b)') AS result;
SELECT test_bms_membership('(b 42)') AS result;
SELECT test_bms_membership('(b 2 1)') AS result;
-- NULL input
SELECT test_bms_membership(NULL) AS result;

-- bms_is_empty()
SELECT test_bms_is_empty(NULL) AS result;
SELECT test_bms_is_empty('(b)') AS result;
SELECT test_bms_is_empty('(b 1)') AS result;

-- bms_singleton_member()
SELECT test_bms_singleton_member('(b)'); -- error
SELECT test_bms_singleton_member('(b 2 1)'); -- error
SELECT test_bms_singleton_member('(b 42)') AS result;
-- NULL input
SELECT test_bms_singleton_member(NULL) AS result;

-- bms_get_singleton_member()
SELECT test_bms_get_singleton_member('(b)');
-- Try with an empty set
SELECT test_bms_get_singleton_member(NULL) AS result;
-- Not a singleton, returns input default
SELECT test_bms_get_singleton_member('(b 6 3)') AS result;
-- Singletone, returns sole member
SELECT test_bms_get_singleton_member('(b 400)') AS result;

-- bms_next_member() and bms_prev_member()
-- First member
SELECT test_bms_next_member('(b 20 15 10 5)', -1) AS result;
-- Second member
SELECT test_bms_next_member('(b 20 15 10 5)', 5) AS result;
-- Member past the end
SELECT test_bms_next_member('(b 20 15 10 5)', 20) AS result;
-- Empty set
SELECT test_bms_next_member('(b)', -1) AS result;
-- Last member
SELECT test_bms_prev_member('(b 20 15 10 5)', 21) AS result;
-- Penultimate member
SELECT test_bms_prev_member('(b 20 15 10 5)', 20) AS result;
-- Past beginning member
SELECT test_bms_prev_member('(b 20 15 10 5)', 5) AS result;
-- Empty set
SELECT test_bms_prev_member('(b)', 100) AS result;
-- Negative prevbit should result in highest possible bit in set
SELECT test_bms_prev_member('(b 127 64 63 0)', -1) AS result;
-- NULL inputs
SELECT test_bms_next_member(NULL, 5) AS result;
SELECT test_bms_prev_member(NULL, 5) AS result;
-- Test module check
SELECT test_bms_next_member('(b 10 5)', NULL) AS result;
SELECT test_bms_prev_member('(b 10 5)', NULL) AS result;

-- bms_hash_value()
SELECT test_bms_hash_value('(b)') = 0 AS result;
SELECT test_bms_hash_value('(b 5 3 1)') = test_bms_hash_value('(b 5 3 1)') AS result;
SELECT test_bms_hash_value('(b 5 3 1)') != test_bms_hash_value('(b 6 4 2)') AS result;
-- NULL input
SELECT test_bms_hash_value(NULL) AS result;

-- bms_overlap()
SELECT test_bms_overlap('(b 5 3 1)', '(b 7 5 3)') AS result;
SELECT test_bms_overlap('(b 5 3 1)', '(b 6 4 2)') AS result;
SELECT test_bms_overlap('(b)', '(b 5 3 1)') AS result;
-- NULL inputs
SELECT test_bms_overlap('(b 5)', NULL) AS result;
SELECT test_bms_overlap(NULL, '(b 5)') AS result;
SELECT test_bms_overlap(NULL, NULL) AS result;

-- bms_is_subset()
SELECT test_bms_is_subset('(b)', '(b 5 3 1)') AS result;
SELECT test_bms_is_subset('(b 3 1)', '(b 5 3 1)') AS result;
SELECT test_bms_is_subset('(b 5 3 1)', '(b 3 1)') AS result;
SELECT test_bms_is_subset('(b 3 1)', '(b 4 2)') AS result;
SELECT test_bms_is_subset(test_bms_add_range(NULL, 0, 31),
                          test_bms_add_range(NULL, 0, 63)) AS result;
-- Is subset with shorter word counts?
SELECT test_bms_is_subset('(b 100 5)', '(b 5)') AS result;
SELECT test_bms_is_subset('(b 100 50 2 1)', '(b 2 1)') AS result;
-- NULL inputs
SELECT test_bms_is_subset('(b 5)', NULL) AS result;
SELECT test_bms_is_subset(NULL, '(b 5)') AS result;
SELECT test_bms_is_subset(NULL, NULL) AS result;

-- bms_subset_compare()
SELECT test_bms_subset_compare(NULL, NULL) AS result;
SELECT test_bms_subset_compare(NULL, '(b 3 1)') AS result;
SELECT test_bms_subset_compare('(b)', '(b)') AS result;
SELECT test_bms_subset_compare('(b)', '(b 1)') AS result;
SELECT test_bms_subset_compare('(b 1)', '(b)') AS result;
SELECT test_bms_subset_compare('(b 3 1)', NULL) AS result;
SELECT test_bms_subset_compare('(b 5 3 1)', '(b 5 3 1)') AS result;
SELECT test_bms_subset_compare('(b 3 1)', '(b 5 3 1)') AS result;
SELECT test_bms_subset_compare('(b 5 3 1)', '(b 3 1)') AS result;
SELECT test_bms_subset_compare('(b 2 1)', '(b 3 1)') AS result;
SELECT test_bms_subset_compare('(b 2 1)', '(b 4 1)') AS result;
SELECT test_bms_subset_compare('(b 3 1)', '(b 64 3 1)') AS result;
SELECT test_bms_subset_compare('(b 64 3 1)', '(b 3 1)') AS result;
SELECT test_bms_subset_compare('(b 64 3 1)', '(b 65 3 1)') AS result;
SELECT test_bms_subset_compare('(b 3 1)', '(b 4 2)') AS result;
SELECT test_bms_subset_compare('(b 1)', '(b 64)') AS result;
SELECT test_bms_subset_compare('(b 0)', '(b 32)') AS result;
SELECT test_bms_subset_compare('(b 0)', '(b 64)') AS result;
SELECT test_bms_subset_compare('(b 64)', '(b 1)') AS result;
SELECT test_bms_subset_compare('(b 2 1)', '(b 64 2 1)') AS result;
SELECT test_bms_subset_compare('(b 200 64)', '(b 201 1)') AS result;
SELECT test_bms_subset_compare('(b 65 64 1)', '(b 64 2 1)') AS result;
SELECT test_bms_subset_compare('(b 128 64 2)', '(b 65 1)') AS result;

-- bms_copy()
SELECT test_bms_copy(NULL) AS result;
SELECT test_bms_copy('(b 7 5 3 1)') AS result;

-- bms_add_members()
SELECT test_bms_add_members('(b 3 1)', '(b 7 5)') AS result;
SELECT test_bms_add_members('(b 5 3 1)', '(b 7 5 2)') AS result;
SELECT test_bms_add_members('(b 5 3 1)', '(b 300 200 100)') AS result;

-- bitmap_hash()
SELECT test_bitmap_hash('(b)') = 0 AS result;
SELECT test_bitmap_hash('(b 5 3 1)') = test_bitmap_hash('(b 5 3 1)') AS result;
SELECT test_bitmap_hash('(b 5 3 1)') = test_bms_hash_value('(b 5 3 1)') AS result;
SELECT test_bitmap_hash('(b 5 3 1)') != test_bitmap_hash('(b 6 4 2)') AS result;
-- Test module check
SELECT test_bitmap_hash(NULL) AS result;

-- bitmap_match()
SELECT test_bitmap_match('(b)', '(b)') AS result;
SELECT test_bitmap_match('(b)', '(b 5 3 1)') AS result;
SELECT test_bitmap_match('(b 5 3 1)', '(b)') AS result;
SELECT test_bitmap_match('(b 5 3 1)', '(b 5 3 1)') AS result;
SELECT test_bitmap_match('(b 5 3 1)', '(b 6 4 2)') AS result;
SELECT test_bitmap_match('(b 3 1)', '(b 5 3 1)') AS result;
-- Check relationship of bitmap_match() with bms_equal()
SELECT (test_bitmap_match('(b 5 3 1)', '(b 5 3 1)') = 0) =
        test_bms_equal('(b 5 3 1)', '(b 5 3 1)') AS result;
SELECT (test_bitmap_match('(b 5 3 1)', '(b 6 4 2)') = 0) =
        test_bms_equal('(b 5 3 1)', '(b 6 4 2)') AS result;
SELECT (test_bitmap_match('(b)', '(b)') = 0) =
        test_bms_equal('(b)', '(b)') AS result;
-- NULL inputs
SELECT test_bitmap_match('(b 5)', NULL) AS result;
SELECT test_bitmap_match(NULL, '(b 5)') AS result;
SELECT test_bitmap_match(NULL, NULL) AS result;

-- bms_overlap_list()
SELECT test_bms_overlap_list('(b 0)', ARRAY[0]) AS result;
SELECT test_bms_overlap_list('(b 3 2)', ARRAY[1,2]) AS result;
SELECT test_bms_overlap_list('(b 4 3)', ARRAY[3,4,5]) AS result;
SELECT test_bms_overlap_list('(b 10 7)', ARRAY[6,7,8,9]) AS result;
SELECT test_bms_overlap_list('(b 5 1)', ARRAY[6,7,8,9]) AS result;
-- Empty list
SELECT test_bms_overlap_list('(b 1)', ARRAY[]::integer[]) AS result;
-- Overlap list with negative numbers
SELECT test_bms_overlap_list('(b 10 5)', ARRAY[-1,5]) AS result; -- error
SELECT test_bms_overlap_list('(b 3 2 1)', ARRAY[-5,-1,0]) AS result; -- error
-- NULL inputs
SELECT test_bms_overlap_list('(b 5)', NULL) AS result;
SELECT test_bms_overlap_list(NULL, ARRAY[1,2,3]) AS result;
SELECT test_bms_overlap_list(NULL, NULL) AS result;

-- bms_nonempty_difference()
SELECT test_bms_nonempty_difference(NULL, '(b 5 3 1)') AS result;
SELECT test_bms_nonempty_difference('(b 5 3 1)', NULL) AS result;
SELECT test_bms_nonempty_difference(NULL, NULL) AS result;
SELECT test_bms_nonempty_difference('(b 5 3 1)', '(b 6 4 2)') AS result;
SELECT test_bms_nonempty_difference('(b 5 3 1)', '(b 5 1)') AS result;
SELECT test_bms_nonempty_difference('(b 5 3 1)', '(b 5 3 1)') AS result;
-- Difference with different word counts
SELECT test_bms_nonempty_difference('(b 5)', '(b 100)') AS result;
SELECT test_bms_nonempty_difference('(b 100)', '(b 5)') AS result;
SELECT test_bms_nonempty_difference('(b 2 1)', '(b 100 50)') AS result;

-- random operations
SELECT test_random_operations(NULL, 10000, 0, 81920) > 0 AS result;

-- perform some random tests on bms_offset_members()
SELECT test_random_offset_operations(NULL, 1000, 0, 1024) AS result;

-- incorrect inputs
SELECT test_bms_num_members('{QUERY}');
SELECT test_bms_num_members('42');
SELECT test_bms_num_members('(i 1 2)');
SELECT test_bms_num_members('()');
SELECT test_bms_num_members('');
-- empty set
SELECT test_bms_num_members('<>') AS result;
SELECT test_bms_copy('<>') AS result;

DROP EXTENSION test_bitmapset;
