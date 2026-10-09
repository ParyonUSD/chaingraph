-- Check b: lightweight DELETE / UPDATE on a table with a projection (plan §2.4, §3.1 GC).
CREATE DATABASE IF NOT EXISTS ch1_wp2_scratch;
SELECT version();
SELECT name, value FROM system.merge_tree_settings WHERE name IN ('lightweight_mutation_projection_mode', 'deduplicate_merge_projection_mode') ORDER BY name FORMAT TSV;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_lwd_throw;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_lwd_drop;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_lwd_rebuild;

CREATE TABLE ch1_wp2_scratch.chk_lwd_throw (k UInt64, c UInt64, commit_seq UInt64, PROJECTION p_c (SELECT * ORDER BY c)) ENGINE = MergeTree ORDER BY k;
INSERT INTO ch1_wp2_scratch.chk_lwd_throw SELECT number, number % 7, number % 3 FROM numbers(1000);
-- default mode: lightweight delete
DELETE FROM ch1_wp2_scratch.chk_lwd_throw WHERE commit_seq = 2;
SELECT 'default mode after lightweight DELETE (1000 = not applied)', count() FROM ch1_wp2_scratch.chk_lwd_throw;
-- default mode: lightweight UPDATE statement
UPDATE ch1_wp2_scratch.chk_lwd_throw SET c = 0 WHERE k = 1;
-- classic mutations (what the plan's GC uses)
ALTER TABLE ch1_wp2_scratch.chk_lwd_throw DELETE WHERE commit_seq = 2 SETTINGS mutations_sync = 2;
SELECT 'after ALTER DELETE (667 = applied)', count() FROM ch1_wp2_scratch.chk_lwd_throw;
ALTER TABLE ch1_wp2_scratch.chk_lwd_throw UPDATE c = 100 WHERE k = 0 SETTINGS mutations_sync = 2;
SELECT 'after ALTER UPDATE, via projection p_c (1 = projection rebuilt)', count() FROM ch1_wp2_scratch.chk_lwd_throw WHERE c = 100 SETTINGS force_optimize_projection = 1;
SELECT 'projection parts active', count() FROM system.projection_parts WHERE database = 'ch1_wp2_scratch' AND table = 'chk_lwd_throw' AND active;
-- per-query override of the mode
DELETE FROM ch1_wp2_scratch.chk_lwd_throw WHERE commit_seq = 1 SETTINGS lightweight_mutation_projection_mode = 'drop';

-- table setting 'drop'
CREATE TABLE ch1_wp2_scratch.chk_lwd_drop (k UInt64, c UInt64, commit_seq UInt64, PROJECTION p_c (SELECT * ORDER BY c)) ENGINE = MergeTree ORDER BY k SETTINGS lightweight_mutation_projection_mode = 'drop';
INSERT INTO ch1_wp2_scratch.chk_lwd_drop SELECT number, number % 7, number % 3 FROM numbers(1000);
DELETE FROM ch1_wp2_scratch.chk_lwd_drop WHERE commit_seq = 2;
SELECT 'drop mode after lightweight DELETE (667 = applied)', count() FROM ch1_wp2_scratch.chk_lwd_drop;
SELECT 'drop mode: projection parts active (0 = projection dropped)', count() FROM system.projection_parts WHERE database = 'ch1_wp2_scratch' AND table = 'chk_lwd_drop' AND active;
SELECT 'drop mode: query with force_optimize_projection', count() FROM ch1_wp2_scratch.chk_lwd_drop WHERE c = 3 SETTINGS force_optimize_projection = 1;

-- table setting 'rebuild'
CREATE TABLE ch1_wp2_scratch.chk_lwd_rebuild (k UInt64, c UInt64, commit_seq UInt64, PROJECTION p_c (SELECT * ORDER BY c)) ENGINE = MergeTree ORDER BY k SETTINGS lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO ch1_wp2_scratch.chk_lwd_rebuild SELECT number, number % 7, number % 3 FROM numbers(1000);
DELETE FROM ch1_wp2_scratch.chk_lwd_rebuild WHERE commit_seq = 2;
SELECT 'rebuild mode after lightweight DELETE (667 = applied)', count() FROM ch1_wp2_scratch.chk_lwd_rebuild;
SELECT 'rebuild mode: via projection (count where c = 3)', count() FROM ch1_wp2_scratch.chk_lwd_rebuild WHERE c = 3 SETTINGS force_optimize_projection = 1;
SELECT 'rebuild mode: projection parts active', count() FROM system.projection_parts WHERE database = 'ch1_wp2_scratch' AND table = 'chk_lwd_rebuild' AND active;
UPDATE ch1_wp2_scratch.chk_lwd_rebuild SET c = 0 WHERE k = 1;

DROP TABLE IF EXISTS ch1_wp2_scratch.chk_lwd_throw;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_lwd_drop;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_lwd_rebuild;
