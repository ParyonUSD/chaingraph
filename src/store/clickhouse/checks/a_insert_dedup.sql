-- Check a: insert_deduplication_token (plan §3.4). Each table gets the same insert twice; count() shows
-- whether the second was dropped. Expected on plain MergeTree: dedup only with
-- non_replicated_deduplication_window > 0. On Cloud the MergeTree engine becomes SharedMergeTree.
CREATE DATABASE IF NOT EXISTS ch1_wp2_scratch;
SELECT version(), getSetting('insert_deduplicate'), getSetting('deduplicate_insert'), getSetting('async_insert_deduplicate'), getSetting('async_insert');
SELECT name, value FROM system.merge_tree_settings WHERE name IN ('non_replicated_deduplication_window', 'replicated_deduplication_window', 'replicated_deduplication_window_seconds', 'replicated_deduplication_window_for_async_inserts') ORDER BY name FORMAT TSV;

DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_default;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_window;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_replwindow;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_vcmt;

-- (1) table defaults
CREATE TABLE ch1_wp2_scratch.chk_dedup_default (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO ch1_wp2_scratch.chk_dedup_default SETTINGS async_insert = 0, insert_deduplication_token = '7:output:0' VALUES (1, 'a'), (2, 'b');
INSERT INTO ch1_wp2_scratch.chk_dedup_default SETTINGS async_insert = 0, insert_deduplication_token = '7:output:0' VALUES (1, 'a'), (2, 'b');
SELECT 'default: same token twice (2 = dedup, 4 = none)', count() FROM ch1_wp2_scratch.chk_dedup_default;

-- (1b) table defaults, async insert with token
INSERT INTO ch1_wp2_scratch.chk_dedup_default SETTINGS async_insert = 1, wait_for_async_insert = 1, insert_deduplication_token = '8:output:0' VALUES (5, 'e');
INSERT INTO ch1_wp2_scratch.chk_dedup_default SETTINGS async_insert = 1, wait_for_async_insert = 1, insert_deduplication_token = '8:output:0' VALUES (5, 'e');
SELECT 'default: async insert, same token twice (5 = dedup, 6 = none)', count() FROM ch1_wp2_scratch.chk_dedup_default;

-- (2) non_replicated_deduplication_window = 100
CREATE TABLE ch1_wp2_scratch.chk_dedup_window (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 0, insert_deduplication_token = '7:output:0' VALUES (1, 'a'), (2, 'b');
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 0, insert_deduplication_token = '7:output:0' VALUES (1, 'a'), (2, 'b');
SELECT 'window=100: same token twice (2 = dedup)', count() FROM ch1_wp2_scratch.chk_dedup_window;
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 0, insert_deduplication_token = '7:output:0' VALUES (9, 'different data, same token');
SELECT 'window=100: same token, different data (2 = token wins)', count() FROM ch1_wp2_scratch.chk_dedup_window;
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 0, insert_deduplication_token = '8:output:0' VALUES (1, 'a'), (2, 'b');
SELECT 'window=100: same data, new token (4 = inserted)', count() FROM ch1_wp2_scratch.chk_dedup_window;
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 1, wait_for_async_insert = 1, insert_deduplication_token = '9:output:0' VALUES (3, 'c');
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 1, wait_for_async_insert = 1, insert_deduplication_token = '9:output:0' VALUES (3, 'c');
SELECT 'window=100: async insert, same token twice (5 = dedup, 6 = none)', count() FROM ch1_wp2_scratch.chk_dedup_window;
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 0, insert_deduplication_token = '10:output:0' SELECT number + 100, 'sel' FROM numbers(3);
INSERT INTO ch1_wp2_scratch.chk_dedup_window SETTINGS async_insert = 0, insert_deduplication_token = '10:output:0' SELECT number + 100, 'sel' FROM numbers(3);
SELECT 'window=100: INSERT SELECT, same token twice (8 = dedup, 11 = none)', count() FROM ch1_wp2_scratch.chk_dedup_window;

-- (3) replicated_deduplication_window = 100 (the replicated / SharedMergeTree knob)
CREATE TABLE ch1_wp2_scratch.chk_dedup_replwindow (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS replicated_deduplication_window = 100;
INSERT INTO ch1_wp2_scratch.chk_dedup_replwindow SETTINGS async_insert = 0, insert_deduplication_token = '7:output:0' VALUES (1, 'a'), (2, 'b');
INSERT INTO ch1_wp2_scratch.chk_dedup_replwindow SETTINGS async_insert = 0, insert_deduplication_token = '7:output:0' VALUES (1, 'a'), (2, 'b');
SELECT 'replicated_window=100: same token twice (2 = dedup)', count() FROM ch1_wp2_scratch.chk_dedup_replwindow;

-- (4) VersionedCollapsingMergeTree with a token
CREATE TABLE ch1_wp2_scratch.chk_dedup_vcmt (node_internal_id UInt32, k UInt64, sign Int8, version UInt64) ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY (node_internal_id, k) SETTINGS non_replicated_deduplication_window = 100;
INSERT INTO ch1_wp2_scratch.chk_dedup_vcmt SETTINGS async_insert = 0, insert_deduplication_token = '7:utxo:0' VALUES (1, 1, 1, 1), (1, 2, -1, 1);
INSERT INTO ch1_wp2_scratch.chk_dedup_vcmt SETTINGS async_insert = 0, insert_deduplication_token = '7:utxo:0' VALUES (1, 1, 1, 1), (1, 2, -1, 1);
SELECT 'vcmt window=100: same token twice (2 = dedup, 4 = none)', count(), sum(sign) FROM ch1_wp2_scratch.chk_dedup_vcmt;

SELECT name, engine FROM system.tables WHERE database = 'ch1_wp2_scratch' AND name LIKE 'chk_dedup%' ORDER BY name FORMAT TSV;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_default;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_window;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_replwindow;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_dedup_vcmt;
