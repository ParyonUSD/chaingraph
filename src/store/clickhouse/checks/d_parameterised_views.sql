-- Check d: parameterised views as the gate (plan §3.2): a mini utxo table + visibility + commit_log,
-- a view shaped like ddl/050_views.sql utxo_v, correctness of the gate, and sort-key use.
CREATE DATABASE IF NOT EXISTS ch1_wp2_scratch;
SELECT version();
DROP VIEW IF EXISTS ch1_wp2_scratch.chk_pv_utxo_v;
DROP VIEW IF EXISTS ch1_wp2_scratch.chk_pv_simple_v;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_pv_utxo;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_pv_visibility;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_pv_commit_log;

CREATE TABLE ch1_wp2_scratch.chk_pv_utxo (node_internal_id UInt32, token_category FixedString(32), transaction_hash FixedString(32), output_index UInt32, value_satoshis Int64, sign Int8, version UInt64, commit_seq UInt64) ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY (node_internal_id, token_category, transaction_hash, output_index) SETTINGS index_granularity = 256;
CREATE TABLE ch1_wp2_scratch.chk_pv_visibility (node_internal_id UInt32, visible_seq UInt64) ENGINE = ReplacingMergeTree(visible_seq) ORDER BY node_internal_id;
CREATE TABLE ch1_wp2_scratch.chk_pv_commit_log (commit_seq UInt64, state Enum8('intent' = 1, 'incomplete' = 2, 'committed' = 3, 'aborted' = 4), state_rank UInt8 MATERIALIZED toUInt8(state)) ENGINE = ReplacingMergeTree(state_rank) ORDER BY commit_seq;

-- filler: 2 nodes x 50k outputs over 500 categories, seq 1, all visible
INSERT INTO ch1_wp2_scratch.chk_pv_utxo SELECT 1 + (number % 2), toFixedString(unhex(lpad(hex(intDiv(number, 2) % 500), 64, '0')), 32), toFixedString(unhex(lpad(hex(number), 64, '0')), 32), 0, 1000, 1, 1, 1 FROM numbers(100000);
-- category X = 32 x 0xAA. node 1: +1 (seq 2), spend -1 (seq 3, committed), +1 (seq 4, aborted), +1 (seq 6, not yet visible)
INSERT INTO ch1_wp2_scratch.chk_pv_utxo VALUES (1, repeat(unhex('AA'), 32), repeat(unhex('01'), 32), 0, 10, 1, 1, 2), (1, repeat(unhex('AA'), 32), repeat(unhex('02'), 32), 0, 20, 1, 1, 2), (1, repeat(unhex('AA'), 32), repeat(unhex('01'), 32), 0, 10, -1, 1, 3), (1, repeat(unhex('AA'), 32), repeat(unhex('03'), 32), 0, 30, 1, 1, 4), (1, repeat(unhex('AA'), 32), repeat(unhex('04'), 32), 0, 40, 1, 1, 6), (2, repeat(unhex('AA'), 32), repeat(unhex('01'), 32), 0, 10, 1, 1, 2);
INSERT INTO ch1_wp2_scratch.chk_pv_visibility VALUES (1, 5), (2, 1), (2, 2);
INSERT INTO ch1_wp2_scratch.chk_pv_commit_log (commit_seq, state) VALUES (1, 'committed'), (2, 'committed'), (3, 'committed'), (4, 'intent'), (4, 'aborted'), (5, 'committed'), (6, 'intent');

CREATE VIEW ch1_wp2_scratch.chk_pv_simple_v AS SELECT * FROM ch1_wp2_scratch.chk_pv_utxo WHERE node_internal_id = {node:UInt32};
SELECT 'simple view node=1 rows', count() FROM ch1_wp2_scratch.chk_pv_simple_v(node = 1);

CREATE VIEW ch1_wp2_scratch.chk_pv_utxo_v AS SELECT node_internal_id, token_category, transaction_hash, output_index, any(value_satoshis) AS value_satoshis FROM ch1_wp2_scratch.chk_pv_utxo WHERE node_internal_id = {node:UInt32} AND commit_seq <= (SELECT max(visible_seq) FROM ch1_wp2_scratch.chk_pv_visibility WHERE node_internal_id = {node:UInt32}) AND commit_seq NOT IN (SELECT commit_seq FROM ch1_wp2_scratch.chk_pv_commit_log FINAL WHERE state = 'aborted') GROUP BY node_internal_id, token_category, transaction_hash, output_index HAVING sum(sign) > 0;

SELECT 'gate node=1 category X (expect only value 20)', groupArray(value_satoshis) FROM ch1_wp2_scratch.chk_pv_utxo_v(node = 1) WHERE token_category = repeat(unhex('AA'), 32);
SELECT 'gate node=2 category X (expect 10: node 1 spend does not leak)', groupArray(value_satoshis) FROM ch1_wp2_scratch.chk_pv_utxo_v(node = 2) WHERE token_category = repeat(unhex('AA'), 32);
EXPLAIN indexes = 1 SELECT * FROM ch1_wp2_scratch.chk_pv_utxo_v(node = 1) WHERE token_category = repeat(unhex('AA'), 32);
SELECT 'gated category lookup under max_rows_to_read = 1000 of 100006 rows (no error = sort key used)', count() FROM ch1_wp2_scratch.chk_pv_utxo_v(node = 1) WHERE token_category = repeat(unhex('AA'), 32) SETTINGS max_rows_to_read = 1000, read_overflow_mode = 'throw';

DROP VIEW IF EXISTS ch1_wp2_scratch.chk_pv_utxo_v;
DROP VIEW IF EXISTS ch1_wp2_scratch.chk_pv_simple_v;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_pv_utxo;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_pv_visibility;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_pv_commit_log;
