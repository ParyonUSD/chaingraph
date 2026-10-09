-- Check e: index_granularity 256 is accepted; marks/primary-index overhead on a tiny table vs 1024 / 8192.
CREATE DATABASE IF NOT EXISTS ch1_wp2_scratch;
SELECT version();
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_g256;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_g1024;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_g8192;
CREATE TABLE ch1_wp2_scratch.chk_g256 (hash FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY hash SETTINGS index_granularity = 256;
CREATE TABLE ch1_wp2_scratch.chk_g1024 (hash FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY hash SETTINGS index_granularity = 1024;
CREATE TABLE ch1_wp2_scratch.chk_g8192 (hash FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY hash SETTINGS index_granularity = 8192;
INSERT INTO ch1_wp2_scratch.chk_g256 SELECT SHA256(toString(number)), number FROM numbers(100000);
INSERT INTO ch1_wp2_scratch.chk_g1024 SELECT SHA256(toString(number)), number FROM numbers(100000);
INSERT INTO ch1_wp2_scratch.chk_g8192 SELECT SHA256(toString(number)), number FROM numbers(100000);
SELECT table, sum(rows) AS rows, sum(marks) AS marks, sum(marks_bytes) AS marks_bytes, sum(primary_key_bytes_in_memory) AS pk_bytes_in_memory, sum(bytes_on_disk) AS bytes_on_disk FROM system.parts WHERE database = 'ch1_wp2_scratch' AND table LIKE 'chk_g%' AND active GROUP BY table ORDER BY table FORMAT TSVWithNames;
SELECT 'point lookup g256', v FROM ch1_wp2_scratch.chk_g256 WHERE hash = SHA256('4242') SETTINGS max_rows_to_read = 256, read_overflow_mode = 'throw';
SELECT 'point lookup g1024', v FROM ch1_wp2_scratch.chk_g1024 WHERE hash = SHA256('4242') SETTINGS max_rows_to_read = 1024, read_overflow_mode = 'throw';
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_g256;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_g1024;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_g8192;
