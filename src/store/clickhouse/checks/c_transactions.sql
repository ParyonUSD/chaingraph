-- Check c: experimental transactions (the plan never relies on them; §3, §6.2).
CREATE DATABASE IF NOT EXISTS ch1_wp2_scratch;
SELECT version();
SELECT name, value, changed FROM system.server_settings WHERE name ILIKE '%transaction%' FORMAT TSV;
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_tx;
CREATE TABLE ch1_wp2_scratch.chk_tx (k UInt64) ENGINE = MergeTree ORDER BY k;
SET allow_experimental_transactions = 1;
BEGIN TRANSACTION;
INSERT INTO ch1_wp2_scratch.chk_tx VALUES (1);
COMMIT;
SELECT 'rows after BEGIN/INSERT/COMMIT', count() FROM ch1_wp2_scratch.chk_tx;
SET implicit_transaction = 1;
INSERT INTO ch1_wp2_scratch.chk_tx VALUES (2);
DROP TABLE IF EXISTS ch1_wp2_scratch.chk_tx;
