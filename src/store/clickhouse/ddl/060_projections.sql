-- Projections, plan §2.1 and §2.4. Separate from the CREATE TABLEs so Phase 1 can drop/vary them.
-- All are full-column (contiguous reads) except where the plan excludes unlocking_bytecode.
-- ADD PROJECTION on an empty table needs no MATERIALIZE; on a filled table run
-- ALTER TABLE ... MATERIALIZE PROJECTION <name> afterwards (a mutation).
-- MATERIALIZED columns are listed explicitly because SELECT * omits them.
-- output's projections set their own granularity and block sizes (the table itself is tuned for
-- point lookups by outpoint, 010_core.sql); they keep the table defaults of the other tables.
-- p_script and p_category sort by their filter columns only (planning-floor.md). With transaction_hash /
-- output_index as further key columns, ClickHouse analysed every `transaction_hash IN (set)` of every output
-- read against all marks of both projections while choosing a projection, also where the base table was the
-- only sensible path (relationship reads by hash): ~250k index steps per part, 0.1-0.25 s per statement on the
-- synthetic copy, ~0.5 s on mainnet. A category or script read now reads its key range and filters the rows.

ALTER TABLE cg.block ADD PROJECTION IF NOT EXISTS p_height
(
    SELECT * ORDER BY height
);

ALTER TABLE cg.transaction ADD PROJECTION IF NOT EXISTS p_id
(
    SELECT * ORDER BY internal_id
);

ALTER TABLE cg.block_transaction ADD PROJECTION IF NOT EXISTS p_tx
(
    SELECT * ORDER BY transaction_hash
);

-- p_txid: the inclusions of transactions by internal id (fix-pass-6.md §6). Rows are written in commit (time) order,
-- so parts overlap in hash: a hash is probed in every part of p_tx, an id only in the part of its time (ids grow
-- with time). Block inclusions of a page read both conditions; ClickHouse takes whichever projection selects fewer
-- granules (time-ordered synthetic, 10,000 transactions: 84 M -> 10 M rows, 196 -> 34 ms). Parts without it (an
-- existing database before MATERIALIZE) are read through p_tx as before.
ALTER TABLE cg.block_transaction ADD PROJECTION IF NOT EXISTS p_txid
(
    SELECT * ORDER BY transaction_internal_id
);

ALTER TABLE cg.output ADD PROJECTION IF NOT EXISTS p_script
(
    SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key
    ORDER BY locking_bytecode_prefix
) WITH SETTINGS (index_granularity = 1024, min_compress_block_size = 65536, max_compress_block_size = 1048576);

ALTER TABLE cg.output ADD PROJECTION IF NOT EXISTS p_category
(
    SELECT *, locking_bytecode_prefix, nonfungible_token_commitment_key
    ORDER BY (token_category, nonfungible_token_commitment_key)
) WITH SETTINGS (index_granularity = 1024, min_compress_block_size = 65536, max_compress_block_size = 1048576);

-- p_outpoint: the outpoint index of input, slim (outpoint + input key + commit_seq) and tuned like output for point
-- lookups by outpoint (granularity 128, 4 KiB blocks): the join from outputs to the inputs spending them reads one
-- granule per outpoint per part (input denormalisation plan §3.2: 4-15x fewer rows than the full-column projection
-- at granularity 1024). Queries that need other input columns read the table itself.
ALTER TABLE cg.input ADD PROJECTION IF NOT EXISTS p_outpoint
(
    SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index,
           commit_seq
    ORDER BY (outpoint_transaction_hash, outpoint_index)
) WITH SETTINGS (index_granularity = 128, min_compress_block_size = 4096, max_compress_block_size = 4096);

-- p_txid on input: the inputs of transactions by internal id (fix-pass-7.md §3, owner choice §11 q1 option (b),
-- restricted to input). input is ordered by transaction hash, so the inputs level of a page (a contiguous id range,
-- e.g. L35's 10,000 transactions) read ~9 hash-scattered granules per transaction in every part (golden: 96.5 M rows,
-- 5.6 CPU-s, 15 of 15 parts, ~75 % of the levels' CPU). Ordered by id, a page's inputs are a few contiguous
-- granules in the part of their time. Slim like p_outpoint (no unlocking_bytecode; a level that selects it reads the
-- table). The inputs level reads `transaction_hash IN … AND transaction_internal_id IN …` and ClickHouse takes
-- whichever selects fewer granules. Parts without it (until MATERIALIZE PROJECTION p_txid) are read by hash as before.
ALTER TABLE cg.input ADD PROJECTION IF NOT EXISTS p_txid
(
    SELECT transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index,
           sequence_number, commit_seq
    ORDER BY transaction_internal_id
);

-- p_node_height: a node's acceptance rows by height (block filters below the collapse, fix-pass-6.md §3). The key
-- stops at height (fix-pass-7.md §2): with a trailing transaction_internal_id, the window reads' `transaction_internal_id
-- IN (ids)` (for p_node_tx) was a key condition of this projection too; a set on its third column excludes no range in
-- which the height varies, so ClickHouse's generic exclusion search walked every mark of the node in every part before
-- choosing p_node_tx (mainnet: ~90 ms of key analysis per statement, small documents 2-3x slower). Readers filter
-- this projection by node and height only.
ALTER TABLE cg.tx_acceptance ADD PROJECTION IF NOT EXISTS p_node_height
(
    SELECT * ORDER BY (node_internal_id, height)
);
