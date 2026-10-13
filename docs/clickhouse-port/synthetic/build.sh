#!/usr/bin/env bash
# cspell:ignore chipnet paryon
# Synthetic mainnet-like database for API statement work (fix-pass-6.md §1). 150 M transactions, 330 per block, node 1
# ('synth-node') accepts every block; outputs / inputs / scripts / tokens as planning-floor.md §3; planted:
#   - a price contract (P2SH32, category SHA256('paryon'), mutable NFT): 3 transactions per block over the last 66,000
#     blocks (197,999), 5 chains (each spends output 0 of the transaction 5 before it);
#   - a loan contract (same category): 1 transaction per 3 blocks (22,000), 300 chains;
#   - cash-account OP_RETURNs in blocks 150,000-199,999 (50,001), 'Jonathan' and 'bitjson' at height 170,000.
# Random spends never take output 0 of a planted transaction, so the planted chains are the only spends of it.
#
# usage: build.sh <database> [hash|time]
#   hash (default): output / input / transaction / tx_acceptance in 12 chunks of the transaction hash's first byte, like
#        the golden backfill (each part holds a hash range: a hash lookup probes one part);
#   time: the same tables in 12 chunks of internal id (time order: every part spans the whole hash range, as a store
#        synced live, or golden's tx_acceptance / block_transaction).
#   golden: as golden mainnet (fix-pass-7.md): output / transaction hash-chunked, input in 12 chunks of the OUTPOINT
#        hash (golden's 21-input transform: every part spans every spender hash, so a lookup by transaction hash probes
#        all of them), tx_acceptance in 12 chunks of internal id (time order), block_transaction in 2 block-ordered parts.
# Merges of the data tables are stopped so the parts stay as written (SYSTEM START MERGES to undo). Needs ~300 GiB.
# Then: node build-utxo-set.mjs <worktree> <database> 1 (the stored UTXO set, ~15-20 min). Drop afterwards with
# max_table_size_to_drop=0 (tables above 50 GB).
set -euo pipefail
db=$1; layout=${2:-hash}; T=150000000; chunks=12
url=${CH_URL:-http://localhost:18123}
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
P="$url/?min_insert_block_size_rows=200000000&min_insert_block_size_bytes=0&max_insert_threads=1&max_threads=14&max_memory_usage=34000000000&max_execution_time=0&insert_deduplicate=0&max_partitions_per_insert_block=0"
q() { curl -sS --fail-with-body "$P" --data-binary "$1"; }
bash "$here/../../../src/store/clickhouse/ddl/apply.sh" "$url" "$db" > /dev/null
while IFS= read -r line; do [[ -n "$line" ]] && curl -sS --fail-with-body "$url" --data-binary "$line"; done < "$here/udf.sql"
for tbl in output input transaction tx_acceptance block_transaction spend; do q "SYSTEM STOP MERGES $db.$tbl"; done
for ((c=0; c<chunks; c++)); do
  a=$(( c * T / chunks )); n=$(( T / chunks ))
  acc_rows="SELECT number + $a AS t FROM numbers($n)"
  if [[ $layout == hash || $layout == golden ]]; then
    lo=$(( 256 * c / chunks )); hi=$(( 256 * (c + 1) / chunks - 1 ))
    from="numbers($T) WHERE reinterpretAsUInt8(substring(h6_tx(number), 1, 1)) BETWEEN $lo AND $hi"
    rows="SELECT number AS t FROM $from"
    tx="SELECT number AS t, arrayJoin(range(toUInt32(h6_nout(number)))) AS i FROM $from"
    txi="SELECT number AS t, arrayJoin(range(toUInt32(h6_nin(number)))) AS j FROM $from"
  else
    a=$(( c * T / chunks )); n=$(( T / chunks ))
    rows="SELECT number + $a AS t FROM numbers($n)"
    tx="SELECT number + $a AS t, arrayJoin(range(toUInt32(h6_nout(number + $a)))) AS i FROM numbers($n)"
    txi="SELECT number + $a AS t, arrayJoin(range(toUInt32(h6_nin(number + $a)))) AS j FROM numbers($n)"
  fi
  echo "$(date -u +%H:%M:%S) chunk $c"
  q "INSERT INTO $db.output (transaction_hash, output_index, transaction_internal_id, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, commit_seq)
     SELECT h6_tx(t), i, t + 1, h6_value(t, i), h6_lb(t, i), h6_cat(t, i), h6_ft(t, i), h6_cap(t, i), h6_commit(t, i), h6_seq(t) FROM ($tx)"
  q "INSERT INTO $db.input (transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index, sequence_number, unlocking_bytecode, commit_seq)
     SELECT h6_tx(t), j, t + 1, if(t % 330 = 0, toFixedString(repeat('\0', 32), 32), h6_tx(h6_prev(t, j))), if(t % 330 = 0, toUInt32(4294967295), h6_previdx(t, j)), toUInt32(4294967295), randomString(40), h6_seq(t) FROM ($txi)"
  q "INSERT INTO $db.tx_acceptance (transaction_hash, node_internal_id, block_internal_id, transaction_internal_id, height, accepted_at, sign, version, commit_seq)
     SELECT h6_tx(t), toUInt32(1), intDiv(t, 330) + 1, t + 1, toUInt32(intDiv(t, 330)), toDateTime64(1500000000 + intDiv(t, 330) * 600, 3, 'UTC'), toInt8(1), h6_seq(t), h6_seq(t) FROM ($( [[ $layout == golden ]] && echo "$acc_rows" || echo "$rows" ))"
  q "INSERT INTO $db.transaction (hash, internal_id, version, locktime, size_bytes, is_coinbase, input_count, output_count, output_value_satoshis, commit_seq)
     SELECT h6_tx(t), t + 1, 2, 0, toUInt32(200 + cityHash64(t, 'sz') % 800), t % 330 = 0, toUInt32(h6_nin(t)), toUInt32(h6_nout(t)), toInt64(cityHash64(t, 'ov') % 1000000000), h6_seq(t) FROM ($rows)"
  if [[ $layout == time ]]; then
    q "INSERT INTO $db.block_transaction (block_internal_id, transaction_index, transaction_internal_id, transaction_hash, commit_seq)
       SELECT intDiv(t, 330) + 1, toUInt32(t % 330), t + 1, h6_tx(t), h6_seq(t) FROM ($rows)"
  fi
done
if [[ $layout == hash || $layout == golden ]]; then
  # golden: 2 parts in block order
  for ((c=0; c<2; c++)); do
    a=$(( c * T / 2 )); n=$(( T / 2 ))
    q "INSERT INTO $db.block_transaction (block_internal_id, transaction_index, transaction_internal_id, transaction_hash, commit_seq)
       SELECT intDiv(t, 330) + 1, toUInt32(t % 330), t + 1, h6_tx(t), h6_seq(t) FROM (SELECT number + $a AS t FROM numbers($n))"
  done
fi
if [[ $layout == golden ]]; then
  # golden's input parts are chunked by outpoint hash (the 21-input transform), not by the spender's hash
  echo "$(date -u +%H:%M:%S) input by outpoint hash"
  q "CREATE TABLE $db.input_o AS $db.input"
  q "SYSTEM STOP MERGES $db.input_o"
  for ((c=0; c<chunks; c++)); do
    lo=$(( 256 * c / chunks )); hi=$(( 256 * (c + 1) / chunks - 1 ))
    q "INSERT INTO $db.input_o SELECT * FROM $db.input WHERE reinterpretAsUInt8(substring(outpoint_transaction_hash, 1, 1)) BETWEEN $lo AND $hi"
  done
  q "EXCHANGE TABLES $db.input AND $db.input_o"
  curl -sS --fail-with-body "$url/?max_table_size_to_drop=0" --data-binary "DROP TABLE $db.input_o"
fi
echo "$(date -u +%H:%M:%S) spend"
# the spend index directly from the generator (the spent output's attributes are known), 7 chunks by spender hash
for ((c=0; c<7; c++)); do
  lo=$(( 256 * c / 7 )); hi=$(( 256 * (c + 1) / 7 - 1 ))
  curl -sS --fail-with-body "$url/?max_insert_threads=4&max_threads=14&max_memory_usage=20000000000&max_execution_time=0&insert_deduplicate=0&max_partitions_per_insert_block=0" --data-binary "
    INSERT INTO $db.spend (transaction_hash, input_index, transaction_internal_id, outpoint_transaction_hash, outpoint_index, value_satoshis, locking_bytecode, token_category, fungible_token_amount, nonfungible_token_capability, nonfungible_token_commitment, commit_seq, spent_commit_seq, batch)
    SELECT h6_tx(t), j, t + 1, h6_tx(p), pi, h6_value(p, pi), h6_lb(p, pi), h6_cat(p, pi), h6_ft(p, pi), h6_cap(p, pi), h6_commit(p, pi), h6_seq(t), h6_seq(p), h6_seq(t)
    FROM (SELECT t, j, h6_prev(t, j) AS p, h6_previdx(t, j) AS pi FROM (SELECT number AS t, arrayJoin(range(toUInt32(h6_nin(number)))) AS j FROM numbers($T) WHERE number % 330 != 0 AND reinterpretAsUInt8(substring(h6_tx(number), 1, 1)) BETWEEN $lo AND $hi))"
done
B=$(( (T + 329) / 330 )); C=$(( (B + 299) / 300 )); LAST=$((1099511627777 + C - 1))
q "INSERT INTO $db.block SELECT SHA256(concat('blk', toString(b))), b + 1, toUInt32(b), 1, toUInt32(1500000000 + b * 600), SHA256(concat('blk', toString(b - 1))), SHA256(concat('mr', toString(b))), 0, 0, 100000, 330, 0, toUInt64(1099511627777 + intDiv(b, 300)) FROM (SELECT number AS b FROM numbers($B))"
q "INSERT INTO $db.node_block (node_internal_id, block_internal_id, block_hash, height, accepted_at, sign, version, commit_seq) SELECT 1, b + 1, SHA256(concat('blk', toString(b))), toUInt32(b), toDateTime64(1500000000 + b * 600, 3, 'UTC'), 1, toUInt64(1099511627777 + intDiv(b, 300)), toUInt64(1099511627777 + intDiv(b, 300)) FROM (SELECT number AS b FROM numbers($B))"
q "INSERT INTO $db.commit_log (commit_seq, state, node_scope, kind, block_hash, row_counts, writer_epoch, started_at, finished_at) SELECT toUInt64(1099511627777 + c), 'committed', [toUInt32(1)], 'block', toFixedString(repeat('\0', 32), 32), map('tx_acceptance', toUInt64(99000), 'node_block', toUInt64(300), 'output', toUInt64(287000), 'input', toUInt64(250000)), 1, now64(3), now64(3) FROM (SELECT number AS c FROM numbers($C))"
q "INSERT INTO $db.node (internal_id, name, protocol_version, user_agent, first_connected_at, latest_connection_began_at, updated_at, commit_seq) VALUES (1, 'synth-node', 70016, '/synthetic/', now64(3), now64(3), now64(3), 1099511627777)"
q "INSERT INTO $db.spend_watermark (watermark, batch_from, rows, orphans, writer_epoch, filled_at) VALUES ($LAST, 0, 0, 0, 1, now64(3))"
q "INSERT INTO $db.visibility (node_internal_id, visible_seq, updated_at) VALUES (0, $LAST, now64(3)), (1, $LAST, now64(3))"
q "SYSTEM START MERGES $db.spend"
echo "$(date -u +%H:%M:%S) done blocks=$B commits=$C last=$LAST"
