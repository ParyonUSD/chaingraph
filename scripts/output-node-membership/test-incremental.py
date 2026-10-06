#!/usr/bin/env python3
"""Disposable local PG18 proof. Uses only Python stdlib and PostgreSQL binaries.

Run: python3 scripts/output-node-membership/test-incremental.py
Optional: PG18_BIN=/path/to/pg18/bin MEMBERSHIP_PLAN_OUTPUT=/tmp/plans.json
No external connection string is accepted. Always stops/removes its own cluster.
This is a correctness/bounded-plan experiment, not a production-scale benchmark.
"""

import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
BIN = Path(os.environ.get("PG18_BIN", "/opt/homebrew/opt/postgresql@18/bin"))
if not (BIN / "postgres").exists():
    raise SystemExit("Set PG18_BIN to the PostgreSQL 18 binary directory")
version = subprocess.check_output([str(BIN / "postgres"), "--version"], text=True)
if " 18." not in version:
    raise SystemExit(f"PostgreSQL 18 required, found: {version}")


def main():
    temporary = Path(tempfile.mkdtemp(prefix="membership-pg18-"))
    data = temporary / "data"
    socket = temporary / "socket"
    socket.mkdir()
    log = temporary / "postgres.log"
    env = os.environ.copy()
    # Override every connection coordinate: never reach an existing database.
    env.update(PGHOST=str(socket), PGPORT="55488", PGDATABASE="postgres", PGUSER="membership_test")
    processes = []
    started = False

    def command(sql, app="membership-test", background=False):
        args = [str(BIN / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", sql]
        local_env = dict(env, PGAPPNAME=app)
        if background:
            process = subprocess.Popen(args, env=local_env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(process)
            return process
        result = subprocess.run(args, env=local_env, text=True, capture_output=True, timeout=45)
        if result.returncode:
            raise AssertionError(f"SQL failed:\n{sql}\n{result.stderr}")
        return result.stdout.strip()

    def finish(process):
        out, err = process.communicate(timeout=10)
        if process.returncode:
            raise AssertionError(f"Concurrent client failed: {out}\n{err}")

    def wait_for(app, event_type):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            if command(f"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE application_name = '{app}' AND wait_event_type = '{event_type}')") == "t":
                return
            time.sleep(0.025)
        raise AssertionError(f"{app} did not enter {event_type}")

    def apply_file(path):
        result = subprocess.run([str(BIN / "psql"), "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", str(path)], env=env, text=True, capture_output=True, timeout=45)
        if result.returncode:
            raise AssertionError(result.stderr)

    def expect_error(sql, message):
        try:
            command(sql)
        except AssertionError as error:
            assert message in str(error), error
        else:
            raise AssertionError(f"expected SQL rejection: {message}")

    def publish(node, changes, txids, direct="[]"):
        return command(f"BEGIN; SELECT output_membership.lock_node({node}); {changes}; SELECT output_membership.refresh_for_node({node}, ARRAY[{txids}]::bigint[], '{direct}'::jsonb); COMMIT;")

    def check(label):
        # Independent normalized-table oracle. Deliberately broad only in tests.
        mismatches = command("""
          WITH actual AS (
            SELECT o.*, ARRAY(
              SELECT n.internal_id FROM node n WHERE EXISTS (
                SELECT 1 FROM transaction t WHERE t.hash=o.transaction_hash AND (
                  EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id=t.internal_id AND nt.node_internal_id=n.internal_id)
                  OR EXISTS (SELECT 1 FROM block_transaction bt JOIN node_block nb USING(block_internal_id)
                    WHERE bt.transaction_internal_id=t.internal_id AND nb.node_internal_id=n.internal_id)
                )
              ) ORDER BY n.internal_id
            ) AS expected_accepted FROM output o
          ), expected AS (
            SELECT a.*, ARRAY(
              SELECT n FROM unnest(expected_accepted) n
              WHERE substring(locking_bytecode FROM 1 FOR 1) <> '\\x6a'::bytea
                AND NOT EXISTS (
                  SELECT 1 FROM input i WHERE i.outpoint_transaction_hash=a.transaction_hash
                    AND i.outpoint_index=a.output_index AND (
                      EXISTS (SELECT 1 FROM node_transaction nt WHERE nt.transaction_internal_id=i.transaction_internal_id AND nt.node_internal_id=n)
                      OR EXISTS (SELECT 1 FROM block_transaction bt JOIN node_block nb USING(block_internal_id)
                        WHERE bt.transaction_internal_id=i.transaction_internal_id AND nb.node_internal_id=n)
                    )
                ) ORDER BY n
            ) AS expected_unspent FROM actual a
          ) SELECT count(*) FROM expected WHERE accepted_node_ids IS DISTINCT FROM expected_accepted
            OR unspent_node_ids IS DISTINCT FROM expected_unspent;
        """)
        assert mismatches == "0", (label, mismatches)
        print(f"PASS {label}", flush=True)

    try:
        subprocess.run([str(BIN / "initdb"), "-D", str(data), "-U", "membership_test", "-A", "trust", "--no-locale", "--encoding=UTF8"], check=True, capture_output=True, text=True)
        with (data / "postgresql.conf").open("a") as conf:
            conf.write(f"\nlisten_addresses = ''\nunix_socket_directories = '{socket}'\nport = 55488\nfsync = off\n")
        subprocess.run([str(BIN / "pg_ctl"), "-D", str(data), "-l", str(log), "-w", "start"], check=True, capture_output=True, text=True)
        started = True
        # Real schema and membership migration; no legacy locking CALL/finalizer.
        migrations = ROOT / "images/hasura/hasura-data/migrations/default"
        apply_file(migrations / "1616195337538_init/up.sql")
        apply_file(migrations / "1790852400000_add_output_node_membership/up.sql")
        apply_file(migrations / "1791280000000_defer_output_membership_maintenance/up.sql")
        assert command("SELECT count(*) FROM pg_trigger WHERE tgname IN ('trigger_output_membership_lock','trigger_zz_output_membership_insert','trigger_zz_output_membership_update','trigger_zz_output_membership_delete') AND tgenabled='D'") == "22"
        assert command("SELECT count(*) FROM pg_trigger WHERE tgname='trigger_output_membership_reject_truncate' AND tgenabled='O'") == "7"
        assert command("SELECT ready FROM output_membership.state WHERE id") == "f"
        assert command("SELECT bool_and(position('pg_advisory' IN pg_get_functiondef(p.oid))=0) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='output_membership' AND p.proname IN ('lock_writer','backfill')") == "t"
        expect_error("""CREATE TEMP TABLE legacy_writer_probe(id integer);
          CREATE TRIGGER legacy_writer_probe BEFORE INSERT ON legacy_writer_probe
            FOR EACH STATEMENT EXECUTE FUNCTION output_membership.lock_writer();
          INSERT INTO legacy_writer_probe VALUES(1);
        """, "legacy array maintenance is retired")
        expect_error("CALL output_membership.backfill(20000)", "legacy backfill is retired")
        print("PASS deferred migration disables 22 legacy triggers, retains 7 TRUNCATE guards, and legacy entry points reject without advisory calls", flush=True)
        # Disable legacy zz maintenance AND global lock triggers before any data.
        command("""DO $$ DECLARE t record; BEGIN
          FOR t IN SELECT c.oid::regclass AS relation, tg.tgname FROM pg_trigger tg
            JOIN pg_class c ON c.oid=tg.tgrelid JOIN pg_namespace ns ON ns.oid=c.relnamespace
            WHERE ns.nspname='public' AND NOT tg.tgisinternal
              AND tg.tgname <> 'trigger_output_membership_reject_truncate'
          LOOP EXECUTE format('ALTER TABLE %s DISABLE TRIGGER %I', t.relation, t.tgname); END LOOP;
          END $$;
          CREATE INDEX spent_by_index ON input(outpoint_transaction_hash,outpoint_index);
          CREATE INDEX block_inclusions_index ON block_transaction(transaction_internal_id);
          CREATE FUNCTION test_hash(id bigint) RETURNS bytea LANGUAGE sql IMMUTABLE AS $$SELECT decode(lpad(to_hex(id),64,'0'),'hex')$$;
          INSERT INTO node(internal_id,name,protocol_version,user_agent) SELECT id,'node-'||id,1,'test' FROM generate_series(1,3) id;
          INSERT INTO transaction(internal_id,hash,version,locktime,size_bytes,is_coinbase)
            SELECT id,test_hash(id),1,0,100,false FROM generate_series(1,12) id WHERE id<>4;
          INSERT INTO output(transaction_hash,output_index,value_satoshis,locking_bytecode)
            SELECT hash,0,100,'\\x51'::bytea FROM transaction;
          INSERT INTO output(transaction_hash,output_index,value_satoshis,locking_bytecode)
            VALUES(test_hash(1),1,0,'\\x6a'::bytea),(test_hash(1),2,0,'\\x'::bytea);
          INSERT INTO input(transaction_internal_id,input_index,outpoint_transaction_hash,outpoint_index,sequence_number,unlocking_bytecode)
            VALUES(2,0,test_hash(1),0,0,'\\x'),(3,0,test_hash(1),0,0,'\\x'),(5,0,test_hash(4),0,0,'\\x');
          INSERT INTO block(internal_id,height,version,timestamp,hash,previous_block_hash,merkle_root,bits,nonce,size_bytes)
            VALUES(10,0,1,0,test_hash(100),test_hash(0),test_hash(0),0,0,100);
          INSERT INTO block_transaction VALUES(10,1,0);
        """)
        apply_file(HERE / "incremental.sql")
        assert command("SELECT output_membership.node_add_if_missing('{}',2),output_membership.node_add_if_missing('{1,3}',2),output_membership.node_add_if_missing('{1,2,3}',2),output_membership.node_remove('{1,2,3}',2)") == "{2}|{1,2,3}|{1,2,3}|{1,3}"
        publish(2, "INSERT INTO node_transaction VALUES(2,1,now())", "1,1")
        publish(1, "INSERT INTO node_transaction VALUES(1,1,now())", "1")
        check("multi-node additions, sorted arrays, OP_RETURN and empty bytecode")
        assert publish(1, "SELECT 1", "1").splitlines()[-1] == "0"
        check("replay performs no heap update")
        publish(1, "INSERT INTO node_block VALUES(1,10,now()); DELETE FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id=1", "1")
        check("confirmation transfer retains block support")
        publish(1, "INSERT INTO node_transaction VALUES(1,2,now()),(1,3,now())", "2,3")
        check("two accepted spenders")
        publish(1, "DELETE FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id=2", "2")
        assert command("SELECT unspent_node_ids FROM output WHERE transaction_hash=test_hash(1) AND output_index=0") == "{2}"
        check("one eviction does not restore an output with another accepted spender")
        publish(1, "DELETE FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id=3", "3")
        check("last spender eviction restores only that node")
        publish(1, "INSERT INTO node_transaction VALUES(1,1,now()); DELETE FROM node_block WHERE node_internal_id=1 AND block_internal_id=10", "1")
        check("removing block support preserves overlapping mempool support")
        publish(1, "INSERT INTO node_block VALUES(1,10,now()); DELETE FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id=1", "1")
        publish(1, "DELETE FROM node_block WHERE node_internal_id=1 AND block_internal_id=10", "1")
        check("one-node reorg preserves the other node")
        publish(1, "INSERT INTO node_transaction VALUES(1,5,now())", "5")
        check("accepted spender before late creator")
        publish(1, "INSERT INTO transaction VALUES(4,test_hash(4),1,0,100,false); INSERT INTO output(transaction_hash,output_index,value_satoshis,locking_bytecode) VALUES(test_hash(4),0,100,'\\x51'); INSERT INTO node_transaction VALUES(1,4,now())", "4")
        assert command("SELECT unspent_node_ids FROM output WHERE transaction_hash=test_hash(4)") == "{}"
        check("late creator sees already accepted spender")
        direct = json.dumps([{"transaction_hash": "\\x" + format(4, "064x"), "output_index": 0}])
        assert publish(1, "DELETE FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id=4", "", direct).splitlines()[-1] == "1"
        check("direct outpoint updates without transaction ID targets")
        publish(1, "INSERT INTO node_transaction VALUES(1,4,now())", "", direct)
        check("direct outpoint creator replay respects accepted spender")

        a = command("BEGIN; SELECT output_membership.lock_node(1); INSERT INTO node_transaction VALUES(1,6,now()); SELECT output_membership.refresh_for_node(1,ARRAY[6]); SELECT pg_sleep(1.5); COMMIT", "same-output-a", True)
        wait_for("same-output-a", "Timeout")
        b = command("BEGIN; SELECT output_membership.lock_node(2); INSERT INTO node_transaction VALUES(2,6,now()); SELECT output_membership.refresh_for_node(2,ARRAY[6]); COMMIT", "same-output-b", True)
        wait_for("same-output-b", "Lock")
        finish(a)
        finish(b)
        assert command("SELECT accepted_node_ids FROM output WHERE transaction_hash=test_hash(6)") == "{1,2}"
        check("concurrent different nodes merge against latest output row")

        a = command("BEGIN; SELECT output_membership.lock_node(1); INSERT INTO node_transaction VALUES(1,7,now()); SELECT output_membership.refresh_for_node(1,ARRAY[7]); SELECT pg_sleep(1.5); COMMIT", "unrelated-a", True)
        wait_for("unrelated-a", "Timeout")
        command("SET statement_timeout='500ms'; BEGIN; SELECT output_membership.lock_node(2); INSERT INTO node_transaction VALUES(2,8,now()); SELECT output_membership.refresh_for_node(2,ARRAY[8]); COMMIT", "unrelated-b")
        assert a.poll() is None, "unrelated writer should finish while first transaction remains open"
        finish(a)
        check("unrelated output writer has no global blocking")

        a = command("BEGIN; SELECT output_membership.lock_node(1); SELECT pg_sleep(1.5); INSERT INTO node_transaction VALUES(1,9,now()); SELECT output_membership.refresh_for_node(1,ARRAY[9]); COMMIT", "same-node-a", True)
        wait_for("same-node-a", "Timeout")
        b = command("BEGIN; SELECT output_membership.lock_node(1); DELETE FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id=9; SELECT output_membership.refresh_for_node(1,ARRAY[9]); COMMIT", "same-node-b", True)
        wait_for("same-node-b", "Lock")
        finish(a)
        finish(b)
        check("same-node publication lock precedes acceptance writes and refresh")

        # Exercise the queued caller API with every transition event. Each
        # command keeps BEGIN/locks/facts/finish on one backend connection.
        command("""BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.lock_node(1);
          INSERT INTO node_transaction VALUES(1,10,now());
          SELECT output_membership.finish_membership_changes(ARRAY[1]); COMMIT;
        """)
        transition = command("""BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.lock_node(1); SELECT output_membership.lock_node(2);
          UPDATE node_transaction SET node_internal_id=2 WHERE node_internal_id=1 AND transaction_internal_id=10;
          SELECT string_agg(node_internal_id||':'||transaction_internal_id,',' ORDER BY node_internal_id)
            FROM pg_temp.output_membership_transaction_changes;
          SELECT output_membership.finish_membership_changes(ARRAY[1,2]); COMMIT;
        """)
        assert "1:10,2:10" in transition, transition
        check("collector UPDATE captures old and new transaction nodes")
        command("""INSERT INTO block(internal_id,height,version,timestamp,hash,previous_block_hash,merkle_root,bits,nonce,size_bytes)
            SELECT id,0,1,0,test_hash(200+id),test_hash(0),test_hash(0),0,0,100 FROM generate_series(30,31) id;
          INSERT INTO block_transaction VALUES(30,11,0),(31,12,0);
          BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(1);
          INSERT INTO node_block VALUES(1,30,now()); SELECT output_membership.finish_membership_changes(ARRAY[1]); COMMIT;
        """)
        transition = command("""BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.lock_node(1); SELECT output_membership.lock_node(2);
          UPDATE node_block SET node_internal_id=2,block_internal_id=31 WHERE node_internal_id=1 AND block_internal_id=30;
          SELECT string_agg(node_internal_id||':'||transaction_internal_id,',' ORDER BY node_internal_id)
            FROM pg_temp.output_membership_transaction_changes;
          SELECT output_membership.finish_membership_changes(ARRAY[1,2]); COMMIT;
        """)
        assert "1:11,2:12" in transition, transition
        check("collector UPDATE captures old and new block transactions")
        command("""BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(2);
          DELETE FROM node_block WHERE node_internal_id=2 AND block_internal_id=31;
          SELECT output_membership.finish_membership_changes(ARRAY[2]); COMMIT;
        """)
        check("collector block deletion publishes acceptance removal")
        replay = command("""BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.lock_node(1); SELECT output_membership.lock_node(2);
          UPDATE node_transaction SET validated_at=validated_at WHERE transaction_internal_id=6;
          SELECT output_membership.note_membership_changes(1,ARRAY[6,6]);
          SELECT output_membership.finish_membership_changes(ARRAY[1,2]);
          SELECT 'left='||(SELECT count(*) FROM pg_temp.output_membership_transaction_changes)
            ||':'||(SELECT count(*) FROM pg_temp.output_membership_outpoint_changes)
            ||':'||current_setting('output_membership.collect_changes'); COMMIT;
        """)
        assert replay.splitlines() == ["0", "left=0:0:off"], replay
        check("queued replay skips heap writes and clears private queues")
        noted = command(f"""BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.lock_node(1);
          INSERT INTO output(transaction_hash,output_index,value_satoshis,locking_bytecode)
            VALUES(test_hash(4),1,1,'\\x51');
          SELECT output_membership.note_membership_changes(1,'{{}}'::bigint[],
            '[{{"transaction_hash":"\\\\x{4:064x}","output_index":1}}]'::jsonb);
          SELECT output_membership.finish_membership_changes(ARRAY[1]); COMMIT;
        """)
        assert noted.splitlines()[-1] == "1", noted
        check("explicit late-body direct outpoint note")
        deferred = command("""BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.lock_node(3); SET LOCAL output_membership.collect_changes='deferred';
          INSERT INTO node_transaction VALUES(3,11,now());
          SELECT 'deferred='||count(*) FROM pg_temp.output_membership_transaction_changes; ROLLBACK;
          BEGIN; INSERT INTO node_transaction VALUES(3,11,now()); ROLLBACK;
        """)
        assert "deferred=0" in deferred, deferred
        check("deferred and default-off collectors skip without publication")
        expect_error("""BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(1);
          INSERT INTO node_transaction VALUES(3,12,now());
          SELECT output_membership.finish_membership_changes(ARRAY[1]); COMMIT;
        """, "node not locked before facts")
        check("unlocked queued node aborts and rolls back normalized writes")
        boundary = command("""BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.note_membership_changes(1,ARRAY[6]); COMMIT;
          BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT 'after_commit='||count(*) FROM pg_temp.output_membership_transaction_changes;
          SELECT output_membership.finish_membership_changes('{}'::integer[]); COMMIT;
        """)
        assert "after_commit=0" in boundary, boundary
        check("ON COMMIT clears queues on reused backend")

        # Enable the actual normalized confirmation/replacement/history-cascade
        # logic. Legacy membership locking/zz triggers remain disabled.
        apply_file(migrations / "1778151011521_cascade_invalidate_mempool_descendants/up.sql")
        apply_file(migrations / "1790950000000_bound_block_confirmation_mempool_cleanup/up.sql")
        command("""ALTER TABLE node_transaction ENABLE TRIGGER trigger_public_node_transaction_insert;
          ALTER TABLE node_block ENABLE TRIGGER trigger_public_node_block_insert;
          ALTER TABLE node_transaction_history ENABLE TRIGGER trigger_public_node_transaction_history_insert;
          INSERT INTO transaction SELECT id,test_hash(id),1,0,100,false FROM generate_series(100,104) id;
          INSERT INTO output(transaction_hash,output_index,value_satoshis,locking_bytecode)
            SELECT test_hash(id),0,100,'\\x51' FROM generate_series(100,104) id;
          INSERT INTO input(transaction_internal_id,input_index,outpoint_transaction_hash,outpoint_index,sequence_number,unlocking_bytecode)
            VALUES(101,0,test_hash(100),0,0,'\\x'),(102,0,test_hash(101),0,0,'\\x'),
              (103,0,test_hash(102),0,0,'\\x'),(104,0,test_hash(100),0,0,'\\x');
          BEGIN; SELECT output_membership.begin_membership_changes();
          SELECT output_membership.lock_node(1); SELECT output_membership.lock_node(2);
          INSERT INTO node_transaction SELECT n,id,now() FROM generate_series(1,2) n CROSS JOIN generate_series(100,103) id;
          SELECT output_membership.finish_membership_changes(ARRAY[1,2]); COMMIT;
        """)
        check("queued publication seeds two-node mempool chain")
        collected = command("""BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(1);
          INSERT INTO node_transaction VALUES(1,104,now());
          SELECT 'queued='||string_agg(transaction_internal_id::text,',' ORDER BY transaction_internal_id)
            FROM pg_temp.output_membership_transaction_changes;
          SELECT output_membership.finish_membership_changes(ARRAY[1]); COMMIT;
        """)
        assert "queued=101,102,103,104" in collected, collected
        assert command("SELECT string_agg(transaction_internal_id::text,',' ORDER BY transaction_internal_id) FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id>=100") == "100,104"
        check("collector captures implicit replacement and recursive history descendants")
        command("""INSERT INTO block(internal_id,height,version,timestamp,hash,previous_block_hash,merkle_root,bits,nonce,size_bytes)
          VALUES(35,0,1,0,test_hash(235),test_hash(0),test_hash(0),0,0,100);
          INSERT INTO block_transaction VALUES(35,104,0);
        """)
        confirmed = command("""BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(1);
          INSERT INTO node_block VALUES(1,35,now());
          SELECT 'queued='||string_agg(transaction_internal_id::text,',' ORDER BY transaction_internal_id)
            FROM pg_temp.output_membership_transaction_changes;
          SELECT output_membership.finish_membership_changes(ARRAY[1]); COMMIT;
        """)
        assert "queued=104\n0" in confirmed, confirmed
        assert command("SELECT count(*) FROM node_transaction WHERE node_internal_id=1 AND transaction_internal_id=104") == "0"
        check("implicit confirmation deletion collected without changing exact membership")

        # Coordinate a real lock-order observation: an inert reader holds Y;
        # writer B's global prelock holds X and waits for Y. Writer A's first
        # node group targets Y and its second group X. It MUST wait for B at X,
        # proving union prelocking precedes node-group publication.
        holder = command("BEGIN; SELECT 1 FROM output WHERE transaction_hash=test_hash(11) AND output_index=0 FOR NO KEY UPDATE; SELECT pg_sleep(3); COMMIT", "groups-holder", True)
        wait_for("groups-holder", "Timeout")
        b = command("BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(3); INSERT INTO node_transaction VALUES(3,10,now()),(3,11,now()); SELECT output_membership.finish_membership_changes(ARRAY[3]); COMMIT", "groups-b", True)
        wait_for("groups-b", "Lock")
        a = command("BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(1); SELECT output_membership.lock_node(2); INSERT INTO node_transaction VALUES(1,11,now()); SELECT output_membership.note_membership_changes(2,ARRAY[10]); SELECT output_membership.finish_membership_changes(ARRAY[1,2]); COMMIT", "groups-a", True)
        wait_for("groups-a", "Lock")
        assert command("SELECT EXISTS (SELECT 1 FROM pg_stat_activity a,pg_stat_activity b WHERE a.application_name='groups-a' AND b.application_name='groups-b' AND b.pid=ANY(pg_blocking_pids(a.pid)))") == "t", "global union must lock X before A's Y-first node group"
        finish(holder)
        finish(b)
        finish(a)
        check("all-node union prelocking prevents cross-group reversed key order")

        # Expand all normalized relations to 100k rows, preserving empty exact
        # membership; avoid an unrealistically tiny-table sequential-scan plan.
        command("""INSERT INTO transaction SELECT id,test_hash(id),1,0,100,false FROM generate_series(10000,109999) id;
          INSERT INTO output(transaction_hash,output_index,value_satoshis,locking_bytecode,accepted_node_ids,unspent_node_ids)
            SELECT test_hash(id),outidx,100,'\\x51','{3}'::integer[],
              CASE WHEN outidx=1 OR id=109999 THEN '{3}'::integer[] ELSE '{}'::integer[] END
            FROM generate_series(10000,109999) id CROSS JOIN generate_series(0,1) outidx;
          INSERT INTO input SELECT id,0,0,0,test_hash(id-1),'\\x' FROM generate_series(10000,109999) id;
          INSERT INTO node_transaction SELECT 3,id,now() FROM generate_series(10000,109999) id;
          INSERT INTO block(internal_id,height,version,timestamp,hash,previous_block_hash,merkle_root,bits,nonce,size_bytes)
            VALUES(20,0,1,0,test_hash(200),test_hash(0),test_hash(0),0,0,31750000);
          INSERT INTO block_transaction SELECT 20,id,id FROM generate_series(10000,109999) id;
          INSERT INTO node_block VALUES(3,20,now()); ANALYZE;
        """)
        # auto_explain captures the ACTUAL nested statements inside the function.
        # Output goes to client stderr at NOTICE, separate from server logs.
        plan_sql = """LOAD 'auto_explain'; SET auto_explain.log_min_duration=0;
          SET auto_explain.log_nested_statements=on; SET auto_explain.log_analyze=on;
          SET auto_explain.log_buffers=on; SET auto_explain.log_format='json';
          SET auto_explain.log_level='notice';
          BEGIN; SELECT output_membership.lock_node(1);
          INSERT INTO node_transaction VALUES(1,12,now());
          SELECT output_membership.refresh_for_node(1,ARRAY[12]); COMMIT;
        """
        result = subprocess.run([str(BIN / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", plan_sql], env=env, text=True, capture_output=True, timeout=20)
        assert result.returncode == 0, result.stderr
        decoder = json.JSONDecoder()
        def parse_plans(stderr):
            parsed_plans = []
            for chunk in stderr.split("NOTICE:"):
                begin = chunk.find('{\n  "Query Text"')
                if begin >= 0:
                    parsed, _ = decoder.raw_decode(chunk[begin:])
                    parsed_plans.append(parsed)
            return parsed_plans
        plans = parse_plans(result.stderr)
        assert plans, result.stderr

        def walk(value):
            if isinstance(value, dict):
                yield value
                for child in value.values():
                    yield from walk(child)
            elif isinstance(value, list):
                for child in value:
                    yield from walk(child)

        nested = [p for p in plans if "public.output" in p["Query Text"] or "public.input" in p["Query Text"]]
        assert len(nested) == 2, [p["Query Text"] for p in nested]
        output_nodes = [n for p in nested for n in walk(p) if n.get("Relation Name") == "output"]
        assert output_nodes and any(n.get("Node Type") == "Tid Scan" for n in output_nodes), output_nodes
        for plan in nested:
            for node in walk(plan):
                assert node.get("Node Type") not in {"Seq Scan", "Hash Join"}, node
                if node.get("Relation Name") in {"output", "input", "transaction", "node_transaction", "block_transaction"}:
                    assert node.get("Actual Rows", 0) <= 3, node
        path = os.environ.get("MEMBERSHIP_PLAN_OUTPUT")
        if path:
            Path(path).write_text(json.dumps(plans, indent=2) + "\n")
        print("PASS actual nested PG18 small-target plans: two refresh statements, 200k unrelated outputs, indexed keys and CTID update; no Seq Scan or Hash Join", flush=True)

        dense_sql = """LOAD 'auto_explain'; SET auto_explain.log_min_duration=0;
          SET auto_explain.log_nested_statements=on; SET auto_explain.log_analyze=on;
          SET auto_explain.log_buffers=on; SET auto_explain.log_format='json';
          SET auto_explain.log_level='notice'; SET statement_timeout='30s';
          BEGIN; SELECT output_membership.begin_membership_changes(); SELECT output_membership.lock_node(1);
          INSERT INTO node_block VALUES(1,20,now());
          SELECT output_membership.finish_membership_changes(ARRAY[1]); COMMIT;
        """
        start = time.monotonic()
        dense = subprocess.run([str(BIN / "psql"), "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-c", dense_sql], env=env, text=True, capture_output=True, timeout=40)
        dense_seconds = time.monotonic() - start
        assert dense.returncode == 0, dense.stderr[-5000:]
        assert dense.stdout.strip().splitlines()[-1] == "200000", dense.stdout
        dense_plans = parse_plans(dense.stderr)
        dense_refresh = [p for p in dense_plans if p["Query Text"].startswith("WITH ids AS") or p["Query Text"].startswith("WITH desired AS")]
        assert len(dense_refresh) == 3, [p["Query Text"][:100] for p in dense_plans]
        for plan in dense_refresh:
            for node in walk(plan):
                if node.get("Relation Name") in {"output", "input", "transaction", "node_transaction", "block_transaction"}:
                    assert node.get("Node Type") != "Seq Scan", node
        collector_plans = [p for p in dense_plans if p["Query Text"].startswith("INSERT INTO pg_temp.output_membership_transaction_changes SELECT change")]
        assert collector_plans, "dense block statement must use the transition-row collector"
        for plan in collector_plans:
            for node in walk(plan):
                if node.get("Relation Name") == "block_transaction":
                    assert node.get("Node Type") in {"Index Scan", "Index Only Scan", "Bitmap Heap Scan"}, node
        assert command("SELECT count(*) FROM output WHERE transaction_hash>=test_hash(10000) AND accepted_node_ids='{1,3}'") == "200000"
        assert command("SELECT count(*) FROM output WHERE transaction_hash>=test_hash(10000) AND unspent_node_ids='{1,3}'") == "100001"
        check("dense block exact 100k transaction / 200k output result")
        if path:
            Path(path).with_name(Path(path).stem + "-dense.json").write_text(json.dumps(dense_plans, indent=2) + "\n")
        print(f"PASS dense queued synthetic block: 100k server-side txids, 200k updated outputs, one global prelock + two refresh statements, {dense_seconds:.3f}s including EXPLAIN instrumentation", flush=True)
        print(version.strip(), flush=True)
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.communicate(timeout=5)
        if started:
            subprocess.run([str(BIN / "pg_ctl"), "-D", str(data), "-m", "immediate", "-w", "stop"], check=True, capture_output=True, text=True)
        shutil.rmtree(temporary)
        print("Disposable local PostgreSQL stopped and removed", flush=True)


if __name__ == "__main__":
    main()
