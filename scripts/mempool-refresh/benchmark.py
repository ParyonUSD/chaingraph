#!/usr/bin/env python3
"""Focused wrapper for the existing disposable actual-agent benchmark.

No cloud, source-node, or existing-harness changes. Adds only a mempool/32
permission to the owned regtest node and verifies the actual agent peer IP.
After automatic reorg ingestion, a separate checkpoint-restored DB proves
startup discovery of the same nonempty source mempool without SQL support.
"""

import argparse
import importlib.util
import ipaddress
import json
from pathlib import Path
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__, add_help=False)
    parser.add_argument('--base-harness', type=Path, required=True)
    parser.add_argument('--agent-peer-ip', required=True)
    options, remaining = parser.parse_known_args()
    peer_ip = str(ipaddress.IPv4Address(options.agent_peer_ip))
    spec = importlib.util.spec_from_file_location('owned_mempool_benchmark', options.base_harness.resolve())
    base = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(base)
    base.SOURCE_FILES.append('src/components/mempool-refresh.ts')

    class MempoolCase(base.Case):
        def setup_node(self):
            if not self.args.mempool_reentry or self.args.explicit_mempool_rebroadcast:
                raise ValueError('This wrapper requires automatic --mempool-reentry without explicit rebroadcast')
            original = base.run

            def owned_permission(args, **kwargs):
                if list(map(str, args[:2])) == ['docker', 'create']:
                    if f'{base.LABEL}={self.owner}' not in args:
                        raise RuntimeError('Refuse permission change without exact case ownership label')
                    args = [*args, f'-whitelist=mempool@{peer_ip}/32', '-debug=net', '-logtimemicros=1']
                return original(args, **kwargs)

            base.run = owned_permission
            try:
                super().setup_node()
            finally:
                base.run = original
            self.trace_protocol = True
            self.report['bip35_owned_permission'] = {
                'whitelist': f'mempool@{peer_ip}/32',
                'scope': 'Only this newly created owner-labeled disposable regtest node',
                'source_observation': 'Explicit IP checked against every actual agent peer below',
                'global_runtime_permissions_changed': False,
            }
            (self.directory / 'mempool-refresh-harness.snapshot.py').write_text(Path(__file__).read_text())
            (self.directory / 'base-harness.snapshot.py').write_text(options.base_harness.read_text())

        def start_agent(self, mode):
            # Pinned BCHN's inbound nNextInvSend derives from non-mockable
            # GetTimeMicros but is compared to mockable current_time. A fixed
            # 2023 clock prevents later BIP35/transaction trickle sends forever.
            # Historical consensus headers remain valid with real node time;
            # change only this owned node, never the production agent clock.
            self.rpc('setmocktime', '0')
            self.report['bip35_fixture_clock'] = {
                'mocktime_during_agent_connections': 0,
                'agent_clock_modified': False,
                'reason': 'BCHN89 inbound send timer mixes wall-clock deadline and mockable comparison',
                'primary_source': 'src/net_processing.cpp lines4357-4358,4710-4713 at pinned89a591f7',
            }
            super().start_agent(mode)

            def ready_peers():
                peers = json.loads(self.rpc('getpeerinfo').stdout)
                selected = [peer for peer in peers if peer.get('subver', '').startswith('/chaingraph-owned-fixture:local/')]
                if len(selected) != 2:
                    return False
                for peer in selected:
                    if peer['addr'].rsplit(':', 1)[0] != peer_ip or peer['permissions'] != ['mempool']:
                        raise RuntimeError('Actual peer IP or narrow mempool-only permission differs from explicit scope')
                suffix = f'{mode}-{self.agent_runs[mode]}'
                (self.directory / f'permission-peerinfo-{suffix}.json').write_text(json.dumps(selected, indent=2) + '\n')
                return True

            self.wait('two actual peers with narrow mempool-only permission', ready_peers)
            (self.directory / f'blockchaininfo-{mode}-{self.agent_runs[mode]}.json').write_text(self.rpc('getblockchaininfo').stdout)

        def benchmark_reorg(self):
            super().benchmark_reorg()
            self.report['reorg']['automatic_reorg_discovery_pass'] = True
            (self.directory / 'reorg-peerinfo-after.json').write_text(self.rpc('getpeerinfo').stdout)
            (self.directory / 'reorg-blockchaininfo-after.json').write_text(self.rpc('getblockchaininfo').stdout)
            self.stop_agent()
            expected = sorted(json.loads(self.rpc('getrawmempool').stdout))
            original_db = self.db
            startup_db = 'startup_' + self.owner
            base.run([self.args.pg_bin / 'createdb', '-w', startup_db], env=dict(self.env, PGDATABASE='postgres'))
            base.run([self.args.pg_bin / 'pg_restore', '-w', '--exit-on-error', '--no-owner', '--no-acl',
                      '-d', startup_db, self.directory / 'checkpoint.dump'], env=dict(self.env, PGDATABASE=startup_db))
            self.db = startup_db
            if self.sql('SELECT count(*) FROM node_transaction') != '0':
                raise RuntimeError('Startup proof must begin with zero normalized mempool support')
            started = time.monotonic()
            self.start_agent('incremental')
            tip = json.loads(self.rpc('getblockheader', self.rpc('getbestblockhash').stdout.strip()).stdout)
            self.wait('startup clone catches the existing alternate chain', lambda: self.block_accepted(tip))
            # As in the base harness, fixed historical block dates require a
            # real restart to mark the fully restored chain caught-up.
            self.stop_agent()
            before = int(self.sql('SELECT count(*) FROM node_transaction'))
            if before != 0:
                raise RuntimeError('Historical catchup unexpectedly enabled mempool before startup proof')
            self.start_agent('incremental')
            self.wait('startup tracking readiness', self.maintenance_ready)
            self.wait('automatic startup nonempty mempool discovery', lambda: int(self.sql('SELECT count(*) FROM node_transaction')) == len(expected))
            actual = json.loads(self.sql("SELECT coalesce(jsonb_agg(encode(t.hash,'hex') ORDER BY encode(t.hash,'hex')),'[]'::jsonb) FROM node_transaction nt JOIN transaction t ON t.internal_id=nt.transaction_internal_id"))
            if actual != expected or sorted(json.loads(self.rpc('getrawmempool').stdout)) != expected or not self.exact_membership():
                raise RuntimeError('Startup identities or full block-OR-mempool membership oracle failed')
            self.report['startup_mempool_discovery'] = {
                'success': True, 'initial_normalized_mempool_count': before,
                'source_mempool_count': len(expected), 'identities_match': True,
                'membership_oracle': 'FULL_BOUNDED_BLOCK_OR_MEMPOOL_VERIFIED',
                'checkpoint_clone_and_catchup_seconds': time.monotonic() - started,
                'support_injected_by_sql': False, 'explicit_rebroadcast': False,
            }
            (self.directory / 'startup-agent-mempool.json').write_text(json.dumps(actual, indent=2) + '\n')
            self.stop_agent()
            self.db = original_db
            self.event('startup-mempool-discovery-verified', **self.report['startup_mempool_discovery'])

    base.Case = MempoolCase
    sys.argv = [str(options.base_harness), *remaining]
    base.main()


if __name__ == '__main__':
    main()
