#!/usr/bin/env python3
"""Archive an exact source revision and package the fresh no-array profile."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile

NORMALIZED_FIXES = {
    '1790950000000_bound_block_confirmation_mempool_cleanup',
    '1791100004000_add_node_block_membership_index',
}
TRACKED_FUNCTIONS = ('accepted_output', 'search_output', 'search_output_prefix', 'unspent_output')
EXCLUDED_FUNCTIONS = ('accepted_output', 'unspent_output')


def prepare(source, revision, yarn_source, destination):
    revision = subprocess.check_output(['git', '-C', str(source), 'rev-parse', revision], text=True).strip()
    if destination.exists():
        raise RuntimeError('Destination must not exist; use a fresh build context')
    archive = subprocess.check_output(['git', '-C', str(source), 'archive', revision])
    destination.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive)) as stream:
        stream.extractall(destination, filter='data')
    # Supply only offline dependencies: never copy .env, build, node_modules or data.
    yarn_hashes = {}
    for directory in ('releases', 'plugins', 'cache'):
        shutil.copytree(yarn_source / '.yarn' / directory, destination / '.yarn' / directory, dirs_exist_ok=True)
        for path in sorted((destination / '.yarn' / directory).rglob('*')):
            if path.is_file():
                yarn_hashes[str(path.relative_to(destination))] = hashlib.sha256(path.read_bytes()).hexdigest()
    migrations = destination / 'images/hasura/hasura-data/migrations/default'
    all_migrations = sorted(path.name for path in migrations.iterdir() if path.is_dir())
    if all_migrations[-1] != '1791280000000_defer_output_membership_maintenance':
        raise RuntimeError('Reviewed migration tail changed; review this profile before packaging')
    selected = [name for name in all_migrations
                if int(name.split('_', 1)[0]) < 1790852400000 or name in NORMALIZED_FIXES]
    if not NORMALIZED_FIXES.issubset(selected):
        raise RuntimeError('Required normalized fixes are missing')
    for name in set(all_migrations) - set(selected):
        shutil.rmtree(migrations / name)
    functions = destination / 'images/hasura/hasura-data/metadata/databases/default/functions'
    expected = ''.join(f'- "!include public_{name}.yaml"\n' for name in TRACKED_FUNCTIONS)
    if (functions / 'functions.yaml').read_text() != expected:
        raise RuntimeError('Tracked function metadata changed; review baseline exclusions')
    (functions / 'functions.yaml').write_text(''.join(
        f'- "!include public_{name}.yaml"\n' for name in TRACKED_FUNCTIONS
        if name not in EXCLUDED_FUNCTIONS))
    for name in EXCLUDED_FUNCTIONS:
        (functions / f'public_{name}.yaml').unlink()
    proof = {
        'profile': 'no-array-baseline', 'source_revision': revision,
        'source_archive_sha256': hashlib.sha256(archive).hexdigest(),
        'selected_migrations': selected,
        'excluded_migrations': sorted(set(all_migrations) - set(selected)),
        'excluded_function_metadata': [f'public.{name}' for name in EXCLUDED_FUNCTIONS],
        'yarn_lock_sha256': hashlib.sha256((destination / 'yarn.lock').read_bytes()).hexdigest(),
        'offline_yarn_sha256': yarn_hashes,
    }
    (destination / 'no-array-build-manifest.json').write_text(json.dumps(proof, indent=2) + '\n')
    # Hasura retains the manifest too, to make packaged migrations auditable.
    shutil.copyfile(destination / 'no-array-build-manifest.json',
                    destination / 'images/hasura/no-array-build-manifest.json')
    print(json.dumps({key: value for key, value in proof.items() if key != 'offline_yarn_sha256'}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--yarn-source', type=Path, required=True)
    parser.add_argument('--destination', type=Path, required=True)
    args = parser.parse_args()
    prepare(args.source, args.revision, args.yarn_source, args.destination)
