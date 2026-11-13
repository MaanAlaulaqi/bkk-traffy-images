#!/usr/bin/env python3
"""Audit checkpoint, batch ranges, failure logs and shard presence on the Hub."""
import argparse
import json
from pathlib import Path
import tempfile
from huggingface_hub import HfApi, hf_hub_download
from pilot import sha256


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repo', required=True)
    p.add_argument('--revision', required=True)
    p.add_argument('--work-dir', required=True, type=Path)
    p.add_argument('--verify-shards', action='store_true', help='Download and hash every shard (uses bandwidth)')
    a = p.parse_args()
    a.work_dir.mkdir(parents=True, exist_ok=True)
    api = HfApi()
    head = api.repo_info(a.repo, repo_type='dataset', revision=a.revision).sha
    paths = set(api.list_repo_files(a.repo, repo_type='dataset', revision=head))
    if 'checkpoints/production.json' not in paths:
        raise ValueError('Production checkpoint missing')
    with tempfile.TemporaryDirectory(prefix='audit-', dir=a.work_dir) as tmp:
        def fetch(path):
            return Path(hf_hub_download(repo_id=a.repo, repo_type='dataset', revision=head, filename=path, local_dir=tmp))
        checkpoint = json.loads(fetch('checkpoints/production.json').read_text())
        manifests = [json.loads(fetch(path).read_text()) for path in paths if path.startswith('manifests/') and path.endswith('.json')]
        manifests.sort(key=lambda x: x['start_row'])
        offset = 0
        failed = 0
        for m in manifests:
            if m['version'] != 2 or m['start_row'] != offset or m['next_row'] <= offset or m['shard'] not in paths:
                raise ValueError(f'Broken range or missing shard near row {offset}')
            if m['failure_count'] and m.get('failure_log') not in paths:
                raise ValueError(f'Missing failure log for {m["batch"]}')
            if a.verify_shards and sha256(fetch(m['shard'])) != m['sha256']:
                raise ValueError('Shard checksum mismatch: ' + m['shard'])
            offset = m['next_row']
            failed += m['failure_count']
        if not manifests or offset != checkpoint['next_row'] or manifests[-1]['batch'] != checkpoint['batch'] or manifests[-1]['sha256'] != checkpoint['sha256']:
            raise ValueError('Checkpoint does not match the batch ledger')
        print(json.dumps({'revision': head, 'rows': offset, 'batches': len(manifests), 'logged_image_failures': failed, 'all_shard_hashes_verified': a.verify_shards}))


if __name__ == '__main__':
    main()
