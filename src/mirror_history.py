#!/usr/bin/env python3
"""Mirror Hugging Face dataset batch commits as metadata-only GitHub commits."""
import argparse
import base64
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time
from huggingface_hub import HfApi, hf_hub_download


def gh_api(path, payload=None):
    cmd = ['gh', 'api', path]
    if payload is not None:
        cmd += ['--method', 'PUT', '--input', '-']
    result = subprocess.run(cmd, input=json.dumps(payload) if payload is not None else None, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError('GitHub request failed: ' + result.stderr.strip())
    return json.loads(result.stdout)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--hf-repo', required=True)
    p.add_argument('--github-repo', required=True)
    p.add_argument('--github-branch', default='main')
    p.add_argument('--work-dir', required=True, type=Path)
    p.add_argument('--interval', type=int, default=60)
    p.add_argument('--once', action='store_true')
    args = p.parse_args()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    tree = gh_api(f'repos/{args.github_repo}/git/trees/{args.github_branch}?recursive=1')
    if tree.get('truncated'):
        raise RuntimeError('GitHub tree is truncated; cannot safely identify existing mirrored batches')
    known = {entry['path'] for entry in tree['tree']}
    api = HfApi()
    while True:
        commits = api.list_repo_commits(args.hf_repo, repo_type='dataset', revision='main')
        for commit in reversed(commits):
            match = re.fullmatch(r'dataset epoch=(\d+) batch=([0-9]+-[0-9a-f]+) next_row=(\d+)', commit.title)
            if not match:
                continue
            epoch, batch, next_row = match.groups()
            path = f'upload-history/{batch}.json'
            if path in known:
                continue
            with tempfile.TemporaryDirectory(prefix='mirror-', dir=args.work_dir) as directory:
                cp = hf_hub_download(repo_id=args.hf_repo, repo_type='dataset', revision=commit.commit_id, filename='checkpoints/production.json', local_dir=directory)
                checkpoint = json.loads(Path(cp).read_text(encoding='utf-8'))
            if checkpoint.get('batch') != batch or checkpoint.get('next_row') != int(next_row):
                raise RuntimeError(f'Hugging Face checkpoint does not match commit {commit.commit_id}')
            keys = ('batch', 'start_row', 'next_row', 'shard', 'sha256', 'failure_count', 'failure_log', 'shard_bytes', 'uploaded_bytes', 'total_failed', 'total_images')
            record = {k: checkpoint[k] for k in keys if k in checkpoint}
            record.update({'epoch': int(epoch), 'huggingface_commit': commit.commit_id, 'huggingface_repository': args.hf_repo, 'huggingface_commit_url': f'https://huggingface.co/datasets/{args.hf_repo}/commit/{commit.commit_id}'})
            content = base64.b64encode((json.dumps(record, indent=2, ensure_ascii=False) + '\n').encode()).decode()
            gh_api(f'repos/{args.github_repo}/contents/{path}', {'message': commit.title, 'content': content, 'branch': args.github_branch})
            known.add(path)
            print(f'Mirrored batch {batch} to GitHub branch {args.github_branch}', flush=True)
        if args.once:
            return
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
