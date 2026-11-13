#!/usr/bin/env python3
"""Upload deterministic, checkpointed image batches to a dataset branch."""
import argparse
import csv
import itertools
import json
from pathlib import Path
import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pilot import build_archive, download, sha256

CHECKPOINT = 'checkpoints/latest.json'


def source_signature(directory):
    files = sorted(Path(directory).glob('bangkok_*.csv'))
    if not files:
        raise ValueError('No bangkok_*.csv files found')
    return [{'name': p.name, 'sha256': sha256(p)} for p in files]


def links(directory):
    for path in sorted(Path(directory).glob('bangkok_*.csv')):
        with path.open(encoding='utf-8-sig', newline='') as f:
            for row in csv.DictReader(f):
                for field in ('photo', 'photo_after'):
                    url = row.get(field, '').strip()
                    if url:
                        yield {'source_csv': path.name, 'ticket_id': row['ticket_id'], 'field': field, 'url': url}


def checked_checkpoint(state, source):
    if state.get('version') != 1 or state.get('source_files') != source:
        raise ValueError('CSV inputs differ from checkpoint; refusing to resume')
    if not isinstance(state.get('next_index'), int) or state['next_index'] < 0:
        raise ValueError('Invalid checkpoint offset')
    return state['next_index']


def fetch_checkpoint(repo, revision, directory):
    from huggingface_hub import hf_hub_download
    from huggingface_hub.errors import EntryNotFoundError
    try:
        path = hf_hub_download(repo_id=repo, repo_type='dataset', revision=revision, filename=CHECKPOINT, local_dir=directory, force_download=True)
    except EntryNotFoundError:
        return None
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def verify_archive(repo, revision, state, directory):
    from huggingface_hub import hf_hub_download
    print('Verifying committed archive:', state['archive'], flush=True)
    path = hf_hub_download(repo_id=repo, repo_type='dataset', revision=revision, filename=state['archive'], local_dir=directory, force_download=True)
    if sha256(path) != state['archive_sha256']:
        raise RuntimeError('Remote archive checksum mismatch; refusing to continue')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--csv-dir', type=Path, required=True)
    p.add_argument('--repo', required=True)
    p.add_argument('--revision', default='main', help='Existing dataset branch')
    p.add_argument('--work-dir', type=Path, required=True)
    p.add_argument('--batch-size', type=int, default=100)
    p.add_argument('--workers', type=int, default=6)
    p.add_argument('--max-batches', type=int, default=0, help='0 means no limit')
    args = p.parse_args()
    if not 1 <= args.batch_size <= 1000 or not 1 <= args.workers <= 16 or args.max_batches < 0:
        p.error('batch size must be 1–1000, workers 1–16 and max-batches nonnegative')
    from huggingface_hub import HfApi, CommitOperationAdd
    api = HfApi()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    remote_head = api.repo_info(args.repo, repo_type='dataset', revision=args.revision).sha
    source = source_signature(args.csv_dir)
    verify_dir = args.work_dir / ('verify-' + uuid.uuid4().hex)
    state = fetch_checkpoint(args.repo, remote_head, verify_dir)
    offset = checked_checkpoint(state, source) if state else 0
    if state:
        verify_archive(args.repo, remote_head, state, verify_dir)
    shutil.rmtree(verify_dir, ignore_errors=True)
    print(f'Resume at URL occurrence {offset}', flush=True)
    stream = itertools.islice(links(args.csv_dir), offset, None)
    for batch_no in itertools.count(1):
        if args.max_batches and batch_no > args.max_batches:
            break
        batch = list(itertools.islice(stream, args.batch_size))
        if not batch:
            print('All source links have been processed.', flush=True)
            break
        epoch = int(time.time())
        batch_id = f'{epoch}-{uuid.uuid4().hex[:8]}'
        directory = args.work_dir / batch_id
        directory.mkdir(exist_ok=False)
        results = [None] * len(batch)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(download, item): i for i, item in enumerate(batch)}
            for done, f in enumerate(as_completed(futures), 1):
                results[futures[f]] = f.result()
                ok = sum(x is not None and x[1] is not None for x in results)
                print(f'Batch {batch_id}: downloaded {done}/{len(batch)} (ok={ok}, failed={done-ok})', flush=True)
        failures = [r for r, data in results if data is None]
        if failures:
            (directory / 'failures.json').write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding='utf-8')
            raise RuntimeError(f'{len(failures)} downloads failed; checkpoint unchanged; details in {directory}')
        archive = directory / 'batch.tar'
        records = build_archive(archive, results)
        archive_path = f'data/{batch_id}.tar'
        manifest_path = f'manifests/{batch_id}.json'
        digest = sha256(archive)
        manifest = {'version': 1, 'batch': batch_id, 'start_index': offset, 'next_index': offset + len(batch), 'archive': archive_path, 'archive_sha256': digest, 'records': records}
        checkpoint = {'version': 1, 'batch': batch_id, 'next_index': offset + len(batch), 'archive': archive_path, 'archive_sha256': digest, 'manifest': manifest_path, 'source_files': source}
        def encoded(value):
            return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        (directory / 'manifest.json').write_bytes(encoded(manifest))
        (directory / 'checkpoint.json').write_bytes(encoded(checkpoint))
        ops = [CommitOperationAdd(path_in_repo=archive_path, path_or_fileobj=str(archive)), CommitOperationAdd(path_in_repo=manifest_path, path_or_fileobj=encoded(manifest)), CommitOperationAdd(path_in_repo=CHECKPOINT, path_or_fileobj=encoded(checkpoint))]
        print(f'Uploading archive 0/1; {len(batch)} links in batch; byte progress follows', flush=True)
        result = api.create_commit(repo_id=args.repo, repo_type='dataset', revision=args.revision, parent_commit=remote_head, operations=ops, commit_message=f'images epoch={epoch} batch={batch_id} next={checkpoint["next_index"]}')
        remote_head = result.oid
        print('Uploaded archive 1/1; verifying remote contents', flush=True)
        verify_archive(args.repo, remote_head, checkpoint, directory / 'verify')
        offset = checkpoint['next_index']
        shutil.rmtree(directory)
        print(f'Verified batch {batch_id}; checkpoint={offset}; failed={sum(r["status"] != "ok" for r in records)}', flush=True)


if __name__ == '__main__':
    main()
