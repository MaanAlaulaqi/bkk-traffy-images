#!/usr/bin/env python3
"""Ordered local Parquet queue: download and upload in parallel."""
import argparse
import fcntl
import hashlib
import itertools
import json
import os
import resource
from pathlib import Path
import shutil
import sys
import threading
import time
import uuid

from datasets import Dataset
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_url
from huggingface_hub.utils import build_hf_headers
import httpx
from httpx import RequestError
from production import (CHECKPOINT, IMAGE_COLS, create_features, fit_batch,
                        get_checkpoint, inspect_sources, rows, sha256,
                        source_file_stats)


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def queue_bytes(path):
    total = 0
    for p in path.rglob('*'):
        try:
            if p.is_file():
                total += p.stat().st_size
        except FileNotFoundError:
            # The uploader can remove a verified batch while size is checked.
            continue
    return total


def ready_batches(path):
    batches = []
    for directory in path.glob('ready-*'):
        meta = json.loads((directory / 'queue.json').read_text(encoding='utf-8'))
        batches.append((meta['start_row'], directory, meta))
    return sorted(batches, key=lambda x: x[0])


def verify_shard_stream(repo, revision, state):
    """Verify remote bytes without creating an extra file in the staging queue."""
    url = hf_hub_url(repo_id=repo, repo_type='dataset', revision=revision, filename=state['shard'])
    digest = hashlib.sha256()
    with httpx.stream('GET', url, headers=build_hf_headers(), follow_redirects=True, timeout=60) as response:
        response.raise_for_status()
        for chunk in response.iter_bytes():
            digest.update(chunk)
    if digest.hexdigest() != state['sha256']:
        raise ValueError('Remote shard checksum mismatch')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv-dir', type=Path, required=True)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--queue-limit-gb', type=float, default=200)
    parser.add_argument('--batch-size', type=int, default=200)
    parser.add_argument('--workers', type=int, default=24)
    parser.add_argument('--timeout', type=float, default=15)
    parser.add_argument('--attempts', type=int, default=3)
    parser.add_argument('--max-batch-mb', type=int, default=1042)
    parser.add_argument('--max-upload-gb', type=float, default=900)
    parser.add_argument('--max-batches', type=int, default=0)
    parser.add_argument('--inherited-lock-fd', type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 500 or not 1 <= args.workers <= 32 or not 1 <= args.max_batch_mb <= 2048 or not 0 < args.timeout <= 120 or not 1 <= args.attempts <= 10 or args.max_batches < 0 or args.max_upload_gb <= 0:
        parser.error('invalid batch, worker, timeout, attempt or upload limit')
    limit = int(args.queue_limit_gb * 1_000_000_000)
    # Reserve space for the producer's shard and one simultaneous verification copy.
    file_limit = args.max_batch_mb * 1_000_000 + 256_000_000
    reserve = 2 * file_limit + 64_000_000
    if limit <= reserve:
        parser.error('queue limit must exceed two max-sized batches plus safety margin')
    # Bound each shard and its verification copy to the amount reserved above.
    _, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    if hard != resource.RLIM_INFINITY and hard < file_limit:
        raise ValueError('System file-size limit is lower than the required reservation')
    resource.setrlimit(resource.RLIMIT_FSIZE, (file_limit, hard))
    args.work_dir.mkdir(parents=True, exist_ok=True)
    lock_path = args.work_dir / 'supervisor.lock'
    if args.inherited_lock_fd is not None:
        lock = os.fdopen(args.inherited_lock_fd, 'a', closefd=False)
        actual, expected = os.fstat(lock.fileno()), lock_path.stat()
        if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
            raise ValueError('Inherited lock does not match work directory')
    else:
        lock = lock_path.open('a')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError('Another uploader is using this work directory')
    queue = args.work_dir / 'pipeline-queue'
    queue.mkdir(exist_ok=True)
    # These folders cannot have been committed: only ready-* folders are uploaded.
    for pattern in ('building-*', 'verify-start-*', 'verify-recovery-*'):
        for incomplete in queue.glob(pattern):
            if incomplete.is_dir() and not incomplete.is_symlink():
                shutil.rmtree(incomplete)
                print(f'Removed interrupted temporary workspace {incomplete.name}', flush=True)
    initial_files = [{'name': p.name} for p in sorted(args.csv_dir.glob('bangkok_*.csv'))]
    initial = source_file_stats(args.csv_dir, initial_files)
    source, header = inspect_sources(args.csv_dir)
    stats = source_file_stats(args.csv_dir, source)
    if initial != stats:
        raise ValueError('Source files changed during hashing')
    api = HfApi()
    head = api.repo_info(args.repo, repo_type='dataset', revision=args.revision).sha
    verify = queue / ('verify-start-' + uuid.uuid4().hex)
    checkpoint = get_checkpoint(args.repo, head, verify)
    if checkpoint:
        if checkpoint.get('version') != 2 or checkpoint.get('source_files') != source or checkpoint.get('header') != header:
            raise ValueError('Checkpoint and source files do not match')
        verify_shard_stream(args.repo, head, checkpoint)
    shutil.rmtree(verify, ignore_errors=True)
    if not checkpoint and any(p.startswith(('data/', 'manifests/', 'failures/')) for p in api.list_repo_files(args.repo, repo_type='dataset', revision=head)):
        raise ValueError('Existing data without a production checkpoint')
    offset = checkpoint['next_row'] if checkpoint else 0
    state = checkpoint or {'next_row': 0, 'uploaded_bytes': 0, 'total_failed': 0, 'total_images': 0}
    # A crash can occur after the remote commit and before local cleanup.
    staged = ready_batches(queue)
    for _, directory, _ in staged:
        partial = directory / 'verify'
        if partial.is_dir() and not partial.is_symlink():
            shutil.rmtree(partial)
    for start, directory, meta in staged:
        if meta['repo'] != args.repo or meta['revision'] != args.revision or meta['source_files'] != source or meta['header'] != header:
            raise ValueError('Queued batch belongs to different inputs or repository')
        if meta['next_row'] <= offset:
            verify_shard_stream(args.repo, head, meta)
            shutil.rmtree(directory)
            print(f'Recovered already committed batch {meta["batch"]}', flush=True)
    staged = ready_batches(queue)
    next_produce = offset
    for start, directory, meta in staged:
        if start != next_produce or sha256(directory / 'batch.parquet') != meta['sha256']:
            raise ValueError('Queued batches are missing, out of order or corrupt')
        next_produce = meta['next_row']
    signal = threading.Condition()
    stopped = threading.Event()
    errors = []
    producer_done = False

    def producer():
        nonlocal producer_done
        try:
            cursor = rows(args.csv_dir, source)
            for _ in range(next_produce):
                if next(cursor, None) is None:
                    raise ValueError('Queue exceeds source row count')
            position = next_produce
            effective = args.batch_size
            while not stopped.is_set():
                with signal:
                    while not stopped.is_set() and (queue_bytes(queue) + reserve > limit or shutil.disk_usage(queue).free < reserve + 2_000_000_000):
                        print(f'Queue waiting for space: {queue_bytes(queue) / 1e9:.2f}/{args.queue_limit_gb:.2f} GB', flush=True)
                        signal.wait(timeout=10)
                if stopped.is_set():
                    break
                input_rows = list(itertools.islice(cursor, effective))
                if not input_rows:
                    break
                epoch = int(time.time())
                batch = f'{epoch}-{uuid.uuid4().hex[:8]}'
                building = queue / ('building-' + batch)
                building.mkdir()
                input_rows, cursor, output, failures = fit_batch(input_rows, cursor, args.workers, args.timeout, args.attempts, args.max_batch_mb * 1_000_000)
                effective = min(effective, len(input_rows))
                if any(f.get('transient') for f in failures):
                    (building / 'failures.json').write_bytes(encode(failures))
                    raise ConnectionError('Transient image download failures; batch left uncommitted')
                shard = building / 'batch.parquet'
                Dataset.from_list(output, features=create_features(header)).to_parquet(str(shard), batch_size=50)
                if source_file_stats(args.csv_dir, source) != stats:
                    raise ValueError('Source files changed during preparation')
                group = f'{position // 50000:04d}'
                path = f'data/{group}/train-{position:09d}-{batch}.parquet'
                meta = {'repo': args.repo, 'revision': args.revision, 'source_files': source, 'header': header, 'batch': batch, 'epoch': epoch, 'start_row': position, 'next_row': position + len(input_rows), 'shard': path, 'sha256': sha256(shard), 'shard_bytes': shard.stat().st_size, 'failure_count': len(failures), 'failure_log': f'failures/{group}/{batch}.jsonl' if failures else None, 'success_count': sum(1 for row in output for col in IMAGE_COLS if row[col] is not None)}
                (building / 'failures.json').write_bytes(encode(failures))
                (building / 'queue.json').write_bytes(encode(meta))
                if queue_bytes(queue) > limit:
                    raise RuntimeError('Queue limit exceeded; increase safety margin before resuming')
                building.rename(queue / f'ready-{position:012d}-{batch}')
                position = meta['next_row']
                print(f'Queued batch {batch}; through row {position}; queue {queue_bytes(queue)/1e9:.2f} GB', flush=True)
                with signal:
                    signal.notify_all()
        except BaseException as exc:
            errors.append(exc)
        finally:
            producer_done = True
            with signal:
                signal.notify_all()

    thread = threading.Thread(target=producer, name='download-producer', daemon=True)
    thread.start()
    committed = 0
    try:
        while True:
            staged = ready_batches(queue)
            if staged:
                start, directory, meta = staged[0]
                if start != state['next_row']:
                    raise ValueError('Queued batch does not follow remote checkpoint')
                if source_file_stats(args.csv_dir, source) != stats:
                    raise ValueError('Source files changed before commit')
                if state.get('uploaded_bytes', 0) + meta['shard_bytes'] > args.max_upload_gb * 1e9:
                    raise ValueError('Configured remote upload limit reached')
                group = f'{start // 50000:04d}'
                record = {'version': 2, 'batch': meta['batch'], 'start_row': start, 'next_row': meta['next_row'], 'shard': meta['shard'], 'sha256': meta['sha256'], 'source_files': source, 'header': header, 'failure_count': meta['failure_count'], 'failure_log': meta['failure_log'], 'shard_bytes': meta['shard_bytes'], 'uploaded_bytes': state.get('uploaded_bytes', 0) + meta['shard_bytes'], 'total_failed': state.get('total_failed', 0) + meta['failure_count'], 'total_images': state.get('total_images', 0) + meta['success_count']}
                manifest = {k: v for k, v in record.items() if k not in ('source_files', 'header')}
                ops = [CommitOperationAdd(path_in_repo=meta['shard'], path_or_fileobj=str(directory / 'batch.parquet')), CommitOperationAdd(path_in_repo=CHECKPOINT, path_or_fileobj=encode(record)), CommitOperationAdd(path_in_repo=f'manifests/{group}/{meta["batch"]}.json', path_or_fileobj=encode(manifest))]
                if meta['failure_count']:
                    failures = json.loads((directory / 'failures.json').read_text(encoding='utf-8'))
                    ops.append(CommitOperationAdd(path_in_repo=meta['failure_log'], path_or_fileobj=''.join(json.dumps(f, ensure_ascii=False) + '\n' for f in failures).encode()))
                print(f'Uploading queued batch {meta["batch"]}', flush=True)
                info = api.create_commit(repo_id=args.repo, repo_type='dataset', revision=args.revision, parent_commit=head, operations=ops, commit_message=f'dataset epoch={meta["epoch"]} batch={meta["batch"]} next_row={meta["next_row"]}')
                head = info.oid
                verify_shard_stream(args.repo, head, record)
                state = record
                shutil.rmtree(directory)
                print('PROGRESS ' + json.dumps({'rows': state['next_row'], 'images': state['total_images'], 'failed_images': state['total_failed'], 'shard_bytes': state['uploaded_bytes'], 'batch': state['batch']}), flush=True)
                with signal:
                    signal.notify_all()
                committed += 1
                if args.max_batches and committed >= args.max_batches:
                    break
            elif producer_done:
                # Producer may have published its last batch after our first scan.
                if ready_batches(queue):
                    continue
                break
            else:
                with signal:
                    signal.wait(timeout=5)
    finally:
        stopped.set()
        with signal:
            signal.notify_all()
        thread.join()
    if errors:
        raise errors[0]
    print('Pipeline stopped; queued batches remain available for resume.', flush=True)


if __name__ == '__main__':
    try:
        main()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 429 or exc.response.status_code >= 500:
            print(f'Transient HTTP error: {exc}', file=sys.stderr, flush=True)
            sys.exit(75)
        raise
    except (ConnectionError, TimeoutError, OSError, RequestError) as exc:
        print(f'Transient I/O error: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        sys.exit(75)
