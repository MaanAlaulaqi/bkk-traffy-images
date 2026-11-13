#!/usr/bin/env python3
"""Build resumable, row-oriented Hugging Face Parquet image shards."""
import argparse
import csv
import io
import itertools
import json
from pathlib import Path
import shutil
import time
import uuid
import urllib.parse
import urllib.error
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from PIL import Image as PILImage
from datasets import Dataset, Features, Image, Value
from huggingface_hub import CommitOperationAdd, HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError
from httpx import RequestError
from pilot import sha256

CHECKPOINT = 'checkpoints/production.json'
IMAGE_COLS = ('image_before', 'image_after')


class BatchTooLarge(ValueError):
    pass


def inspect_sources(folder):
    files = sorted(Path(folder).glob('bangkok_*.csv'))
    if not files:
        raise ValueError('No bangkok_*.csv files found')
    signatures = []
    header = None
    for path in files:
        with path.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.reader(stream)
            fields = next(reader, [])
        if not {'ticket_id', 'photo', 'photo_after'}.issubset(fields) or len(fields) != len(set(fields)):
            raise ValueError(f'Invalid CSV header: {path.name}')
        if header is not None and fields != header:
            raise ValueError(f'CSV header mismatch: {path.name}')
        header = fields
        signatures.append({'name': path.name, 'sha256': sha256(path)})
    if set(header) & {'source_file', *IMAGE_COLS, 'image_before_status', 'image_after_status'}:
        raise ValueError('Input column collides with output column')
    return signatures, header


def source_file_stats(folder, source):
    result = []
    for entry in source:
        path = Path(folder) / entry['name']
        stat = path.stat()
        result.append((entry['name'], stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns))
    actual = sorted(p.name for p in Path(folder).glob('bangkok_*.csv'))
    if actual != [entry['name'] for entry in source]:
        raise ValueError('Source file list changed')
    return result


def rows(folder, source):
    for file in source:
        path = Path(folder) / file['name']
        with path.open(encoding='utf-8-sig', newline='') as stream:
            reader = csv.DictReader(stream)
            for row in reader:
                if None in row or any(v is None for v in row.values()):
                    raise ValueError(f'Malformed CSV row in {path.name}, line {reader.line_num}')
                if not row['ticket_id']:
                    raise ValueError(f'Missing ticket ID in {path.name}, line {reader.line_num}')
                yield {**row, 'source_file': path.name}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def download(item, timeout, attempts):
    parsed = urllib.parse.urlsplit(item['url'])
    if parsed.scheme != 'https' or parsed.netloc != 'storage.googleapis.com' or not parsed.path.startswith('/traffy_public_bucket/'):
        return {**item, 'status': 'failed', 'error_type': 'DisallowedURL', 'error': 'URL is outside approved image bucket', 'attempts': 0}, None
    opener = urllib.request.build_opener(NoRedirect())
    for attempt in range(attempts):
        try:
            req = urllib.request.Request(item['url'], headers={'User-Agent': 'bkk-traffy-dataset/1.0'})
            with opener.open(req, timeout=timeout) as response:
                data = response.read(20_000_001)
                kind = response.headers.get_content_type()
            if len(data) > 20_000_000 or not kind.startswith('image/'):
                raise ValueError('Response is not an image or exceeds 20 MB')
            return {**item, 'status': 'ok', 'content_type': kind, 'attempts': attempt + 1}, data
        except Exception as exc:
            if attempt + 1 == attempts:
                transient = isinstance(exc, (TimeoutError, ConnectionError, urllib.error.URLError)) and (not isinstance(exc, urllib.error.HTTPError) or exc.code == 429 or exc.code >= 500)
                return {**item, 'status': 'failed', 'error_type': type(exc).__name__, 'error': str(exc), 'attempts': attempt + 1, 'transient': transient}, None
            time.sleep(2 ** attempt)


def download_image(item, timeout, attempts):
    record, data = download(item, timeout, attempts)
    if data is not None:
        try:
            with PILImage.open(io.BytesIO(data)) as image:
                image.verify()
        except Exception as exc:
            return {**record, 'status': 'failed', 'error_type': type(exc).__name__, 'error': f'Invalid image: {exc}'}, None
    return record, data


def create_features(header):
    fields = {name: Value('string') for name in header}
    fields.update({'source_file': Value('string'), 'image_before': Image(decode=False), 'image_after': Image(decode=False), 'image_before_status': Value('string'), 'image_after_status': Value('string')})
    return Features(fields)


def make_batch(input_rows, workers, timeout, attempts, max_bytes):
    output = [{**row, 'image_before': None, 'image_after': None, 'image_before_status': 'no_url', 'image_after_status': 'no_url'} for row in input_rows]
    failures = []
    tasks = []
    for i, row in enumerate(input_rows):
        for field, column in [('photo', 'image_before'), ('photo_after', 'image_after')]:
            url = row[field].strip()
            if url:
                tasks.append((i, column, {'source_csv': row['source_file'], 'ticket_id': row['ticket_id'], 'field': field, 'url': url}))
    total = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = {}
        task_iterator = iter(tasks)
        def submit_next():
            try:
                index, col, item = next(task_iterator)
            except StopIteration:
                return False
            pending[executor.submit(download_image, item, timeout, attempts)] = (index, col)
            return True
        for _ in range(min(workers, len(tasks))):
            submit_next()
        done_count = 0
        while pending:
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                index, col = pending.pop(future)
                record, content = future.result()
                done_count += 1
                output[index][col + '_status'] = record['status']
                if content is None:
                    failures.append(record)
                else:
                    total += len(content)
                    if total > max_bytes:
                        raise BatchTooLarge('Batch exceeds --max-batch-mb')
                    output[index][col] = {'bytes': content, 'path': None}
                print(f'Downloads {done_count}/{len(tasks)}; failures {len(failures)}', flush=True)
                submit_next()
    return output, failures


def fit_batch(input_rows, iterator, workers, timeout, attempts, max_bytes):
    """Split oversized batches without advancing or dropping any source rows."""
    while True:
        try:
            output, failures = make_batch(input_rows, workers, timeout, attempts, max_bytes)
            return input_rows, iterator, output, failures
        except BatchTooLarge:
            if len(input_rows) <= 1:
                raise
            split = len(input_rows) // 2
            iterator = itertools.chain(input_rows[split:], iterator)
            input_rows = input_rows[:split]
            print(f'Batch exceeds image size limit; retrying with {len(input_rows)} tickets and deferring the remainder', flush=True)


def get_checkpoint(repo, revision, tmp):
    try:
        path = hf_hub_download(repo_id=repo, repo_type='dataset', revision=revision, filename=CHECKPOINT, local_dir=tmp, force_download=True)
    except EntryNotFoundError:
        return None
    return json.loads(Path(path).read_text(encoding='utf-8'))


def verify_shard(repo, revision, state, tmp):
    path = hf_hub_download(repo_id=repo, repo_type='dataset', revision=revision, filename=state['shard'], local_dir=tmp, force_download=True)
    if sha256(path) != state['sha256']:
        raise ValueError('Committed shard checksum mismatch')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--csv-dir', required=True, type=Path)
    p.add_argument('--repo', required=True)
    p.add_argument('--revision', required=True, help='Existing dataset branch')
    p.add_argument('--work-dir', required=True, type=Path)
    p.add_argument('--batch-size', type=int, default=200)
    p.add_argument('--workers', type=int, default=6)
    p.add_argument('--timeout', type=float, default=15)
    p.add_argument('--attempts', type=int, default=3)
    p.add_argument('--max-batch-mb', type=int, default=256)
    p.add_argument('--max-batches', type=int, default=1, help='0 processes all rows')
    p.add_argument('--max-upload-gb', type=float, default=900, help='Stop before total committed shard bytes exceed this many decimal GB')
    args = p.parse_args()
    if not 1 <= args.batch_size <= 500 or not 1 <= args.workers <= 16 or not 0 < args.timeout <= 120 or not 1 <= args.attempts <= 10 or not 1 <= args.max_batch_mb <= 1024 or args.max_batches < 0 or args.max_upload_gb <= 0:
        p.error('Invalid batch size, worker count, timeout, attempts, memory limit or batch count')
    api = HfApi()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    initial_files = [{'name': p.name} for p in sorted(args.csv_dir.glob('bangkok_*.csv'))]
    initial_stats = source_file_stats(args.csv_dir, initial_files)
    source, header = inspect_sources(args.csv_dir)
    source_stats = source_file_stats(args.csv_dir, source)
    if source_stats != initial_stats:
        raise ValueError('Source files changed while hashing; try again')
    head = api.repo_info(args.repo, repo_type='dataset', revision=args.revision).sha
    tmp = args.work_dir / ('verify-' + uuid.uuid4().hex)
    checkpoint = get_checkpoint(args.repo, head, tmp)
    if checkpoint:
        if checkpoint.get('version') != 2 or checkpoint.get('source_files') != source or checkpoint.get('header') != header or type(checkpoint.get('next_row')) is not int or checkpoint['next_row'] < 0:
            raise ValueError('Checkpoint invalid or source files changed')
        verify_shard(args.repo, head, checkpoint, tmp)
    shutil.rmtree(tmp, ignore_errors=True)
    if checkpoint is None and any(path.startswith(('data/', 'manifests/', 'failures/')) for path in api.list_repo_files(args.repo, repo_type='dataset', revision=head)):
        raise ValueError('No production checkpoint, but data files already exist; refusing to start')
    offset = checkpoint['next_row'] if checkpoint else 0
    uploaded_bytes = checkpoint.get('uploaded_bytes', 0) if checkpoint else 0
    total_failed = checkpoint.get('total_failed', 0) if checkpoint else 0
    total_images = checkpoint.get('total_images', 0) if checkpoint else 0
    iterator = rows(args.csv_dir, source)
    for _ in range(offset):
        if next(iterator, None) is None:
            raise ValueError('Checkpoint exceeds source row count')
    print(f'Resuming at ticket row {offset}', flush=True)
    effective_batch_size = args.batch_size
    for count in itertools.count(1):
        if args.max_batches and count > args.max_batches:
            break
        input_rows = list(itertools.islice(iterator, effective_batch_size))
        if not input_rows:
            print('All source rows processed. Inspect failure logs for missing images.', flush=True)
            break
        epoch = int(time.time())
        batch = f'{epoch}-{uuid.uuid4().hex[:8]}'
        work = args.work_dir / batch
        if shutil.disk_usage(args.work_dir).free < 2_000_000_000:
            raise ValueError('Less than 2 GB local disk space available; stopping before batch')
        work.mkdir(exist_ok=False)
        input_rows, iterator, output, failures = fit_batch(input_rows, iterator, args.workers, args.timeout, args.attempts, args.max_batch_mb * 1_000_000)
        effective_batch_size = min(effective_batch_size, len(input_rows))
        transient_failures = sum(bool(f.get('transient')) for f in failures)
        task_count = sum(bool(row[field].strip()) for row in input_rows for field in ('photo', 'photo_after'))
        if transient_failures:
            (work / 'failures.json').write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding='utf-8')
            print(f'Transient outage: {transient_failures}/{task_count} downloads failed; checkpoint unchanged', flush=True)
            return 75
        shard = work / 'batch.parquet'
        Dataset.from_list(output, features=create_features(header)).to_parquet(str(shard), batch_size=50)
        # Detect local modifications without rereading every multi-GB CSV for each batch.
        if source_file_stats(args.csv_dir, source) != source_stats:
            raise ValueError('Source files changed during batch; no commit created')
        group = f'{offset // 50000:04d}'
        shard_path = f'data/{group}/train-{offset:09d}-{batch}.parquet'
        failure_path = f'failures/{group}/{batch}.jsonl'
        shard_size = shard.stat().st_size
        if uploaded_bytes + shard_size > args.max_upload_gb * 1_000_000_000:
            raise ValueError('Configured upload size limit reached; checkpoint unchanged')
        success_count = sum(1 for row in output for col in IMAGE_COLS if row[col] is not None)
        state = {'version': 2, 'batch': batch, 'start_row': offset, 'next_row': offset + len(input_rows), 'shard': shard_path, 'sha256': sha256(shard), 'source_files': source, 'header': header, 'failure_count': len(failures), 'failure_log': failure_path if failures else None, 'shard_bytes': shard_size, 'uploaded_bytes': uploaded_bytes + shard_size, 'total_failed': total_failed + len(failures), 'total_images': total_images + success_count}
        manifest = {k:v for k,v in state.items() if k not in ('source_files', 'header')}
        def encode(x): return json.dumps(x, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        (work / 'checkpoint.json').write_bytes(encode(state))
        ops = [CommitOperationAdd(path_in_repo=shard_path, path_or_fileobj=str(shard)), CommitOperationAdd(path_in_repo=CHECKPOINT, path_or_fileobj=encode(state)), CommitOperationAdd(path_in_repo=f'manifests/{group}/{batch}.json', path_or_fileobj=encode(manifest))]
        if failures:
            text = ''.join(json.dumps(f, ensure_ascii=False) + '\n' for f in failures)
            ops.append(CommitOperationAdd(path_in_repo=failure_path, path_or_fileobj=text.encode()))
        print(f'Uploading shard 0/1 with {len(input_rows)} tickets and {len(failures)} failures', flush=True)
        info = api.create_commit(repo_id=args.repo, repo_type='dataset', revision=args.revision, parent_commit=head, operations=ops, commit_message=f'dataset epoch={epoch} batch={batch} next_row={state["next_row"]}')
        head = info.oid
        print('Uploading shard 1/1; verifying remote checksum', flush=True)
        verify_shard(args.repo, head, state, work / 'verify')
        offset = state['next_row']
        uploaded_bytes, total_failed, total_images = state['uploaded_bytes'], state['total_failed'], state['total_images']
        print('PROGRESS ' + json.dumps({'rows': offset, 'images': total_images, 'failed_images': total_failed, 'shard_bytes': uploaded_bytes, 'batch': batch}), flush=True)
        shutil.rmtree(work)
        print(f'Verified {batch}; next row {offset}; failures {len(failures)}', flush=True)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except (ConnectionError, TimeoutError, OSError, RequestError) as exc:
        print(f'Transient I/O error: {type(exc).__name__}: {exc}', file=sys.stderr, flush=True)
        sys.exit(75)
