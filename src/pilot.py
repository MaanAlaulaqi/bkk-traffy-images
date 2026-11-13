#!/usr/bin/env python3
"""Create, verify and clean up a temporary Hugging Face image pilot."""
import argparse
import csv
import hashlib
import io
import json
import mimetypes
import os
from pathlib import Path
import random
import tarfile
import time
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

FIELDS = ('photo', 'photo_after')
MAX_BYTES = 20_000_000


def sample_links(directory, limit, seed):
    """Reservoir sample image links across all CSVs, reproducibly and in bounded memory."""
    rng = random.Random(seed)
    sample = []
    total = 0
    for path in sorted(Path(directory).glob('bangkok_*.csv')):
        with path.open(encoding='utf-8-sig', newline='') as handle:
            for row in csv.DictReader(handle):
                for field in FIELDS:
                    url = row.get(field, '').strip()
                    if not url:
                        continue
                    total += 1
                    item = {'source_csv': path.name, 'ticket_id': row['ticket_id'], 'field': field, 'url': url}
                    if len(sample) < limit:
                        sample.append(item)
                    else:
                        index = rng.randrange(total)
                        if index < limit:
                            sample[index] = item
    if not total:
        raise ValueError('No image URLs found in CSV files')
    return sample, total


def download(item):
    for attempt in range(3):
        try:
            request = urllib.request.Request(item['url'], headers={'User-Agent': 'bkk-traffy-image-pilot/1.0'})
            with urllib.request.urlopen(request, timeout=30) as response:
                data = response.read(MAX_BYTES + 1)
                content_type = response.headers.get_content_type()
            if len(data) > MAX_BYTES:
                raise ValueError('image exceeds 20 MB')
            if not content_type.startswith('image/'):
                raise ValueError('unexpected content type: ' + content_type)
            return {**item, 'status': 'ok', 'content_type': content_type, 'sha256': hashlib.sha256(data).hexdigest()}, data
        except Exception as exc:
            if attempt == 2:
                return {**item, 'status': 'failed', 'error': str(exc)}, None
            time.sleep(2 ** attempt)


def build_archive(path, results):
    records = []
    with tarfile.open(path, 'w') as tar:
        for number, (record, data) in enumerate(results):
            record = dict(record)
            if data is not None:
                ext = mimetypes.guess_extension(record['content_type']) or '.img'
                # Numeric names avoid treating external IDs as paths; manifest preserves associations.
                name = f'images/{number:06d}{ext}'
                record['archive_path'] = name
                info = tarfile.TarInfo(name)
                info.size = len(data)
                tar.addfile(info, io.BytesIO(data))
            records.append(record)
        data = json.dumps(records, ensure_ascii=False, indent=2).encode('utf-8')
        info = tarfile.TarInfo('manifest.json')
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    return records


def sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv-dir', type=Path, required=True)
    parser.add_argument('--repo', required=True)
    parser.add_argument('--count', type=int, default=100)
    parser.add_argument('--seed', type=int, default=20261113)
    parser.add_argument('--workers', type=int, default=6)
    parser.add_argument('--work-dir', type=Path, required=True, help='Local pilot workspace; removed after verification')
    args = parser.parse_args()
    if args.count < 1 or not 1 <= args.workers <= 16:
        parser.error('count must be positive and workers must be between 1 and 16')
    from huggingface_hub import HfApi, hf_hub_download
    import shutil
    run_id = f'{int(time.time())}-{uuid.uuid4().hex[:8]}'
    branch = f'pilot-{run_id}'
    # Never reuse an existing workspace, making cleanup narrowly scoped.
    work = args.work_dir / branch
    work.mkdir(parents=True, exist_ok=False)
    a = HfApi()
    created = False
    verified = False
    start = time.monotonic()
    try:
        original = a.dataset_info(args.repo).sha
        sample, population = sample_links(args.csv_dir, args.count, args.seed)
        print(f'{run_id}: selected {len(sample)} of {population} URLs', flush=True)
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            results = list(pool.map(download, sample))
        successes = sum(data is not None for _, data in results)
        print(f'{run_id}: downloaded {successes}; failed {len(results) - successes}', flush=True)
        if not successes:
            raise RuntimeError('No successful downloads')
        archive = work / 'pilot.tar'
        records = build_archive(archive, results)
        digest = sha256(archive)
        a.create_branch(args.repo, branch=branch, repo_type='dataset', revision=original)
        created = True
        a.upload_file(path_or_fileobj=archive, path_in_repo='pilot/pilot.tar', repo_id=args.repo, repo_type='dataset', revision=branch, commit_message=f'pilot epoch={run_id.split("-")[0]} batch={run_id}')
        remote = hf_hub_download(repo_id=args.repo, repo_type='dataset', revision=branch, filename='pilot/pilot.tar', local_dir=work / 'verify', force_download=True)
        if sha256(remote) != digest:
            raise RuntimeError('Downloaded archive checksum mismatch')
        verified = True
        print(json.dumps({'batch': run_id, 'selected': len(sample), 'population': population, 'success': successes, 'failed': len(records)-successes, 'archive_bytes': archive.stat().st_size, 'image_bytes': sum(len(data) for _, data in results if data is not None), 'sha256': digest, 'elapsed_seconds': round(time.monotonic()-start, 1)}), flush=True)
    finally:
        if created:
            a.delete_branch(args.repo, branch=branch, repo_type='dataset')
            print('Removed temporary remote branch:', branch, flush=True)
        if verified:
            shutil.rmtree(work)
            print('Removed verified local pilot files:', work, flush=True)
        else:
            print('Local files retained for inspection:', work, flush=True)
    print('Main revision unchanged:', a.dataset_info(args.repo).sha == original, flush=True)


if __name__ == '__main__':
    main()
