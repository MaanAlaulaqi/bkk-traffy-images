#!/usr/bin/env python3
"""Run the uploader with bounded transient retries and persistent logs."""
import argparse
import datetime
import fcntl
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--csv-dir', required=True)
    p.add_argument('--repo', required=True)
    p.add_argument('--revision', required=True)
    p.add_argument('--work-dir', required=True, type=Path)
    p.add_argument('--batch-size', type=int, default=100)
    p.add_argument('--workers', type=int, default=24)
    p.add_argument('--max-batches', type=int, default=1)
    p.add_argument('--max-upload-gb', type=float, default=900)
    p.add_argument('--max-batch-mb', type=int, default=1042)
    p.add_argument('--retries', type=int, default=5)
    args = p.parse_args()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    lock = (args.work_dir / 'supervisor.lock').open('w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit('Another uploader is already running in this work directory')
    log_path = args.work_dir / 'production.log'
    script = Path(__file__).with_name('production.py')
    cmd = [sys.executable, '-u', str(script), '--csv-dir', args.csv_dir, '--repo', args.repo, '--revision', args.revision, '--work-dir', str(args.work_dir / 'batches'), '--batch-size', str(args.batch_size), '--workers', str(args.workers), '--max-batches', str(args.max_batches), '--max-upload-gb', str(args.max_upload_gb), '--max-batch-mb', str(args.max_batch_mb)]
    child = None
    stopping = False
    def stop(signum, frame):
        nonlocal stopping
        stopping = True
        if child and child.poll() is None:
            child.send_signal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with log_path.open('a', encoding='utf-8', buffering=1) as log:
        def emit(line):
            print(line, flush=True)
            log.write(line + '\n')
        for attempt in range(args.retries + 1):
            emit(f'{datetime.datetime.now(datetime.timezone.utc).isoformat()} supervisor start attempt={attempt + 1}')
            child = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            for line in child.stdout:
                emit(line.rstrip('\n'))
            code = child.wait()
            emit(f'{datetime.datetime.now(datetime.timezone.utc).isoformat()} supervisor exit code={code}')
            if code == 0 or stopping:
                return code
            if code != 75 or attempt == args.retries:
                emit('Stopped: non-retryable error or retry limit reached; inspect this log before restarting.')
                return code
            delay = min(30 * 2**attempt, 300)
            emit(f'Transient failure; retrying in {delay}s (attempt {attempt + 1}/{args.retries})')
            for _ in range(delay):
                if stopping:
                    return 130
                time.sleep(1)
    return 1


if __name__ == '__main__':
    sys.exit(main())
