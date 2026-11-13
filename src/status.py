#!/usr/bin/env python3
"""Display a compact live view of the background dataset uploader."""
import argparse
import datetime
import json
from pathlib import Path
import re
import subprocess
import sys
import time


def snapshot(path):
    if not path.exists():
        return None, 'Waiting for the first log entry.'
    with path.open('rb') as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - 2_000_000))
        text = f.read().decode('utf-8', errors='replace')
    text = re.sub(r'\x1b\[[0-9;]*[A-Za-z]', '', text).replace('\r', '\n')
    progress = None
    phase = 'Waiting for uploader.'
    for line in text.splitlines():
        if line.startswith('PROGRESS '):
            try:
                progress = json.loads(line[9:])
            except json.JSONDecodeError:
                pass
        if line.startswith('Downloads '):
            phase = line
        elif line.startswith(('Uploading shard ', 'Resuming at ', 'Verified ', 'Transient failure;', 'Transient outage:', 'Stopped:', 'All source rows processed.', 'Transient I/O error:')):
            phase = line
        elif 'supervisor exit code=' in line:
            phase = line
    return progress, phase


def view(log, total):
    try:
        proc = subprocess.run(['systemctl', '--user', 'is-active', 'bkk-traffy-upload.service'], text=True, capture_output=True, timeout=5)
        state = proc.stdout.strip() or 'unknown'
    except (OSError, subprocess.TimeoutExpired):
        state = 'unknown'
    progress, phase = snapshot(log)
    now = datetime.datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
    lines = ['Bangkok dataset uploader', f'Updated: {now}    Service: {state}', '']
    if progress:
        rows = progress.get('rows', 0)
        pct = f' ({100 * rows / total:.2f}%)' if total else ''
        lines.extend([f'Tickets committed: {rows:,}' + (f' / {total:,}' if total else '') + pct,
                      f'Images saved:      {progress.get("images", 0):,}',
                      f'Images failed:     {progress.get("failed_images", 0):,}',
                      f'Shard data:        {progress.get("shard_bytes", 0) / 1e9:.3f} GB (900 GB pipeline cap)',
                      f'Last batch:        {progress.get("batch", "unknown")}'])
    else:
        lines.append('No verified batch progress found in recent log.')
    lines.extend(['', 'Current activity:', phase, '', 'Refreshes every 2 seconds. Press Ctrl+C to close; upload continues.'])
    return '\n'.join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--log', type=Path, default=Path(__file__).resolve().parents[1] / '.upload-work/production.log')
    p.add_argument('--total', type=int, default=1446228, help='Approximate source ticket count')
    p.add_argument('--once', action='store_true')
    args = p.parse_args()
    if args.once or not sys.stdout.isatty():
        print(view(args.log, args.total))
        return
    try:
        while True:
            print('\033[H\033[2J' + view(args.log, args.total), end='', flush=True)
            time.sleep(2)
    except KeyboardInterrupt:
        print('\nClosed status view; uploader continues.')


if __name__ == '__main__':
    main()
