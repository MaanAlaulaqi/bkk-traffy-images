# Bangkok Traffy dataset pipeline

Build a versioned, row-oriented image dataset from the CSV files in `../bangkok` and store it in [Hugging Face](https://huggingface.co/datasets/rocketrat/bkk-traffy-images). Every Parquet row preserves the original CSV fields, source filename, nullable `image_before` and `image_after` image columns, and a status for each image. Original URLs remain in `photo` and `photo_after`. Image failures have no placeholder and are logged separately.

## Setup

The existing environment is `~/.venvs/bkk-huggingface`. Install dependencies with `python -m pip install -r requirements.txt` inside a virtual environment, then run `hf auth login` with a token that can write to the dataset. Never commit tokens. See [INSTALL_LOG.md](INSTALL_LOG.md) for changes made to the existing environment.

## Upload and resume

Run one process per branch. Choose an existing branch; publishing to `main` makes data visible according to the dataset's visibility settings. The following intentionally uploads **one batch**:

```bash
~/.venvs/bkk-huggingface/bin/python src/production.py \
  --csv-dir ../bangkok --repo rocketrat/bkk-traffy-images \
  --revision YOUR-BRANCH --work-dir .upload-work --max-batches 1
```

Repeat the same command to resume; `--max-batches 0` continues until all source rows are processed. Each commit atomically writes a Parquet shard, its manifest, the checkpoint and any failure log. It includes `epoch=<seconds>` and a unique `batch=<id>`. After upload the shard is downloaded and checked by SHA-256; successfully verified local batch files are removed. A restart uses the remote checkpoint, verifies its shard, and rejects changed input CSV hashes. If a commit succeeded but its response was lost, rerunning resumes from the committed checkpoint. Interruptions before a commit may leave local working files for inspection; do not delete them until remote status is known.

Uploader defaults: 200 ticket rows per batch, 6 workers (the supervisor defaults to 24), 3 attempts, 15-second network timeout and a 256 MB batch image-byte limit (the supervisor defaults to 1,042 MB). Tune with `--batch-size`, `--workers`, `--attempts`, `--timeout`, and `--max-batch-mb`. The byte limit excludes active downloads and serialization copies and is not a hard process memory limit; reduce batch size and workers if memory is constrained. The script restricts image downloads to the original public Traffy storage bucket and does not follow redirects. If the image byte limit is exceeded, the uploader splits the batch and retries the first half, keeping the remaining rows for the next batch. A single ticket exceeding the limit stops without advancing the checkpoint. Timeouts apply to blocking network operations rather than a strict total deadline.

Failed image downloads are recorded in `failures/<group>/<batch>.jsonl`, and corresponding Parquet image values are null with status `failed`. Missing URLs have status `no_url`. Normal resume moves past failures; it does not automatically retry or backfill them. Audit failures before treating the dataset as complete. Keep inputs unchanged throughout a run; each file is hashed at startup and its size, inode and timestamps are checked before each commit. The script also checks for changes while hashing. Deliberate filesystem metadata manipulation is not fully guarded against.

## Audit and consume

Check batch coverage, shard presence, and the number of logged failures:

```bash
~/.venvs/bkk-huggingface/bin/python src/audit.py --repo rocketrat/bkk-traffy-images --revision YOUR-BRANCH --work-dir .upload-work
```

Add `--verify-shards` to download and hash every shard; this can use substantial bandwidth and disk space. The audit checks consistency through the checkpoint, not whether every source row has been processed or failures resolved. A complete source run ends with `All source rows processed` and can still have logged failures.

Parquet streaming was tested against two temporary shards using `datasets.load_dataset('parquet', data_files=[...], split='train', streaming=True)`. For general use, point `data_files` at the desired shard URLs or use a dataset configuration that selects only `data/**/*.parquet`. Verify the Hub viewer and automatic loading after publishing a dataset card.

## Capacity and privacy

The sample suggests hundreds of GB; actual size, quota, public storage terms and network costs must be checked before a full run. At 200 tickets per commit the full dataset could produce thousands of commits, which may degrade Hub repository performance. Source comments, coordinates, addresses and photographs may contain personal information. Review redistribution permissions and choose appropriate repository visibility before publishing.

`src/pilot.py` and `src/upload.py` are earlier experimental pipelines. Their TAR checkpoints are incompatible with `src/production.py`; do not mix them on the same branch. GitHub pilot branches retain test history; Hugging Face test branches have been removed.

## Supervised run

Run `src/supervise.py` with the same `--csv-dir`, `--repo`, `--revision` and `--work-dir` options as the uploader. It writes a persistent `production.log`, prevents two supervisors using the same work directory, and retries transient errors up to five times with backoff. `--max-batches 0` processes all rows; the default is one batch. `--max-upload-gb` sets a cap on shard bytes recorded by this pipeline (default 900 decimal GB), not total account usage. The log's `PROGRESS` JSON lines include ticket rows, successful images, failed images and uploaded shard bytes. A temporary network download failure stops the current batch without advancing its checkpoint, to avoid logging every image as permanently missing.

The supervisor runs only while its process remains alive; sleep pauses it, and a shutdown or process termination requires starting it again. It does not restart itself after reboot. When stopping it with Ctrl-C, rerun the command to resume. A batch in progress may need to be downloaded again; incomplete local files are retained in the work directory. The retry limit and permanent errors stop the run for inspection. Storage already used by other repositories and remote cache are outside the byte cap.

## Live status

Run `~/.venvs/bkk-huggingface/bin/python src/status.py` from the project directory for a compact view that refreshes in place. Press Ctrl+C to exit the view without stopping the uploader. Add `--once` for one snapshot. The ticket total is an approximate count from the original CSV collection; the storage number is only the shard bytes recorded by this pipeline, not total account usage. The full detailed log remains at `.upload-work/production.log`.

Each Hugging Face dataset batch is mirrored by a metadata-only GitHub commit on `main`, with the same epoch and batch ID. Files in `upload-history/` contain batch counters, checksum and the Hugging Face commit ID; they do not contain dataset records or images. The two histories remain separate. `bkk-traffy-github-mirror.service` checks for new batches and retries after errors. The GitHub repository is public, so its batch metadata is public even when the dataset is private.

## Overlapping pipeline (staged)

`src/pipeline.py` prepares Parquet batches while a single uploader commits earlier batches in row order. The local `pipeline-queue` defaults to **200 decimal GB**. Downloads pause as the queue approaches that threshold, with reserved headroom for an in-progress shard and temporary files. Remote verification streams bytes instead of keeping a second shard on disk. This limit applies to this queue directory, not other working files, library caches or total filesystem use; use an operating-system filesystem quota if a strict filesystem-wide cap is required. Verified uploads are removed from the queue. If interrupted, committed batches are verified and reconciled against the remote checkpoint; ready batches are reused in order. Incomplete building and verification directories belonging to this pipeline are removed during recovery.

The supervisor uses this mode by default, with `--queue-limit-gb 200`; use `--no-pipeline` for the earlier sequential uploader. It shares the supervisor lock with the existing uploader; stop the existing writer before starting it. Parent-commit protection still applies to independent machines or different work directories. The queue may contain unuploaded dataset data; keep it private and retain it when troubleshooting failed runs.
