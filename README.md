# Bangkok Traffy image dataset

This repository contains the pilot pipeline for the [Hugging Face dataset](https://huggingface.co/datasets/rocketrat/bkk-traffy-images). The original CSVs remain in the sibling `bangkok` directory.

## Temporary pilot

Install `huggingface_hub` in a virtual environment and authenticate with `hf auth login`. Then, from this directory:

```bash
~/.venvs/bkk-huggingface/bin/python src/pilot.py --csv-dir ../bangkok --repo rocketrat/bkk-traffy-images --count 200 --work-dir .pilot-work
```

The script uniformly samples URL occurrences using reservoir sampling, downloads images with bounded concurrency and retries, builds a TAR archive with a JSON manifest, uploads it to a new temporary dataset branch and verifies a fresh download by SHA-256. Each manifest entry records the ticket, CSV, image field, source URL, status and archive path. The download counter shows completed links and success/failure counts; the Hub client shows byte progress while uploading the single archive. The branch is deleted after the test; local pilot files are removed only after successful verification. Failed runs retain their local workspace for investigation. GitHub branches preserve test history, without image files.

Batch IDs use `<Unix-seconds>-<random suffix>` and Hugging Face commit messages include `epoch=<Unix-seconds> batch=<batch-id>`. Use the batch ID, not timestamps alone, as a unique key. Git itself also records commit timestamps. The JSON output provides machine-readable results.

This is a transfer pilot, not a resumable production dataset uploader. TAR layout and metadata are provisional and not yet verified for Hugging Face dataset streaming. Sample entries may contain personal information; review rights, privacy and repository visibility before publishing a production dataset.

## Checkpointed uploader (test on a dedicated branch first)

`src/upload.py` processes URL occurrences in deterministic CSV/row/field order. Each batch is committed atomically with its TAR archive, manifest, and `checkpoints/latest.json`. The checkpoint stores the next occurrence and hashes of all source CSVs. Restarting verifies the last remote archive and rejects changed source inputs before continuing. Epoch seconds and a unique batch ID appear in every commit message.

```bash
~/.venvs/bkk-huggingface/bin/python src/upload.py --csv-dir ../bangkok --repo rocketrat/bkk-traffy-images --revision YOUR-BRANCH --work-dir .upload-work --batch-size 100 --max-batches 1
```

Use a single uploader per dataset branch. An optimistic parent-commit check prevents silently overwriting concurrent changes. If a push response is lost, rerunning reads the remote checkpoint to decide where to resume. Local batch directories from interrupted or failed runs are retained for inspection and may be removed manually after verifying remote state. If any downloads fail after retries, the batch stops without advancing the checkpoint, records failure details locally, and is retried on the next run. Persistent failures require investigation. The TAR/manifest format is not yet integrated with Hugging Face streaming.

Review the dataset's visibility and the source data's publication rights and privacy before uploading production data.
