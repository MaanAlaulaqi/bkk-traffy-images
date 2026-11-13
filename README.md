# Bangkok Traffy image dataset

This repository contains the pilot pipeline for the [Hugging Face dataset](https://huggingface.co/datasets/rocketrat/bkk-traffy-images). The original CSVs remain in the sibling `bangkok` directory.

## Temporary pilot

Install `huggingface_hub` in a virtual environment and authenticate with `hf auth login`. Then, from this directory:

```bash
~/.venvs/bkk-huggingface/bin/python src/pilot.py --csv-dir ../bangkok --repo rocketrat/bkk-traffy-images --count 200 --work-dir .pilot-work
```

The script uniformly samples URL occurrences using reservoir sampling, downloads images with bounded concurrency and retries, builds a TAR archive with a JSON manifest, uploads it to a new temporary dataset branch and verifies a fresh download by SHA-256. Each manifest entry records the ticket, CSV, image field, source URL, status and archive path. The branch is deleted after the test; local pilot files are removed only after successful verification. Failed runs retain their local workspace for investigation. GitHub branches preserve test history, without image files.

Batch IDs use `<Unix-seconds>-<random suffix>` and Hugging Face commit messages include `epoch=<Unix-seconds> batch=<batch-id>`. Use the batch ID, not timestamps alone, as a unique key. Git itself also records commit timestamps. The JSON output provides machine-readable results.

This is a transfer pilot, not a resumable production dataset uploader. TAR layout and metadata are provisional and not yet verified for Hugging Face dataset streaming. Sample entries may contain personal information; review rights, privacy and repository visibility before publishing a production dataset.
