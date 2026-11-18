---
language:
- th
pretty_name: Bangkok Traffy Fondue Civic Reports with Images
task_categories:
- text-classification
- image-classification
tags:
- civic-issues
- bangkok
- thailand
- multimodal
- image-text
- citizen-reports
configs:
- config_name: default
  data_files:
  - split: train
    path: "data/*/*.parquet"
---

# Bangkok Traffy Fondue Civic Reports with Images

## Dataset summary

A work-in-progress research collection of Bangkok civic issue reports originally downloaded as monthly CSV files from [Traffy x Bangkok](https://bangkok.traffy.in.th/). Each record brings the original CSV fields together with the available downloaded images in a row-oriented Parquet shard. Reports cover matters such as streets, sidewalks, flooding, waste and other local issues. Free-text content is predominantly Thai; other languages and mixed-language text may occur.

The repository is a **derived research dataset**, not an official publication or endorsement by the Bangkok Metropolitan Administration (BMA), Traffy Fondue, NECTEC or NSTDA. The data reflects recorded reports and administrative actions, which may be incomplete, outdated or incorrect. Uploaded content is still being processed; counts and coverage change as ingestion proceeds.

## Source and attribution

- **Direct CSV source:** [Traffy x Bangkok](https://bangkok.traffy.in.th/), as identified by the dataset maintainer. The inputs to this repository are monthly `bangkok_YYYY-MM.csv` files downloaded from that site. The exact export settings and original download date have not been independently verified.
- **Original platform and public-sector context:** Traffy Fondue reports for Bangkok. A [Bangkok Metropolitan Administration manual](https://webportal.bangkok.go.th/public/user_files_editor/119/Traffy%20Fondue/%E0%B8%84%E0%B8%B9%E0%B9%88%E0%B8%A1%E0%B8%B7%E0%B8%AD%E0%B8%81%E0%B8%B2%E0%B8%A3%E0%B9%83%E0%B8%8A%E0%B9%89%E0%B8%87%E0%B8%B2%E0%B8%99%20Traffy%20Fondue%20%E0%B8%AA%E0%B8%B3%E0%B8%AB%E0%B8%A3%E0%B8%B1%E0%B8%9A%E0%B8%81%E0%B8%A3%E0%B8%B8%E0%B8%87%E0%B9%80%E0%B8%97%E0%B8%9E%E0%B8%A1%E0%B8%AB%E0%B8%B2%E0%B8%99%E0%B8%84%E0%B8%A3.pdf) describes the Bangkok interface and CSV export.
- **Related official catalog:** [Open Government Data of Thailand: Traffy Fondue complaint statistics](https://data.go.th/en/dataset/traffy-fondue). This is related catalog information, not independently verified as the exact export used here. The catalog identifies the BMA and links to the Traffy source.
- **Derived dataset preparation:** [bkk-traffy-images pipeline](https://github.com/MaanAlaulaqi/bkk-traffy-images). The pipeline reads the monthly CSVs, downloads eligible image URLs, validates images, and packages records and available image bytes together.

## Access, license and attribution status

This repository is currently **private**. Source data and photographs have not been assigned a license in this card because the license and redistribution rights for this particular CSV export and its images have not been verified. The related government catalog states that its license is not specified; that statement does not itself establish the terms for these exact files or for individual images. Do not assume that public accessibility of a source grants permission to redistribute, publish images, or use them for every purpose. Review the source's applicable terms and image rights before sharing or publishing.

Credit the original Traffy x Bangkok / BMA sources as appropriate, and separately cite this derived dataset and its pipeline when using them. A formal citation and redistribution license have not yet been established.

## Snapshot and coverage

The original input collection contained **60 monthly CSV files** named from `bangkok_2021-09.csv` through `bangkok_2026-11.csv`, with gaps in the filename sequence. A local scan counted **1,446,228 input rows** and found a `ticket_id` in every row with no duplicates across those input files. Those counts describe the input collection, not necessarily every currently uploaded record. Filename months do not by themselves establish complete monthly coverage or guarantee the date of every record.

At Hugging Face revision [`0e309689ad57bedccd5ff5ae95f2b2a4a9f98f8d`](https://huggingface.co/datasets/rocketrat/bkk-traffy-images/tree/0e309689ad57bedccd5ff5ae95f2b2a4a9f98f8d), checked on **2026-11-18**, the ingestion checkpoint recorded:

| Measure | Value |
| --- | ---: |
| Processed input rows | 1,367,000 |
| Successfully embedded images | 2,448,838 |
| Failed image downloads or validations | 1,707 |
| Committed shard bytes | 1,045,687,850,615 |

The success count includes both initial and after images, not distinct tickets or necessarily unique image content. A report may have zero, one or two available images. Missing URLs do not count as failed downloads. These checkpoint counters are cumulative pipeline records, not an independent audit of every stored shard. The dataset may have advanced after this snapshot; consult `checkpoints/production.json` at the revision being used.

## Structure

The unit of observation is a ticket. Text, metadata and embedded image values are stored together in Parquet rows under `data/<group>/`. The original CSV values are retained as strings; the uploader does not establish typed dates, coordinates, label taxonomy or jurisdiction semantics. Missing strings can be empty.

| Field(s) | Description |
| --- | --- |
| `ticket_id` | Source ticket identifier; use it to connect a row to the original CSV. |
| `source_file` | Name of the input monthly CSV. |
| `comment` | Source report text. |
| `type`, `problemtype_tag` | Source issue-type fields; label semantics, hierarchical values and multiplicity need auditing before use as targets. |
| `organization`, `organization_action` | Source organization-related fields; they may contain multiple names and may reflect later processing. Their meaning as routing ground truth needs verification. |
| `photo`, `photo_after` | Original source URLs for the initial and after images. |
| `image_before`, `image_after` | Nullable image objects with embedded bytes; `image_before` comes from `photo`, and `image_after` from `photo_after`. |
| `image_before_status`, `image_after_status` | `ok` for a downloaded and validated image; `no_url` for an empty URL; `failed` for an unsuccessful download or image validation. |
| `coords`, `address`, `subdistrict`, `district`, `province` | Source location fields; may be incomplete or sensitive. |
| `timestamp`, `last_activity`, `timestamp_inprogress`, `timestamp_finished` | Source timestamp strings; timezone and exact field semantics have not been independently verified. |
| `state`, `star`, `count_reopen`, `duration_minutes_*`, `message_id` | Source status, rating, reopening, duration and identifier fields; validate semantics for a particular analysis. |

`checkpoints/production.json` is the latest cumulative ingestion checkpoint. `manifests/<group>/` records batch row ranges, shard paths and checksums. `failures/<group>/` contains per-batch JSONL records for image failures, including source URL and error details when a failure occurred. These are operational records, not train/test labels.

The configuration named `train` is an ingestion grouping for Hugging Face loading, **not a curated or leakage-safe training split**. Define and record task-specific train, validation and test splits before fitting a model.

## Processing and use

The uploader reads source rows in sorted CSV filename order and preserves their original fields. For `photo` and `photo_after`, it requests image URLs from the Traffy public storage bucket, checks response content type and size, verifies image decoding, then embeds successful image bytes without intentionally creating replacement images. An empty URL produces a null image with `no_url`; a failed request or validation produces a null image with `failed` and a failure-log entry. Successful source values and availability may vary over time. The uploader writes a Parquet shard, manifest, failure log where needed, and checkpoint together in a Hub commit; it verifies the committed shard before removing the local staged copy.

Potential research uses include Thai civic-report classification, image classification, multimodal retrieval, and analysis of recorded agency associations. These are candidate tasks, not pre-validated benchmark labels. Prior to submission-time prediction, exclude later fields such as `image_after`, final status, later organization actions and resolution timestamps from inputs; establish when each intended target was recorded. Unfinished reports require special handling in resolution-time studies.

For a reproducible read, pin a revision and specify the shard glob. The repository is private, so authenticate with a token that has access:

```python
from datasets import load_dataset

repo = "rocketrat/bkk-traffy-images"
revision = "0e309689ad57bedccd5ff5ae95f2b2a4a9f98f8d"
files = f"hf://datasets/{repo}@{revision}/data/*/*.parquet"

reports = load_dataset(
    "parquet", data_files=files, split="train", streaming=True, token=True
)
example = next(iter(reports))
print(example["ticket_id"], example["source_file"], example["comment"])
# example["image_before"] and example["image_after"] are image columns.
```

Loading a private dataset requires an authenticated Hugging Face session. Streaming avoids downloading the complete collection at once, but image access still transfers data. The wildcard and dataset viewer behavior should be verified against the client version in use. Pin exact dataset, model and preprocessing versions for research.

## Limitations, privacy and responsible use

- Reports reflect who used the reporting system and administrative processes; they are not a representative measure of all residents or all local problems. Coverage, categories and reporting practices can change across time and location.
- Reports and labels may be noisy, incomplete, duplicated, subjective, disputed or stale. Image availability may be selective, and image-download failures can bias a training sample. No independently curated train/test split or expert label validation is provided.
- Text, precise coordinates, addresses and images may reveal personal or sensitive information, including faces, license plates or private property. Review access, minimize retained features, and consider redaction before research sharing or publication. The dataset has not been verified as anonymized.
- Automated issue or agency suggestions can misroute a report. Evaluate per-category and per-area performance, uncertainty and missing-image cases; use appropriate human review and do not represent predictions as official agency decisions.
- Avoid leakage: split before fitting preprocessing or building a retrieval index, keep duplicate or related tickets and translated versions together, and restrict retrieval to material legitimately available at prediction time. An image taken after resolution is not a submission-time input.
- This collection primarily reflects Bangkok and Thai-language reporting. It does not establish performance in other languages, cities or jurisdictions.

## Maintenance and contact

Maintained as a research dataset at [rocketrat/bkk-traffy-images](https://huggingface.co/datasets/rocketrat/bkk-traffy-images). Pipeline issues can be documented in the [project repository](https://github.com/MaanAlaulaqi/bkk-traffy-images). No public maintenance schedule, benchmark or dedicated contact address has been established.
