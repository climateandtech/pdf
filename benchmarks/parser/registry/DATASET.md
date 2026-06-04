# Parser benchmark registry (research dataset)

Flat, append-only benchmark records for multistage document indexing experiments.

## Layout

```text
benchmarks/parser/registry/
  manifest.jsonl          # one JSON object per mode result (dataset table)
  DATASET.md              # this file
  schema/ct-parser-benchmark-v1.json
  runs/<run_id>/
    run_config.json
    metrics.json
    summary.md
    dataset_meta.json
```

## Loading in Python

```python
import json
from pathlib import Path

rows = [
    json.loads(line)
    for line in Path("benchmarks/parser/registry/manifest.jsonl").read_text().splitlines()
    if line.strip()
]
```

## Reproducing a run

```bash
cd pdf
DOCLING_GPU_PROFILE=capped_5gb ./venv-benchmark/bin/python scripts/parser_benchmark.py \
  --pdf /path/to/report.pdf \
  --modes fast_text_tables,fast_text,standard,rich \
  --run-id my-run-001
```

Each run appends rows to `manifest.jsonl` and writes full artifacts under `runs/<run_id>/`.

## Key fields (manifest row)

| Field | Meaning |
|-------|---------|
| `pdf_sha256` | Content-addressed PDF fingerprint |
| `mode` | Parse preset (`fast_text_tables`, etc.) |
| `pages_per_min` | `(page_count / elapsed_s) * 60` |
| `pipe_ready_tables` | Tables with more than one cell (structured) |
| `picture_count` | Layout-detected figures |
| `docling_version` | Docling wheel version at run time |
| `gpu_name` | GPU model from nvidia-smi |

## Citation note

When publishing, cite the git commit hash, `manifest.jsonl` row `record_id`s, and PDF SHA-256 values so others can reproduce comparisons.
