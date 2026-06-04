# Parser benchmark + registry testing

Three tiers for this feature area:

## 1. Unit (`tests/test_parse_modes.py`, `tests/test_parser_registry.py`, `tests/test_parser_benchmark_contract.py`)

Fast, no GPU. Covers:

- `fast_text_tables` preset options
- `manifest.jsonl` append-only registry contract
- `write_run` / `dataset_meta.json` artifact shape

```bash
cd pdf
python3 -m pip install -r tests/requirements-test.txt
python3 -m pytest tests/test_parse_modes.py tests/test_parser_registry.py tests/test_parser_benchmark_contract.py -q
```

## 2. Functional (GPU host, mocked transport N/A)

Run on GPU with `venv-benchmark`:

```bash
DOCLING_GPU_PROFILE=capped_5gb ./venv-benchmark/bin/python scripts/parser_benchmark.py \
  --pdf tests/fixtures/testpdf.pdf \
  --modes fast_text_tables,fast_text,standard \
  --run-id local-smoke
```

Verifies Docling integration and appends rows to `benchmarks/parser/registry/manifest.jsonl`.

## 3. E2E (opt-in)

Full NATS worker round trip — existing pdf e2e suites; not required for registry schema changes.

## Research dataset

- Flat table: `benchmarks/parser/registry/manifest.jsonl`
- Per-run artifacts: `benchmarks/parser/registry/runs/<run_id>/`
- Schema: `benchmarks/parser/registry/schema/ct-parser-benchmark-v1.json`

## Quality gate

```bash
cd pdf && QG_STRICT=1 ./scripts/quality-gate.sh quick
```

Maps changed modules to unit tests and runs ruff on touched Python files.
