# GPU production setup (176.9.98.94)

Production PDF workers run as **`smoldocling`** at `/home/smoldocling/apps/pdf`. Ollama embeddings run under **`marc`** on the same host.

Laptop deploy scripts live in **`coolify-provisioning/`** (sibling repo): `gpu-setup-production.sh`, `gpu-deploy-worker.sh`, `gpu-sync-nats-env.sh`.

## Architecture

| Component | User | Process | NATS |
|-----------|------|---------|------|
| Docling worker | `smoldocling` | `docling_worker.py` | `docs.process.*` → `docs.result` |
| GLiNER infer | `smoldocling` | `kg_gliner_worker.py` | `kg.infer` (request/reply) |
| Ollama (bge-m3) | `marc` | Ollama | HTTP `:16942` (platform embed) |

Docling production venv: **`venv/`** — pinned `docling>=2.96.0,<2.97.0` in `requirements.txt` (pass-1 default `fast_text_tables`).

Pass-1 requests from the platform always include `docling_options` from `DOCLING_PARSE_MODE` (default `fast_text_tables`). Pass-2 enrichment is queued by the platform when `FEATURE_MULTISTAGE_INDEXING=1` and the document profile flags OCR/VLM/table follow-up.

## Systemd user units (production)

Both workers use **user systemd** with `Restart=always`. Linger must be enabled so units survive logout/reboot.

| Unit | Log file |
|------|----------|
| `smoldocling-docling-worker.service` | `worker.log` |
| `smoldocling-kg-gliner-worker.service` | `kg_gliner.log` |

Unit files: `infrastructure/systemd/`. Install with `scripts/install_systemd_services.sh` or one-shot `scripts/setup_production_services.sh`.

### One-time migrate (from laptop)

```bash
cd coolify-provisioning
./gpu-setup-production.sh
```

This enables linger, git-pulls this repo on GPU, installs units, stops legacy nohup/tmux workers, and starts systemd.

### Routine deploy (from laptop)

```bash
cd coolify-provisioning
./gpu-sync-nats-env.sh          # when NATS URL/token change
./gpu-deploy-worker.sh            # git pull + pip + restart via systemd
```

### On GPU (smoldocling)

```bash
export XDG_RUNTIME_DIR=/run/user/$(id -u)

systemctl --user status smoldocling-docling-worker smoldocling-kg-gliner-worker
systemctl --user restart smoldocling-docling-worker
tail -f ~/apps/pdf/worker.log
./status_worker.sh
```

Legacy **`pdf-docling-worker.service`** (from old `deploy_worker.sh`) is removed on migrate.

## Docling version

Production **`venv/`** runs Docling **2.96.x** (same pin as `requirements.txt`). Benchmark scripts may reuse `venv-benchmark/` but production no longer stays on 2.42.

### Isolated venvs (optional Nemotron / experiments — not production NATS)

Production **`venv/`** is the NATS worker. Optional isolated venvs:

| Venv | Purpose | Setup script |
|------|---------|--------------|
| `venv-benchmark/` | Docling ≥2.43 smoke + parser benchmarks | `scripts/setup_isolated_benchmark_env.sh` |
| `venv-benchmark/` + Nemotron OCR | Docling-native Nemotron OCR (pass 2) | `scripts/setup_docling_nemotron_ocr.sh` |
| `venv-nemotron/` | Legacy standalone HF package (optional fallback) | `scripts/setup_nemotron_gpu.sh` |

### Docling upgrade candidate (isolated)

```bash
cd ~/apps/pdf
./scripts/setup_isolated_benchmark_env.sh

DOCLING_GPU_PROFILE=capped_5gb ./venv-benchmark/bin/python \
  scripts/docling_capability_smoke.py --pdf tests/fixtures/minimal.pdf --mode fast_text

./venv-benchmark/bin/python scripts/parser_benchmark.py \
  --pdf tests/fixtures/minimal.pdf --mode fast_text --output /tmp/parser-bench
```

Do **not** point `smoldocling-docling-worker.service` at `venv-benchmark` until strategy decision and production pin change.

### Docling-native Nemotron OCR (pass 2 enrichment)

Preferred over standalone `venv-nemotron` — uses `NemotronOcrOptions` inside Docling ([GTC integration](https://www.docling.ai/blog/20260311_00_docling_at_gtc/)):

```bash
cd ~/apps/pdf
./scripts/setup_isolated_benchmark_env.sh
./scripts/setup_docling_nemotron_ocr.sh

DOCLING_GPU_PROFILE=capped_5gb ./venv-benchmark/bin/python \
  scripts/docling_capability_smoke.py --pdf tests/fixtures/minimal.pdf --mode nemotron_enrich
```

Platform/worker: send parse mode `nemotron_enrich` or `ocr_engine=nemotron` in docling options when enrichment detection flags OCR need.

### Legacy standalone Nemotron (optional fallback)

Requires **Python 3.12**, CUDA toolkit on PATH for the C++ extension build:

```bash
cd ~/apps/pdf
./scripts/setup_nemotron_gpu.sh

./venv-nemotron/bin/python scripts/nemotron_smoke_test.py \
  --pdf tests/fixtures/minimal.pdf --pages 0
```

Platform default enrichment backend is `nemotron` (`ct-platform` `ENRICHMENT_BACKEND`); GPU wrapper is `nemotron_service.py` (standalone `venv-nemotron`) until Docling ships `NemotronOcrOptions` on PyPI.

## Environment

Copy `environment_config.txt` → `.env`. Sync from laptop:

```bash
cd coolify-provisioning
./gpu-sync-nats-env.sh --restart
./gpu-sync-loki-env.sh --restart   # optional Loki push
```

Key vars: `NATS_URL`, `NATS_TOKEN`, `DOCLING_GPU_PROFILE` (production unit sets `20gb_capped`).

### Shared GPU memory (Docling + Ollama)

Static budget — no runtime probing required for ops, but worker applies an Ollama reserve gate before CUDA:

| Var | Default | Purpose |
|-----|---------|---------|
| `DOCLING_GPU_CAP_GB` | `8` | PyTorch hard cap for Docling CUDA |
| `DOCLING_OLLAMA_RESERVE_GB` | `12` | Minimum VRAM left for Ollama on 20GB card |
| `DOCLING_CPU_NUM_THREADS` | `8` | CPU fallback / reserve path |
| `OMP_NUM_THREADS` | `8` | Host threads when on CPU |
| `DOCLING_ACCELERATOR_PREFERENCE` | `auto` | `auto` \| `cpu` \| `cuda` |

Behavior:

1. Try CUDA only when `used + cold_load` fits within Ollama reserve.
2. `ThreadedPdfPipelineOptions` batch sizes default to **1** (layout/OCR/table/queue).
3. Converter cache per device — no per-job GPU cleanup on success.
4. On CUDA OOM/cuDNN → one CPU retry (`device_reason=oom_retry`).

Emergency: `DOCLING_ACCELERATOR_PREFERENCE=cpu` (Docling never touches GPU).

## Troubleshooting

| Symptom | Check |
|---------|--------|
| `Failed to connect to bus: No medium found` | Enable linger; use `XDG_RUNTIME_DIR=/run/user/1003` |
| Worker not running after reboot | `loginctl show-user smoldocling -p Linger` → `yes` |
| Duplicate workers | `./scripts/setup_production_services.sh` stops nohup PIDs before systemd start |
| `nats: maximum payload exceeded` | Ensure `main` includes S3 spill for large results (`result_publish.py`) |
| Ollama bge-m3 NaN | `OLLAMA_FLASH_ATTENTION=false` on marc Ollama (`coolify-provisioning/gpu-fix-ollama-bge-m3-nan.sh`) |
| cuDNN / GPU OOM during parse | Worker retries once on CPU (`oom_retry`); check `device_reason` in `docs.result`; tune `DOCLING_GPU_CAP_GB` / Ollama model size |
| GPU git pull blocked by local edits | `gpu-deploy-worker.sh --reset` from laptop or stash on GPU |

## SSH access

```bash
ssh gpu   # root; Host gpu in ~/.ssh/config → 176.9.98.94
sudo -u smoldocling bash -lc 'cd ~/apps/pdf && git status'
```
