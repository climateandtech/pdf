# GPU production setup (176.9.98.94)

Production PDF workers run as **`smoldocling`** at `/home/smoldocling/apps/pdf`. Ollama embeddings run under **`marc`** on the same host.

## Architecture

| Component | User | Process | NATS |
|-----------|------|---------|------|
| Docling worker | `smoldocling` | `docling_worker.py` | `docs.process.*` → `docs.result` |
| GLiNER infer | `smoldocling` | `kg_gliner_worker.py` | `kg.infer` (request/reply) |
| Ollama (bge-m3) | `marc` | Ollama | HTTP `:16942` (platform embed) |

Docling production venv: **`venv/`** — pinned `docling>=2.42.0,<2.43` in `requirements.txt`.

## Systemd user units (production)

Both workers use **user systemd** with `Restart=always`. Linger must be enabled so units survive logout/reboot.

| Unit | Log file |
|------|----------|
| `smoldocling-docling-worker.service` | `worker.log` |
| `smoldocling-kg-gliner-worker.service` | `kg_gliner.log` |

Unit files live in `infrastructure/systemd/` and are installed by `scripts/install_systemd_services.sh`.

### One-time migrate (laptop)

From `coolify-provisioning/`:

```bash
./gpu-setup-production.sh
```

This:

1. Runs `loginctl enable-linger smoldocling` (root)
2. Runs `gpu-deploy-worker.sh --no-restart` (git pull, `pip install`, unit tests)
3. Runs `scripts/setup_production_services.sh` on GPU (install units, stop legacy nohup/tmux, start systemd)

### Routine deploy (laptop)

```bash
cd coolify-provisioning
./gpu-sync-nats-env.sh          # when NATS URL/token change
./gpu-deploy-worker.sh            # git pull + pip + restart via systemd
```

### On GPU (smoldocling)

Use `XDG_RUNTIME_DIR` when SSH does not provide a user bus:

```bash
export XDG_RUNTIME_DIR=/run/user/$(id -u)

systemctl --user status smoldocling-docling-worker smoldocling-kg-gliner-worker
systemctl --user restart smoldocling-docling-worker
tail -f ~/apps/pdf/worker.log
./status_worker.sh
```

Legacy **`pdf-docling-worker.service`** (from old `deploy_worker.sh`) is removed on migrate.

## Isolated venvs (benchmark / enrichment — not production NATS)

Production **`venv/`** stays on Docling 2.42 until benchmarks pass. Upgrade candidates use separate venvs:

| Venv | Purpose | Setup script |
|------|---------|--------------|
| `venv-benchmark/` | Docling ≥2.43 smoke + parser benchmarks | `scripts/setup_isolated_benchmark_env.sh` |
| `venv-nemotron/` | Nemotron OCR v2 enrichment (Python 3.12) | `scripts/setup_nemotron_gpu.sh` |

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

### Nemotron OCR v2 (isolated)

Requires **Python 3.12**, CUDA toolkit on PATH for the C++ extension build:

```bash
cd ~/apps/pdf
./scripts/setup_nemotron_gpu.sh

./venv-nemotron/bin/python scripts/nemotron_smoke_test.py \
  --pdf tests/fixtures/minimal.pdf --pages 0
```

Platform default enrichment backend is `nemotron` (see `ct-platform` `ENRICHMENT_BACKEND`); GPU worker wiring for pass-2 enrichment is separate from the NATS Docling worker.

## Environment

Copy `environment_config.txt` → `.env`. Sync from laptop:

```bash
cd coolify-provisioning
./gpu-sync-nats-env.sh --restart
./gpu-sync-loki-env.sh --restart   # optional Loki push
```

Key vars: `NATS_URL`, `NATS_TOKEN`, `DOCLING_GPU_PROFILE` (production unit sets `full` → `20gb_nats` profile).

## Troubleshooting

| Symptom | Check |
|---------|--------|
| `Failed to connect to bus: No medium found` | Enable linger; use `XDG_RUNTIME_DIR=/run/user/1003` |
| Worker not running after reboot | `loginctl show-user smoldocling -p Linger` → `yes` |
| Duplicate workers | `./setup_production_services.sh` stops nohup PIDs before systemd start |
| `nats: maximum payload exceeded` | Ensure pdf `main` includes S3 spill for large results (`result_publish.py`) |
| Ollama bge-m3 NaN | `OLLAMA_FLASH_ATTENTION=false` on marc Ollama (see `gpu-fix-ollama-bge-m3-nan.sh`) |

## Related docs

- Laptop ops: `coolify-provisioning/GPU-DEPLOY.md`
- Indexing throughput plan: multistage pipeline, parser/chunker benchmarks in `ct-platform`
