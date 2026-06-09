# Laptop ops (pdf repo)

Deploy and test GPU workers from your laptop using scripts in this repo.  
Coolify-provisioning wrappers (`gpu-deploy-worker.sh`, `gpu-deploy-pdf-test.sh`) may delegate here.

## Prerequisites

- `ssh gpu` works (Hetzner GPU host)
- `smoldocling` user with `~/apps/pdf` (prod) and optionally `~/apps/pdf-test` (test)
- Test `.env` on GPU: NATS `:4223` + test S3 bucket (see [GPU_NATS_TEST.md](GPU_NATS_TEST.md))

## Deploy (after `git push`)

```bash
cd pdf

# Production (:4222) — parse + chunk + kg-gliner
./scripts/laptop_deploy_prod.sh

# Test broker (:4223)
./scripts/laptop_deploy_test.sh --branch main
```

Options: `--no-restart`, `--reset` (discard GPU local edits).

## Worker control

```bash
./scripts/laptop_workers.sh status
./scripts/laptop_workers.sh status test
./scripts/laptop_workers.sh restart prod
./scripts/laptop_workers.sh logs test-chunk -n 80
```

## NATS test E2E (parse → chunk → result)

```bash
./scripts/laptop_deploy_test.sh --branch main
./scripts/laptop_test_nats_e2e.sh
```

On GPU directly:

```bash
cd ~/apps/pdf-test && ./scripts/run_test_nats_e2e.sh
```

## JetStream subjects

`docs.chunk.*` must be on the DOCUMENTS stream. Deploy scripts run `ensure_documents_stream.py` automatically.

Manual verify:

```bash
./scripts/reset_jetstream_test.sh --verify-only   # test :4223
./scripts/reset_jetstream.sh --verify-only        # prod :4222
```

## Full platform round trip (chunks + embed in DB)

GPU smokes prove **parse + HybridChunker tiers** on NATS. End-to-end **Postgres CHUNK rows + embeddings** runs on **ct-platform**:

```bash
cd ct-platform
E2E_NATS_TEST=1 DOCLING_CHUNKER=hierarchical_hybrid ./scripts/docker-nats-ingest-e2e.sh
```

Requires: test workers on `:4223`, platform test DB/Celery, Ollama embed on GPU.
