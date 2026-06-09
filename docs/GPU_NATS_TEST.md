# GPU pdf-test environment (ct-nats-test)

Separate from **production** `~/apps/pdf` (broker `89.167.15.10:4222`, branch `main`).

| | Production | Test (`pdf-test`) |
|--|------------|-------------------|
| Directory | `~/apps/pdf` | `~/apps/pdf-test` |
| NATS | `89.167.15.10:4222` | `89.167.15.10:4223` (`ct-nats-test`) |
| systemd parse | `smoldocling-docling-worker` | `smoldocling-docling-worker-test` |
| systemd chunk | `smoldocling-docling-chunk-worker` | `smoldocling-docling-chunk-worker-test` |
| Log parse | `worker.log` | `worker-test.log` |
| Log chunk | `chunk-worker.log` | `chunk-worker-test.log` |
| GPU profile | `full` | `20gb_nats` (coexists with prod + Ollama) |

Laptop and Docker E2E use `.env.nats.test` (port **4223**, Mac-reachable).

## Sync model (git only)

GPU `pdf-test` tracks **remote branches** — no rsync, no agent commits on `pdf`.

1. You commit and push `climateandtech/pdf` (any branch).
2. From laptop: `./gpu-deploy-pdf-test.sh --branch <that-branch>`.

Prod (`~/apps/pdf`, `:4222`) and test (`~/apps/pdf-test`, `:4223`) stay independent.

**Hierarchical chunking** uses NATS `docs.chunk.*`: parse worker uploads `parsed/{request_id}/docling.json` to S3, chunk worker (CPU) publishes `docs.result.*`. Deploy restarts parse + chunk units.

## One-time setup (from laptop)

```bash
cd coolify-provisioning
source .env && source .env.platform
./setup-nats-test.sh          # if ct-nats-test not running
./gpu-pdf-test-env.sh --branch main   # clone pdf-test, venv, .env, test worker
```

Push test-worker/systemd changes to `pdf` first, then run the above (or `gpu-deploy-pdf-test.sh`).

## Deploy any branch (after push)

```bash
# in pdf repo: git push origin feat/my-parse-mode
./gpu-deploy-pdf-test.sh --branch feat/my-parse-mode
./gpu-deploy-pdf-test.sh --branch feat/my-parse-mode --reset   # discard GPU local edits
```

## Sync NATS + S3 env only

```bash
./gpu-sync-nats-test-env.sh --restart
```

S3 credentials copied from `ct-platform/backend/.env`.

### S3 (separate test bucket)

Production uploads use `uniformly-entail-expedited`. Test stack uses **`ct-storage-test`** (or your name in `.env.s3.test`):

```bash
cp .env.s3.test.example .env.s3.test
./setup-s3-test-env.sh --create --sync-gpu
```

Never point pdf-test at the production bucket.

## Platform Docker E2E (same broker as test worker)

```bash
cd ct-platform
E2E_NATS_TEST=1 ./scripts/docker-nats-ingest-e2e.sh
```

## NATS stream (one-time per broker)

Add `docs.chunk.*` to the DOCUMENTS stream (prod `:4222` or test `:4223`):

```bash
# test broker — set NATS_URL/NATS_TOKEN in pdf-test .env or export from coolify .env.nats.test
cd ~/apps/pdf-test && source .env && source venv/bin/activate
python scripts/ensure_documents_stream.py
```

## E2E smoke (test broker)

On GPU `pdf-test` (or laptop with same `.env` + S3 test bucket):

```bash
cd ~/apps/pdf-test
source .env && source venv/bin/activate
python scripts/gpu_nats_chunk_e2e_smoke.py tests/fixtures/minimal.pdf
python scripts/gpu_nats_chunk_e2e_smoke.py tests/fixtures/minimal.pdf --hierarchical
```

## On GPU

```bash
ssh gpu
sudo -u smoldocling bash -lc 'systemctl --user status smoldocling-docling-worker-test smoldocling-docling-chunk-worker-test'
tail -f ~/apps/pdf-test/worker-test.log
tail -f ~/apps/pdf-test/chunk-worker-test.log
```

Install/restart test units after git deploy:

```bash
cd ~/apps/pdf-test && ./scripts/install_systemd_test_workers.sh
systemctl --user restart smoldocling-docling-worker-test smoldocling-docling-chunk-worker-test
```

From laptop (`coolify-provisioning/`): `./gpu-workers.sh status test`, `./gpu-deploy-pdf-test.sh --branch <branch>`
