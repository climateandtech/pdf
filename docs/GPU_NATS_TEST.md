# GPU pdf-test environment (ct-nats-test)

Separate from **production** `~/apps/pdf` (broker `89.167.15.10:4222`, branch `main`).

| | Production | Test (`pdf-test`) |
|--|------------|-------------------|
| Directory | `~/apps/pdf` | `~/apps/pdf-test` |
| NATS | `89.167.15.10:4222` | `89.167.15.10:4223` (`ct-nats-test`) |
| systemd | `smoldocling-docling-worker` | `smoldocling-docling-worker-test` |
| Log | `worker.log` | `worker-test.log` |
| GPU profile | `full` | `20gb_nats` (coexists with prod + Ollama) |

Laptop and Docker E2E use `.env.nats.test` (port **4223**, Mac-reachable).

## Sync model (git only)

GPU `pdf-test` tracks **remote branches** — no rsync, no agent commits on `pdf`.

1. You commit and push `climateandtech/pdf` (any branch).
2. From laptop: `./gpu-deploy-pdf-test.sh --branch <that-branch>`.

Prod (`~/apps/pdf`, `:4222`) and test (`~/apps/pdf-test`, `:4223`) stay independent; only test worker restarts on deploy.

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

## On GPU

```bash
ssh gpu
sudo -u smoldocling bash -lc 'systemctl --user status smoldocling-docling-worker-test'
tail -f ~/apps/pdf-test/worker-test.log
```

Stop test worker only: `~/apps/pdf-test/scripts/stop_worker_test.sh`

From laptop (`coolify-provisioning/`): `./gpu-workers.sh status test`, `./gpu-workers.sh restart test`, `./gpu-workers.sh logs test -f`
