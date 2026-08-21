# VM Migration Playbook

Purpose: move the VN Strategy Data System from the local desktop to a company VM with minimal downtime and a safe rollback path.

This playbook is based on lessons from the Steam data-system migration. The main principle is simple: local stays production until the VM proves it can run the API, ETL, cron, and reports successfully.

## Migration Pattern

1. Keep the local desktop as production.
2. Build the VM as a shadow environment.
3. Seed the VM from local database/files.
4. Validate row parity and key answers.
5. Run one or more delta catch-ups.
6. Start the VM API/tunnel.
7. Enable VM cron.
8. Run local and VM in parallel briefly.
9. Disable local cron only after VM scheduled jobs succeed.

## Non-Negotiable Rules

- Do not disable local cron before VM cron succeeds.
- Do not rely only on API `/health`; the API can be healthy while ETL is broken.
- Do not run duplicate heavy SensorTower crawlers from local and VM for long.
- Do not commit `.env`, `.venv`, raw data, logs, API keys, MCP tokens, or ngrok secrets.
- Do not wipe unrelated cron entries; only remove the managed VN Strategy block.
- Do not migrate the Top Apps Facets API adapter into production until SensorTower confirms pagination behavior.

## Local Preparation

Run from local WSL:

```bash
cd /mnt/d/Coding/vn_strategy_data_system
git status
bash -n scripts/ops/*.sh
bash scripts/ops/check_production_health.sh
```

Before migration, make sure the repo has:

- committed code changes
- clean or understood git status
- working local Docker stack
- latest successful weekly pipeline run
- latest successful daily/weekly report delivery
- current `.env.example` templates

## VM Bootstrap

On the VM:

```bash
cd /opt
git clone <repo-url> vn_strategy_data_system
cd /opt/vn_strategy_data_system
```

Create local secrets from templates:

```bash
cp .env.example .env
cp modeling_layer/.env.example modeling_layer/.env
```

Fill the VM `.env` files manually. Do not copy secrets into Git or shared agent instructions.

## Python Runtime

Use the same Python version as local unless the project is explicitly upgraded.

If the VM package manager does not provide the required version, use `uv`:

```bash
cd /opt/vn_strategy_data_system

sudo apt install -y curl
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.local/bin/env

uv python install 3.12
uv venv --python 3.12 .venv

source .venv/bin/activate
uv pip install -r requirements.txt
python -c "import pandas; print('deps ok')"
```

## Start VM Services

```bash
cd /opt/vn_strategy_data_system/modeling_layer
docker compose -p vn_strategy_data_system up -d postgres api
docker compose -p vn_strategy_data_system ps
curl -fsS http://127.0.0.1:8001/health
```

If MCP/ngrok is enabled on the VM, start them only after API and DB validation pass.

## Seed Bundle

The seed should contain:

- Postgres custom dump
- required `data/base` current stores
- required `data/staging` current outputs
- required `manual/*_current.csv` mappings
- manifest with file sizes and sha256 hashes

The seed should exclude:

- `.env`
- `.venv`
- logs
- tmp folders
- bulky raw archives unless required for audit/rebuild

Target scripts to implement if not already present:

```bash
bash scripts/ops/export_vm_seed_bundle.sh
bash scripts/ops/restore_vm_seed_bundle.sh --confirm-vm-restore <dump> <archive>
```

Implemented local scripts:

```bash
# Local export
cd /mnt/d/Coding/vn_strategy_data_system
bash scripts/ops/export_vm_seed_bundle.sh

# VM restore after rsync transfer
cd /opt/vn_strategy_data_system
bash scripts/ops/restore_vm_seed_bundle.sh \
  --confirm-vm-restore data/tmp/vm_seed/<bundle-id>
```

The seed bundle is written under `data/tmp/vm_seed/<bundle-id>/` and includes:

- `postgres.dump`
- `filesystem_seed.tar.gz`
- `filesystem_seed_files.txt`
- `manifest.json`

## Delta Catch-Up

After the first seed, local may continue ingesting new data. Use delta bundles before cutover.

Important: delta selection should use load timestamp, not metric date. Historical backfills may have old metric dates but new load times.

For SensorTower, confirm every fact/dimension table has a reliable load timestamp before using this pattern. If a table lacks one, add a metadata field or use run artifact timestamp tracking before VM migration.

Target scripts to implement:

```bash
bash scripts/ops/export_vm_delta_bundle.sh --since <loaded_at-cutoff>
bash scripts/ops/restore_vm_delta_bundle.sh --confirm-vm-delta-restore <bundle-dir>
```

Current implementation status:

- `export_vm_delta_bundle.sh` exists but intentionally exits with a clear blocked status.
- `restore_vm_delta_bundle.sh` exists but intentionally exits with a clear blocked status.
- Reason: the current SensorTower core schema does not yet define reliable `loaded_at` metadata across all large fact tables.
- Do not use metric dates for VM deltas; add load metadata first.

Recommended delta contents:

- latest small dimension/current mapping tables
- fact rows where `loaded_at >= cutoff`
- current pointer/manifest files
- no giant historical raw archive

## Transfer To VM

If hostname does not resolve, use VM IP and SSH port:

```bash
ssh -p <port> user@vm-ip "mkdir -p /opt/vn_strategy_data_system/data/tmp/vm_seed/<bundle-id>"

rsync -avh --progress -e "ssh -p <port>" \
  data/tmp/vm_seed/<bundle-id>/ \
  user@vm-ip:/opt/vn_strategy_data_system/data/tmp/vm_seed/<bundle-id>/
```

If the VM lacks `rsync`:

```bash
sudo apt install -y rsync
```

## Validation

Validation must include both service health and data correctness.

Run on VM:

```bash
cd /opt/vn_strategy_data_system
bash scripts/ops/check_production_health.sh
```

Run row-count validation locally and on VM:

```bash
cd /opt/vn_strategy_data_system/modeling_layer
docker exec -i vn-strategy-modeling-postgres psql \
  -U postgres \
  -d mydb \
  -f /sql/vm_validation_counts.sql
```

Implemented validation SQL:

```text
modeling_layer/sql/vm_validation_counts.sql
```

Target validation query pattern:

```sql
select
  'core.fact_app_performance_daily' as object_name,
  count(*) as rows,
  count(distinct app_id) as apps,
  min(date)::date as min_date,
  max(date)::date as max_date
from core.fact_app_performance_daily
union all
select
  'core.fact_app_performance_active_users',
  count(*),
  count(distinct app_id),
  min(date)::date,
  max(date)::date
from core.fact_app_performance_active_users;
```

Expected after final delta:

- row counts match or differences are explained
- max dates match
- important VN/CN/WW smoke queries match
- API `/health` returns OK
- `/meta/catalog` returns expected objects
- MCP health/tool discovery works if MCP is enabled

## VM ETL Validation

Do not cut over after restore alone.

Run one manual VM ETL job:

```bash
cd /opt/vn_strategy_data_system
bash scripts/ops/run_weekly_pipeline.sh
```

Then confirm:

- run log exists under `data/logs/cron/<run_id>/`
- `status.json` is `success`
- current manifests/max dates advanced as expected
- modeling incremental load succeeded
- analytics refresh succeeded
- API restarted cleanly

Only after that, wait for one real cron-triggered VM job to succeed.

## API And Tunnel Cutover

Start VM public services:

```bash
cd /opt/vn_strategy_data_system/modeling_layer
docker compose -p vn_strategy_data_system up -d api ngrok
docker logs --tail 80 vn-strategy-modeling-ngrok
curl -fsS https://<vm-public-url>/health
```

Test with a real read-only API key:

```bash
curl -fsS \
  -H "X-API-Key: <vm-api-key>" \
  "https://<vm-public-url>/meta/catalog"
```

Update the chatbot/agent base URL only after VM API and VM ETL validation pass.

## Cron Cutover

Install VM cron:

```bash
cd /opt/vn_strategy_data_system
bash scripts/ops/install_cron_jobs.sh
crontab -l
sudo service cron status
```

Expected managed jobs:

- weekly ETL: Monday 03:00 GMT+7
- quarterly ETL: day 2 of Mar/Jun/Sep/Dec at 00:00 GMT+7
- daily report: 08:30 GMT+7
- weekly report: Monday 10:30 GMT+7

Keep local cron enabled until the first VM scheduled ETL succeeds.

## Disable Local Cron Safely

After VM cron succeeds, back up local cron and remove only the VN Strategy block:

```bash
crontab -l > /tmp/local_cron_before_vn_strategy_cutover.bak

crontab -l 2>/dev/null \
  | sed '/# BEGIN VN_STRATEGY_DATA_SYSTEM/,/# END VN_STRATEGY_DATA_SYSTEM/d' \
  | crontab -
```

Keep local API/tunnel alive for one business day as rollback, then shut it down.

## Rollback

Rollback is valid if:

- local DB and API are still available
- local cron backup exists
- local public URL can be restored

Rollback steps:

```bash
crontab /tmp/local_cron_before_vn_strategy_cutover.bak
cd /mnt/d/Coding/vn_strategy_data_system/modeling_layer
docker compose -p vn_modeling_clean up -d postgres api ngrok
curl -fsS https://<local-public-url>/health
```

Then switch the chatbot/agent base URL back to local.

## Cutover Success Definition

Migration is complete when:

- VM DB row counts match local after final delta.
- VM API returns correct answers for key smoke tests.
- VM cron has completed at least one scheduled ETL run.
- Daily/weekly reports are delivered from VM.
- Chatbot/MCP use the VM public URL.
- Local cron is disabled.
- Local API remains available only as temporary rollback.
