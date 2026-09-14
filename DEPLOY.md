# How to deploy — RevisioFitxers (sample)

Step-by-step guide to deploy this sample end to end into a Databricks workspace. It deploys a
Databricks Asset Bundle (schema, volume, ingest+classify job, app) plus a Lakebase state store.

> ⚠️ **Sample code — not production-ready.** Review, harden, and test before any real use. See
> [`LICENSE`](LICENSE).

---

## 1. Prerequisites

**Workspace capabilities** (confirm these are available in your workspace/region):
- **Serverless compute** (the job runs serverless).
- **AI Functions**: `ai_parse_document`, `ai_classify`, `ai_extract`, `ai_gen`.
- **Databricks Apps**.
- **Lakebase** (Databricks-managed Postgres) — the app stores state here.

**Permissions** for the deploying user:
- Create schema / volume / table in a catalog — **or** an existing catalog you can write to.
- `CREATE CATALOG` on the metastore *(optional — if you don't have it, reuse an existing catalog, see step 3)*.
- Create & manage **Databricks Apps**; create a **Lakebase** project; create & run **Jobs**.

**Local tooling:**
- **Databricks CLI ≥ 0.240** — <https://docs.databricks.com/dev-tools/cli>. Check: `databricks --version`.
- **Python 3.10+** — `setup.sh` creates its own virtualenv and installs the bootstrap deps
  (`databricks-sdk>=0.81`, `psycopg[binary]`, `openpyxl`).
- **Node.js is not needed locally** — the app builds on the app compute at deploy time.
- Network access to install the Python bootstrap deps.

---

## 2. Authenticate

```bash
databricks auth login --host https://<your-workspace>.cloud.databricks.com --profile <profile>
```

Verify:

```bash
databricks auth profiles                       # your <profile> should be listed / valid
databricks current-user me --profile <profile> # confirms you're authenticated
```

---

## 3. Deploy (one command)

```bash
CATALOG=<your_catalog> LAKEBASE_PROJECT=<your_project> ./setup.sh <profile> dev
```

- `CATALOG` — the Unity Catalog catalog for the schema/volume/tables. `setup.sh` **creates it if
  missing**; if you lack `CREATE CATALOG`, pass an **existing** catalog you can write to.
- `LAKEBASE_PROJECT` — name for the Lakebase project (created if missing).
- `<profile>` — your CLI profile; `dev` is the bundle target.

`setup.sh` is **idempotent** (safe to re-run) and performs, in order:

1. Create a local venv and install bootstrap deps.
2. **Ensure the catalog** exists (`scripts/ensure_catalog.py`).
3. **Ensure the Lakebase project + state schema**, seeding the taxonomy (`scripts/ensure_lakebase.py`
   + `sql/lakebase_schema.sql`); it discovers the Lakebase host/endpoint.
4. **`bundle deploy`** the schema, volume, ingest+classify job and app.
5. **Run** the ingest+classify job once.
6. **Start the app**, **grant** its service principal read/write on the state schema
   (`scripts/grant_app_access.py`), then **restart** so the app picks up the grant.

It prints the **app URL** at the end.

> `SKIP_RUN=1 ...` deploys without running the job.

---

## 4. Verify

```bash
# App is running:
databricks apps get revisiofitxers-dev --profile <profile> -o json   # app_status.state == RUNNING

# App logs (should show "state store reachable; taxonomy rows: 5" and no ConfigurationError):
databricks apps logs revisiofitxers-dev --profile <profile>
```

Then open the printed **app URL** (it's behind Databricks OAuth), create a case file, and upload a
ZIP or a few files. Status goes `PROCESSING` → `READY` (or `NEEDS_REVIEW` if a file couldn't be read).

---

## 5. Manual / step-by-step (alternative to `setup.sh`)

If you prefer to run each step yourself (`PROFILE`, `CATALOG`, `PROJECT` are yours):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install "databricks-sdk>=0.81" "psycopg[binary]" openpyxl

# 1) catalog
python scripts/ensure_catalog.py --profile $PROFILE --catalog $CATALOG

# 2) lakebase + state schema (writes connection facts to facts.json)
python scripts/ensure_lakebase.py --profile $PROFILE --project $PROJECT --json-out facts.json
LB_HOST=$(python -c "import json;print(json.load(open('facts.json'))['host'])")
LB_ENDPOINT=$(python -c "import json;print(json.load(open('facts.json'))['endpoint'])")
LB_PGDB=$(python -c "import json;print(json.load(open('facts.json'))['pg_database'])")
VARS="catalog=$CATALOG,lakebase_host=$LB_HOST,lakebase_endpoint=$LB_ENDPOINT,lakebase_pg_database=$LB_PGDB"

# 3) deploy + 4) run job
databricks bundle deploy -t dev --profile $PROFILE --var="$VARS"
databricks bundle run revisiofitxers_ingest -t dev --profile $PROFILE --var="$VARS"

# 5) start app, grant SP, restart
databricks bundle run revisiofitxers_app -t dev --profile $PROFILE --var="$VARS"
python scripts/grant_app_access.py --profile $PROFILE --app revisiofitxers-dev \
  --endpoint "$LB_ENDPOINT" --host "$LB_HOST" --pg-database "$LB_PGDB"
databricks bundle run revisiofitxers_app -t dev --profile $PROFILE --var="$VARS"
```

---

## 6. Configuration reference

Bundle variables (in `databricks.yml`; override with `--var="key=value"`):

| Variable | Purpose |
|---|---|
| `catalog` | UC catalog for schema/volume/tables |
| `schema` / `volume` | Managed schema and Volume names |
| `lakebase_host` / `lakebase_endpoint` / `lakebase_pg_database` | Lakebase connection (discovered by `ensure_lakebase.py`) |
| `lakebase_branch` / `lakebase_database` | Lakebase resource paths for the app's Postgres binding |

`setup.sh` env overrides: `CATALOG`, `LAKEBASE_PROJECT`, `SKIP_RUN`.

The 5 classification categories live in `sql/lakebase_schema.sql` (`taxonomy_categories`) and are read
by both the app and the pipeline — edit them there for your own document types.

---

## 7. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `PERMISSION_DENIED` creating a catalog | You lack `CREATE CATALOG` — re-run with `CATALOG=<existing_catalog>`. |
| App shows **502 / crash-loops** on first start | Check `databricks apps logs`. A clean restart after the first deploy lets it pick up the service-principal grant; `setup.sh` does this automatically. |
| App can't read the state schema | The app SP needs `USAGE/SELECT` on the schema — run `scripts/grant_app_access.py`, then restart the app (folded into `setup.sh`). |
| `workspace_id mismatch` on deploy | The profile's stored `workspace_id` is stale — re-run `databricks auth login` for that profile. |
| Job/app can't reach Lakebase | Confirm **Lakebase is available in your region** and the `lakebase_*` vars are set (they come from `ensure_lakebase.py`). |
| First classification is slow (minutes) | Serverless **cold start** — subsequent runs are faster. |

---

## 8. Teardown

```bash
databricks bundle destroy -t dev --profile <profile>   # removes schema, volume, job, app
```

The **Lakebase project** and the **catalog** are not bundle-managed — remove them separately if you
want them fully gone.

> 💡 **Cost note:** the Lakebase project and the running app are **billable** (both scale to zero when
> idle). Tear them down when you're finished evaluating.
