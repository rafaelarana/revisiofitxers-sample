# RevisioFitxers â document classification sample (Databricks)

Sample application that classifies the documents of a *case file* (expedient) automatically using
**Databricks AI Functions**. A user uploads documents (a ZIP or individual files); the platform
unpacks and normalizes them, classifies each into a fixed set of categories, extracts key metadata,
and shows the results in a multi-user app. Everything runs inside Databricks â documents never leave
the platform.

> ## â ï¸ Sample code â not production-ready
> This repository is **sample / illustrative code** meant to demonstrate an end-to-end pattern on
> Databricks. It is **not** a Databricks product, carries no support or SLA, and is **not intended
> for production use** without your own review, security hardening, testing, and error handling.
> See [`LICENSE`](LICENSE).

## What it does

1. **Upload** the documents of a case file (a ZIP or several files: PDF, emails, spreadsheetsâ¦).
2. **Auto Loader** normalizes them in a serverless stream (unzip incl. nested, detach `.eml`
   attachments, extract XLSX text, dedupe by content hash).
3. **AI Functions** classify and enrich: `ai_parse_document` â `ai_classify` â `ai_extract` â
   `ai_gen` (a short summary).
4. Results and lifecycle state land in **Lakebase (Postgres)**; the **app** shows them, with
   per-user scoping, a live processing view, and in-app document preview/download.

Files that can't be read are flagged and their case file moves to a *needs-review* state â nothing
is silently dropped.

## Architecture (event-driven)

```
[App Â· upload] â [UC Volume /landing] â [Auto Loader Â· normalize] â [AI Functions] â [Lakebase Â· state] â [App Â· review]
                                        ââââââââââââ Unity Catalog Â· serverless Â· in-platform models ââââââââââââ
```

- Ingest + classify run as one serverless Lakeflow job; the app triggers it after each upload (a
  `file_arrival` trigger is kept, paused, as a backstop).
- The app is an **AppKit (TypeScript/React)** app on **Databricks Apps**; it reads/writes Lakebase as
  its service principal and writes uploads to the Volume on behalf of the signed-in user.

## Repository layout

```
databricks.yml                 bundle root: variables + the dev target
resources/
  uc.yml                       schema + volume (UC resources)
  pipeline.yml                 ingest + classify job (Auto Loader + AI Functions)
  app.yml                      the AppKit app + its resources (Lakebase, volume, ingest job)
  job.yml                      a batch variant of the pipeline (reference / backfill)
src/
  shared/normalize.py          unzip / eml / xlsx / dedup â one implementation (app + pipeline + tests)
  pipeline/                    Auto Loader ingest + classify/merge into Lakebase
  app/                         AppKit app (server routes + React UI)
  notebooks/                   batch-mode notebooks (reference)
sql/lakebase_schema.sql        state store schema (6 tables) + taxonomy seed
scripts/                       ensure_catalog Â· ensure_lakebase Â· grant_app_access (bootstrap)
setup.sh                       one-command, from-zero deploy
tests/                         pytest for the shared normalize module
```

## Dependencies

- **Databricks workspace** with: **serverless compute**, **AI Functions** (`ai_parse_document`,
  `ai_classify`, `ai_extract`, `ai_gen`), **Databricks Apps**, and **Lakebase** available in your
  region.
- **Databricks CLI** â¥ 0.240 (bundles + apps) â https://docs.databricks.com/dev-tools/cli
- **Python 3.10+** locally â `setup.sh` creates a venv and installs the bootstrap deps:
  `databricks-sdk>=0.81`, `psycopg[binary]`, `openpyxl`.
- **Node.js 18+** for the AppKit app (the app builds on the app compute at deploy time via
  `npm install`; you do not need to build it locally).

## Deploy from scratch

> 📘 **Full step-by-step guide** (prerequisites, verification, troubleshooting, teardown): see [`DEPLOY.md`](DEPLOY.md).

```bash
databricks auth login --profile <your-profile>            # once, against your workspace
CATALOG=<your_catalog> LAKEBASE_PROJECT=<your_project> \
  ./setup.sh <your-profile> dev
```

`setup.sh` is idempotent and does everything end to end: create/verify the Unity Catalog catalog â
create/verify the Lakebase project + state schema (and seed the taxonomy) â `bundle deploy` â run
the ingest + classify job â start the app â grant the app service principal read/write on the state
schema â restart the app. It prints the app URL at the end.

If you don't have `CREATE CATALOG` on your metastore, pass an existing catalog with `CATALOG=...`.

Tear down: `databricks bundle destroy -t dev --profile <your-profile>` (removes schema/volume/job/app;
remove the Lakebase project and catalog separately if you want them fully gone).

## Providing your own documents

This sample ships **without any sample documents**. Provide your own:

- **Recommended:** run `setup.sh`, open the app, create a case file and upload a ZIP or files.
- Or drop files into the Volume at `landing/<case-id>/raw/` and run the ingest job.

The 5 classification categories are defined in `sql/lakebase_schema.sql` (`taxonomy_categories`) and
read by both the app and the pipeline â edit them there for your own document types.

## Known limitations (sample scope)

- No human-in-the-loop correction UI, no faceted export to Excel, no semantic search / assistant.
- No PII masking or data-quality expectations wired in.
- Per-user scoping is demo-grade (owner-based); adapt to your own authorization model.
- Cold-start latency: the first classification run in a while can take a few minutes (serverless
  warm-up).
- Regional availability of Lakebase and Databricks Apps varies â confirm for your workspace.

## License

See [`LICENSE`](LICENSE) â Databricks sample-code terms, provided **AS IS**, no warranty, not for
production without your own review and hardening.
