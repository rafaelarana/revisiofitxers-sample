#!/usr/bin/env bash
# One-click, from-zero deploy of the RevisioFitxers app + pipeline + Lakebase state store
# into a fresh Databricks workspace.
#
# What it does (all idempotent — safe to re-run):
#   1. Ensure the Unity Catalog catalog exists.
#   2. Ensure the Lakebase (Autoscaling Postgres) project + state schema exist (+ seed taxonomy).
#   3. Deploy the bundle (schema, volume, ingest+classify job, app) with discovered Lakebase config.
#   4. Run the ingest+classify job once (processes anything already in Examples/ or landing/).
#
# Prereqs: `databricks auth login --profile <name>` run once for the target workspace,
# plus python3 (the script manages its own venv for the two bootstrap deps).
#
# Usage:
#   ./setup.sh <profile> [target]
#   CATALOG=my_cat LAKEBASE_PROJECT=revisiofitxers ./setup.sh my-workspace dev
#
# Env overrides:
#   CATALOG           UC catalog for schema/volume/tables         (default: revisiofitxers_sample)
#   LAKEBASE_PROJECT  Lakebase project id                          (default: revisiofitxers)
#   SKIP_RUN=1        Deploy only; don't run the job.
set -euo pipefail

PROFILE="${1:-DEFAULT}"
TARGET="${2:-dev}"
CATALOG="${CATALOG:-revisiofitxers_sample}"
LAKEBASE_PROJECT="${LAKEBASE_PROJECT:-revisiofitxers}"

cd "$(dirname "$0")"

# --- 0) local venv for the bootstrap scripts (databricks-sdk + psycopg) ------------------
VENV=".venv"
if [ ! -x "$VENV/bin/python" ]; then
  echo "== 0/4 Creating local venv for bootstrap deps =="
  python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install --quiet --upgrade pip "databricks-sdk>=0.81" "psycopg[binary]" openpyxl
PY="$VENV/bin/python"

# --- 1) catalog --------------------------------------------------------------------------
echo "== 1/4 Ensuring catalog '$CATALOG' exists (profile=$PROFILE) =="
"$PY" scripts/ensure_catalog.py --profile "$PROFILE" --catalog "$CATALOG"

# --- 2) Lakebase project + schema --------------------------------------------------------
echo "== 2/4 Ensuring Lakebase project '$LAKEBASE_PROJECT' + state schema =="
FACTS_JSON="$(mktemp)"
"$PY" scripts/ensure_lakebase.py --profile "$PROFILE" --project "$LAKEBASE_PROJECT" \
  --json-out "$FACTS_JSON"
LB_HOST="$("$PY" -c "import json;print(json.load(open('$FACTS_JSON'))['host'])")"
LB_ENDPOINT="$("$PY" -c "import json;print(json.load(open('$FACTS_JSON'))['endpoint'])")"
LB_PGDB="$("$PY" -c "import json;print(json.load(open('$FACTS_JSON'))['pg_database'])")"
rm -f "$FACTS_JSON"
echo "   Lakebase host: $LB_HOST"

VARS="catalog=$CATALOG,lakebase_host=$LB_HOST,lakebase_endpoint=$LB_ENDPOINT,lakebase_pg_database=$LB_PGDB"

# --- 3) deploy ---------------------------------------------------------------------------
echo "== 3/4 Deploying bundle (target=$TARGET) =="
databricks bundle deploy -t "$TARGET" --profile "$PROFILE" --var="$VARS"

# --- 4) run job --------------------------------------------------------------------------
if [ "${SKIP_RUN:-0}" = "1" ]; then
  echo "== 4/5 SKIP_RUN=1 — skipping job run =="
else
  echo "== 4/5 Running ingest + classify job =="
  databricks bundle run revisiofitxers_ingest -t "$TARGET" --profile "$PROFILE" --var="$VARS"
fi

# --- 5) start app + grant SP read access on the state schema -----------------------------
echo "== 5/5 Starting app and granting its SP read access =="
databricks bundle run revisiofitxers_app -t "$TARGET" --profile "$PROFILE" --var="$VARS"
APP_NAME="revisiofitxers-$TARGET"
"$PY" scripts/grant_app_access.py --profile "$PROFILE" --app "$APP_NAME" \
  --endpoint "$LB_ENDPOINT" --host "$LB_HOST" --pg-database "$LB_PGDB"
# Restart so the app picks up the new grant on boot.
databricks bundle run revisiofitxers_app -t "$TARGET" --profile "$PROFILE" --var="$VARS"
APP_URL="$(databricks apps get "$APP_NAME" --profile "$PROFILE" -o json | "$PY" -c "import json,sys;print(json.load(sys.stdin).get('url',''))")"

echo
echo "Done. RevisioFitxers deployed to profile '$PROFILE' (target '$TARGET')."
echo "  Catalog   : $CATALOG"
echo "  Lakebase  : $LB_HOST / $LB_PGDB (project $LAKEBASE_PROJECT)"
echo "  App URL   : $APP_URL"
