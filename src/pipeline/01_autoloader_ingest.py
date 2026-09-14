# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 1) Auto Loader ingest + normalize  (Phase 1)
# MAGIC
# MAGIC Watches the UC Volume `landing/**` area with Auto Loader (`cloudFiles`, binaryFile) and,
# MAGIC for each newly-landed file, normalizes it (unzip incl. nested, detach `.eml` attachments,
# MAGIC xlsx→text, content-hash dedup) into `staging/<expedient_id>/{binary,text}` + a per-expedient
# MAGIC `manifest.json`, then moves the processed raw file to `archive/<expedient_id>/`.
# MAGIC
# MAGIC Normalization uses the shared `src/shared/normalize.py` module — the SAME implementation
# MAGIC the app backend and unit tests use (spec §8.2). Auto Loader's checkpoint gives exactly-once
# MAGIC ingestion; `Trigger.AvailableNow` drains all pending files then stops (the DAB job's
# MAGIC `file_arrival` trigger re-runs this whenever new files land — spec G4/D5).
# MAGIC
# MAGIC ⚠️ Watches **only** `landing/**`. `staging/` + `archive/` are outputs and are NOT watched,
# MAGIC so unzipping inside the batch never re-triggers the stream (spec D6).

# COMMAND ----------

import os
import sys

# Params: prefer job/task parameters (widgets), fall back to spark.conf (pipeline config).
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")


def _param(name: str, conf_key: str) -> str:
    v = dbutils.widgets.get(name)
    if v:
        return v
    return spark.conf.get(conf_key)


catalog = _param("catalog", "revisiofitxers.catalog")
schema = _param("schema", "revisiofitxers.schema")
volume = _param("volume", "revisiofitxers.volume")

VOLUME_ROOT = f"/Volumes/{catalog}/{schema}/{volume}"
LANDING = f"{VOLUME_ROOT}/landing"
STAGING = f"{VOLUME_ROOT}/staging"
ARCHIVE = f"{VOLUME_ROOT}/archive"
CHECKPOINT = f"{VOLUME_ROOT}/_checkpoints/autoloader_ingest"
SCHEMA_LOC = f"{VOLUME_ROOT}/_checkpoints/autoloader_schema"

print(f"catalog/schema/volume : {catalog}/{schema}/{volume}")
print(f"watching              : {LANDING}/**  (cloudFiles, directory listing)")
print(f"staging  -> {STAGING}")
print(f"archive  -> {ARCHIVE}")

# Ensure the folder skeleton exists (idempotent).
for p in (LANDING, STAGING, ARCHIVE, f"{VOLUME_ROOT}/_checkpoints"):
    os.makedirs(p, exist_ok=True)

# COMMAND ----------

# Make the bundled shared module importable. The bundle syncs the repo to
# {workspace.file_path}/files/...; this notebook lives at .../files/src/pipeline/,
# so src/ is two levels up from __file__ isn't available in notebooks — resolve via
# the notebook's own workspace path instead.
_nb_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
# _nb_path = /Workspace/.../files/src/pipeline/01_autoloader_ingest  ->  src root = .../files/src
_src_root = "/Workspace" + _nb_path.rsplit("/src/", 1)[0] + "/src"
if _src_root not in sys.path:
    sys.path.insert(0, _src_root)
print(f"src root on path: {_src_root}")

from shared.normalize import normalize_files, write_result_to_dir  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume-safe writers
# MAGIC Small artifacts (staged files, manifest) write fine through the Volume FUSE mount;
# MAGIC we inject explicit writers so the shared module stays filesystem-agnostic.

# COMMAND ----------

import json
import re
import shutil
from pathlib import Path

# landing/<expedient_id>/raw/<...>  — capture the expedient id segment.
_EXP_RE = re.compile(r"/landing/([^/]+)/")


def expedient_id_from_path(file_path: str) -> str:
    m = _EXP_RE.search(file_path)
    return m.group(1) if m else "_unassigned"


def process_batch(batch_df, batch_id: int):
    # Collect this micro-batch's files to the driver. binaryFile rows are small here
    # (expedient uploads), so driver-side normalization is fine for Phase 1 volumes.
    rows = batch_df.select("path", "content").collect()
    if not rows:
        return

    # Group landed files by expedient so each expedient gets its own staging/ + manifest.
    by_exp: dict[str, list[tuple[str, bytes]]] = {}
    raw_paths_by_exp: dict[str, list[str]] = {}
    for r in rows:
        exp = expedient_id_from_path(r["path"])
        # name relative to the expedient's raw/ folder, so lineage reads naturally
        rel = r["path"].split(f"/landing/{exp}/raw/", 1)[-1] if "/raw/" in r["path"] else Path(r["path"]).name
        by_exp.setdefault(exp, []).append((rel, bytes(r["content"])))
        raw_paths_by_exp.setdefault(exp, []).append(r["path"])

    for exp, sources in by_exp.items():
        result = normalize_files(sources)
        stage_root = f"{STAGING}/{exp}"
        counts = write_result_to_dir(result, stage_root, clean=False)
        print(f"[batch {batch_id}] expedient={exp}: staged {counts} from {result.n_source_files} source file(s)")

        # Move processed raw files landing/<exp>/raw -> archive/<exp>/ for idempotency.
        # Auto Loader's `path` is a URI (e.g. "dbfs:/Volumes/..."); strip the scheme to
        # get the local FUSE path shutil can operate on.
        arch_dir = f"{ARCHIVE}/{exp}"
        os.makedirs(arch_dir, exist_ok=True)
        for src in raw_paths_by_exp[exp]:
            fs_src = re.sub(r"^[a-zA-Z0-9]+:", "", src)  # drop "dbfs:" / "file:" scheme
            if not fs_src.startswith("/"):
                fs_src = "/" + fs_src
            dest = f"{arch_dir}/{Path(fs_src).name}"
            if not os.path.exists(fs_src):
                # already archived on a prior (retried) batch — safe no-op
                continue
            shutil.move(fs_src, dest)
            print(f"[batch {batch_id}] archived {fs_src} -> {dest}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader stream — `cloudFiles` binaryFile over `landing/**`

# COMMAND ----------

stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "binaryFile")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    # Only real uploaded files under an expedient's raw/ folder should trigger work.
    .option("pathGlobFilter", "*")
    .load(LANDING)
    # Defensive: ignore anything not under a raw/ path (e.g. stray files at landing root).
    .filter("path LIKE '%/raw/%'")
)

query = (
    stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
print("ingest run complete")
