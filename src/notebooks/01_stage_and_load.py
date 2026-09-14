# Databricks notebook source
# MAGIC %md
# MAGIC # 1) Stage & load — RevisioFitxers MVP
# MAGIC
# MAGIC Recursively walks the synced sample-data folder and normalizes it into two buckets in the
# MAGIC UC Volume, because `ai_parse_document` only supports PDF/JPG/PNG/TIFF/DOC/DOCX/PPT/PPTX:
# MAGIC - `staging/binary/` — files handed as-is to `ai_parse_document` downstream
# MAGIC - `staging/text/`   — pre-extracted text for formats it can't read (.xlsx cells, .eml bodies)
# MAGIC
# MAGIC `.zip` (incl. nested) is unpacked and `.eml` attachments are detached, recursively, with
# MAGIC content-hash deduplication. `staging/manifest.json` keeps the lineage back to the original
# MAGIC path/filename so the final report can cite it.

# COMMAND ----------

dbutils.widgets.text("catalog", "revisiofitxers_sample")
dbutils.widgets.text("schema", "mvp")
dbutils.widgets.text("volume", "raw_docs")
dbutils.widgets.text("sample_data_path", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")
sample_data_path = dbutils.widgets.get("sample_data_path")

VOLUME_ROOT = f"/Volumes/{catalog}/{schema}/{volume}"
print(f"Sample data source : {sample_data_path}")
print(f"Volume root        : {VOLUME_ROOT}")

import os

assert os.path.isdir(VOLUME_ROOT), (
    f"Volume root '{VOLUME_ROOT}' doesn't exist or isn't visible to this cluster. "
    f"Most likely cause: the schema/volume name passed in doesn't match what was actually "
    f"deployed — `mode: development` in databricks.yml auto-prefixes UC schema names "
    f"(e.g. 'mvp' -> 'dev_<user>_mvp'), so job parameters must resolve through "
    f"${{resources.schemas.mvp_schema.name}} / ${{resources.volumes.raw_docs.name}}, not the "
    f"bare ${{var.schema}}/${{var.volume}} (see resources/job.yml). Check `databricks bundle "
    f"summary` for the actual deployed catalog/schema/volume names."
)

# COMMAND ----------

import hashlib
import json
import shutil
import zipfile
from email import policy
from email.parser import BytesParser
from pathlib import Path

import openpyxl

SRC_ROOT = Path(sample_data_path)
STAGE_ROOT = Path(f"{VOLUME_ROOT}/staging")
BINARY_DIR = STAGE_ROOT / "binary"
TEXT_DIR = STAGE_ROOT / "text"

BINARY_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".doc", ".docx", ".ppt", ".pptx"}

manifest = []
seen_hashes = {}


def content_hash(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()[:12]


def safe_stem(rel_path: str, suffix: str) -> str:
    h = hashlib.sha1(rel_path.encode()).hexdigest()[:16]
    return f"doc_{h}{suffix}"


def register(rel_lineage: str, data: bytes, suffix: str, kind: str, extracted_text: str | None = None):
    h = content_hash(data)
    if h in seen_hashes:
        manifest.append({"lineage": rel_lineage, "kind": "duplicate", "duplicate_of": seen_hashes[h]})
        return
    staged_name = safe_stem(rel_lineage, suffix)
    if kind == "binary":
        (BINARY_DIR / staged_name).write_bytes(data)
    else:
        (TEXT_DIR / staged_name).write_text(extracted_text or "", encoding="utf-8")
    seen_hashes[h] = staged_name
    manifest.append({
        "lineage": rel_lineage,
        "kind": kind,
        "staged_name": staged_name,
        "original_filename": Path(rel_lineage.split(" > ")[-1]).name,
    })


def xlsx_to_text(data: bytes) -> str:
    import io
    wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True, read_only=True)
    lines = []
    for ws in wb.worksheets:
        lines.append(f"### Hoja: {ws.title}")
        for row in ws.iter_rows(values_only=True):
            cells = [str(c) for c in row if c is not None]
            if cells:
                lines.append(" | ".join(cells))
    return "\n".join(lines)


def process_eml(rel_lineage: str, data: bytes):
    msg = BytesParser(policy=policy.default).parsebytes(data)
    subject, sender, date = msg.get("subject", ""), msg.get("from", ""), msg.get("date", "")
    body = msg.get_body(preferencelist=("plain", "html"))
    body_text = body.get_content() if body else ""
    text = f"Asunto: {subject}\nDe: {sender}\nFecha: {date}\n\n{body_text}"
    register(f"{rel_lineage} (cuerpo)", text.encode("utf-8"), ".txt", "text", extracted_text=text)
    for part in msg.iter_attachments():
        fname = part.get_filename() or "attachment"
        content = part.get_content()
        if isinstance(content, str):
            content = content.encode("utf-8")
        process_entry(f"{rel_lineage} > {fname}", content, Path(fname).suffix.lower())


def process_zip(rel_lineage: str, data: bytes):
    import io
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            process_entry(f"{rel_lineage} > {info.filename}", zf.read(info), Path(info.filename).suffix.lower())


def process_entry(rel_lineage: str, data: bytes, suffix: str):
    if suffix == ".zip":
        process_zip(rel_lineage, data)
    elif suffix == ".eml":
        process_eml(rel_lineage, data)
    elif suffix in (".xlsx", ".xlsm"):
        try:
            text = xlsx_to_text(data)
        except Exception as e:
            text = f"[ERROR extrayendo xlsx: {e}]"
        register(rel_lineage, data, suffix, "text", extracted_text=text)
    elif suffix in BINARY_EXTS:
        register(rel_lineage, data, suffix, "binary")
    else:
        register(rel_lineage, data, suffix or ".bin", "skipped")


# COMMAND ----------

shutil.rmtree(STAGE_ROOT, ignore_errors=True)
BINARY_DIR.mkdir(parents=True, exist_ok=True)
TEXT_DIR.mkdir(parents=True, exist_ok=True)

n_source_files = 0
for path in sorted(SRC_ROOT.rglob("*")):
    if path.is_dir() or path.name.startswith("."):
        continue
    n_source_files += 1
    rel = str(path.relative_to(SRC_ROOT))
    process_entry(rel, path.read_bytes(), path.suffix.lower())

assert n_source_files > 0, (
    f"No files found under {SRC_ROOT} — check that sample data was synced "
    f"(bundle deploy should have copied '{dbutils.widgets.get('sample_data_path')}')."
)

(STAGE_ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")

kinds = {}
for row in manifest:
    kinds[row["kind"]] = kinds.get(row["kind"], 0) + 1
print(f"Source files scanned : {n_source_files}")
print(f"Staged               : {kinds}")
print(f"Binary  -> {BINARY_DIR}")
print(f"Text    -> {TEXT_DIR}")
print(f"Manifest-> {STAGE_ROOT / 'manifest.json'}")
