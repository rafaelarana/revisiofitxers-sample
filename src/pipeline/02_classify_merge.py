# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 2) Classify + upsert into Lakebase state store  (Phase 2)
# MAGIC
# MAGIC Reads the normalized `staging/<expedient_id>/` artifacts produced by task 1, runs the
# MAGIC AI-Functions classification (EXACT prompts from `src/notebooks/02_pipeline.py`), and
# MAGIC upserts results into the Lakebase (Postgres) state store keyed by `(expedient_id,
# MAGIC content_hash)` so re-runs are idempotent. Prior classifications are archived to
# MAGIC `classification_history` (non-destructive); expedient status/counters + `events` update.
# MAGIC (spec §5.2 / build-plan Phase 2, tasks 2.2–2.4.)

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("volume", "")
dbutils.widgets.text("lakebase_endpoint", "")   # projects/<p>/branches/<b>/endpoints/<e>
dbutils.widgets.text("lakebase_host", "")
dbutils.widgets.text("lakebase_pg_database", "databricks_postgres")


def _p(name, conf_key=None, default=""):
    v = dbutils.widgets.get(name)
    if v:
        return v
    if conf_key:
        try:
            return spark.conf.get(conf_key)
        except Exception:
            return default
    return default


catalog = _p("catalog", "revisiofitxers.catalog")
schema = _p("schema", "revisiofitxers.schema")
volume = _p("volume", "revisiofitxers.volume")
lb_endpoint = _p("lakebase_endpoint", "revisiofitxers.lakebase_endpoint")
lb_host = _p("lakebase_host", "revisiofitxers.lakebase_host")
lb_pgdb = _p("lakebase_pg_database", "revisiofitxers.lakebase_pg_database", "databricks_postgres")

staging_root = f"/Volumes/{catalog}/{schema}/{volume}/staging"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")
print(f"staging root : {staging_root}")
print(f"lakebase     : {lb_host}/{lb_pgdb}  ({lb_endpoint})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build docs_all across all expedients — manifests carry expedient_id via the path

# COMMAND ----------

import glob

# Each expedient has its own staging/<exp>/manifest.json. Read them all with read_files
# (explicit columns — no pandas type inference), tagging expedient_id from the file path:
# .../staging/<expedient_id>/manifest.json
if not glob.glob(f"{staging_root}/*/manifest.json"):
    print("No staged manifests found — nothing to classify.")
    dbutils.notebook.exit("no-op")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW manifest_all AS
SELECT
  regexp_extract(_metadata.file_path, '/staging/([^/]+)/manifest.json$', 1) AS expedient_id,
  lineage::STRING            AS lineage,
  kind::STRING               AS kind,
  staged_name::STRING        AS staged_name,
  original_filename::STRING  AS original_filename
FROM read_files('{staging_root}/*/manifest.json', format => 'json', multiLine => true)
WHERE kind IN ('binary', 'text')
""")
n_manifest = spark.table("manifest_all").count()
print(f"manifest entries to classify: {n_manifest}")
if n_manifest == 0:
    dbutils.notebook.exit("no-op")

# COMMAND ----------

# Parse binary docs; passthrough text docs. staged_name ties back to expedient_id via manifest.
# We read per-expedient so file paths resolve; union into one docs_all view.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW docs_parsed AS
SELECT
  regexp_extract(path, '([^/]+)$', 1) AS staged_name,
  concat_ws('\\n', transform(try_cast(parsed:document:elements AS ARRAY<VARIANT>), e -> e:content::STRING)) AS text_blocks
FROM (
  SELECT path, ai_parse_document(content, map('version', '2.0')) AS parsed
  FROM read_files('{staging_root}/*/binary/', format => 'binaryFile')
)
""")

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW docs_text AS
SELECT regexp_extract(_metadata.file_path, '([^/]+)$', 1) AS staged_name, value AS text_blocks
FROM read_files('{staging_root}/*/text/', format => 'text', wholetext => true)
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW docs_all AS
SELECT m.expedient_id, m.lineage, m.original_filename, m.staged_name, d.text_blocks
FROM docs_parsed d JOIN manifest_all m USING (staged_name)
WHERE d.text_blocks IS NOT NULL AND length(d.text_blocks) > 0
UNION ALL
SELECT m.expedient_id, m.lineage, m.original_filename, m.staged_name, t.text_blocks
FROM docs_text t JOIN manifest_all m USING (staged_name)
WHERE t.text_blocks IS NOT NULL AND length(t.text_blocks) > 0
""")

# Failed docs = manifest binary/text entries that produced NO usable text (unparseable,
# corrupt, empty, or an unsupported format that slipped past staging). ERROR path
# (build-plan Phase 2 carry-over): record these explicitly as ERROR so their expedient
# is marked NEEDS_REVIEW — instead of silently dropping them and leaving the expedient
# stuck in PROCESSING with 0 docs.
spark.sql("""
CREATE OR REPLACE TEMP VIEW docs_failed AS
SELECT m.expedient_id, m.lineage, m.original_filename, m.staged_name, m.kind
FROM manifest_all m
LEFT ANTI JOIN docs_all d USING (staged_name)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich — resum (ai_gen, català), classificació (5 cats), metadata (ai_extract)
# MAGIC Prompts copied VERBATIM from src/notebooks/02_pipeline.py (spec §3.2 D4).

# COMMAND ----------

enriched = spark.sql("""
SELECT
  expedient_id,
  lineage,
  original_filename,
  staged_name,
  ai_gen(concat(
    'Resumeix el contingut d''aquest document en català, en 2-3 frases breus i concises, sense inventar dades que no hi apareguin: ',
    substr(text_blocks, 1, 6000)
  )) AS resum,
  ai_classify(
    text_blocks,
    '{
      "Resolucio adjudicacio": "Resolucion que adjudica un contrato/expediente a un proveedor",
      "Notificacio adjudicatari": "Notificacion oficial al adjudicatario informando de la adjudicacion",
      "Publicacio adj - PSCP": "Anuncio o publicacion de la adjudicacion en la Plataforma de Serveis de Contractacio Publica (PSCP)",
      "Inf fiscalitza fav doc DR_adjudic": "Informe de fiscalizacion favorable, fase de adjudicacion (DR)",
      "Inf fiscalitza fav foc RD_prepar": "Informe de fiscalizacion favorable, fase de preparacion (RD)"
    }',
    map('version', '2.0', 'instructions', 'Documentos de expedientes de contratacion publica / intervencion (sector público). Elige la categoria que mejor describe el proposito principal del documento.')
  ):response[0]::STRING AS classificacio,
  extracted:titol_document::STRING          AS titol_document,
  extracted:numero_expedient::STRING        AS numero_expedient,
  extracted:data_document::STRING           AS data_document,
  extracted:entitat_organisme::STRING       AS entitat_organisme,
  extracted:proveidor_adjudicatari::STRING  AS proveidor_adjudicatari,
  extracted:import::STRING                  AS import_,
  extracted:tipus_procediment::STRING       AS tipus_procediment
FROM (
  SELECT *, ai_extract(
    text_blocks,
    '{
      "titol_document": {"type": "string"},
      "numero_expedient": {"type": "string"},
      "data_document": {"type": "string"},
      "entitat_organisme": {"type": "string"},
      "proveidor_adjudicatari": {"type": "string"},
      "import": {"type": "string"},
      "tipus_procediment": {"type": "string"}
    }',
    map('version', '2.0', 'instructions', 'Documentos de expedientes de contratacion publica / intervencion en catalan/castellano. Extrae solo lo que aparezca explicitamente; deja vacio si no aparece.')
  ):response AS extracted
  FROM docs_all
)
""")

# Cast everything to plain strings before .collect() — nested VARIANT/struct columns
# don't serialize back through Spark Connect (serverless). All columns here are STRING.
rows = enriched.collect()
failed_rows = spark.table("docs_failed").collect()
print(f"classified rows: {len(rows)} · failed (ERROR) rows: {len(failed_rows)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert into Lakebase (Postgres) — idempotent on (expedient_id, content_hash)

# COMMAND ----------

import json

import psycopg
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
token = w.postgres.generate_database_credential(lb_endpoint).token
user = w.current_user.me().user_name

dsn = f"host={lb_host} dbname={lb_pgdb} user={user} password={token} sslmode=require"


def content_hash(staged_name: str) -> str:
    # staged_name is doc_<sha1(lineage)[:16]><suffix>; reuse it as a stable per-doc key.
    return staged_name


def nn(v):
    """Normalize empty strings to NULL for Postgres."""
    return v if v not in ("", None) else None


upserted = 0
with psycopg.connect(dsn, autocommit=False) as conn:
    with conn.cursor() as cur:
        cur.execute("SET search_path TO revisiofitxers")
        # Ensure expedient rows exist for EVERYTHING the pipeline discovered — both
        # successfully-classified and failed (ERROR) docs — so an expedient whose files all
        # failed to parse still gets a row and its status recomputed (→ NEEDS_REVIEW).
        exp_ids = sorted({r["expedient_id"] for r in rows} | {r["expedient_id"] for r in failed_rows})
        for exp in exp_ids:
            cur.execute(
                """INSERT INTO expedients (expedient_id, owner, status)
                   VALUES (%s, %s, 'PROCESSING')
                   ON CONFLICT (expedient_id) DO UPDATE SET status='PROCESSING', updated_at=now()""",
                (exp, user),
            )
        for r in rows:
            exp = r["expedient_id"]
            chash = content_hash(r["staged_name"])
            doc_id = f"{exp}:{chash}"

            # documents upsert
            cur.execute(
                """INSERT INTO documents (doc_id, expedient_id, content_hash, original_filename,
                        original_lineage, staged_name, status)
                   VALUES (%s,%s,%s,%s,%s,%s,'CLASSIFIED')
                   ON CONFLICT (doc_id) DO UPDATE SET status='CLASSIFIED', updated_at=now()""",
                (doc_id, exp, chash, r["original_filename"], r["lineage"], r["staged_name"]),
            )
            # archive prior classification (if any) then upsert latest
            cur.execute(
                """INSERT INTO classification_history
                   (doc_id, titol, resum, classificacio, confidence, num_expedient, data_document,
                    entitat_organisme, proveidor_adjudicatari, import_, tipus_procediment, model,
                    prompt_version, source, correction_instruction)
                   SELECT doc_id, titol, resum, classificacio, confidence, num_expedient, data_document,
                    entitat_organisme, proveidor_adjudicatari, import_, tipus_procediment, model,
                    prompt_version, source, correction_instruction
                   FROM classifications WHERE doc_id = %s""",
                (doc_id,),
            )
            cur.execute(
                """INSERT INTO classifications
                   (doc_id, titol, resum, classificacio, num_expedient, data_document,
                    entitat_organisme, proveidor_adjudicatari, import_, tipus_procediment,
                    model, prompt_version, source)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'AUTO')
                   ON CONFLICT (doc_id) DO UPDATE SET
                     titol=EXCLUDED.titol, resum=EXCLUDED.resum, classificacio=EXCLUDED.classificacio,
                     num_expedient=EXCLUDED.num_expedient, data_document=EXCLUDED.data_document,
                     entitat_organisme=EXCLUDED.entitat_organisme,
                     proveidor_adjudicatari=EXCLUDED.proveidor_adjudicatari,
                     import_=EXCLUDED.import_, tipus_procediment=EXCLUDED.tipus_procediment,
                     source='AUTO', created_at=now()""",
                (
                    doc_id,
                    nn(r["titol_document"]) or r["original_filename"],
                    r["resum"],
                    r["classificacio"],
                    nn(r["numero_expedient"]),
                    nn(r["data_document"]),
                    nn(r["entitat_organisme"]),
                    nn(r["proveidor_adjudicatari"]),
                    nn(r["import_"]),
                    nn(r["tipus_procediment"]),
                    "ai_functions",
                    "v2.0",
                ),
            )
            cur.execute(
                """INSERT INTO events (expedient_id, doc_id, "user", type, payload_json)
                   VALUES (%s,%s,%s,'DOC_CLASSIFIED', %s::jsonb)""",
                (exp, doc_id, user, json.dumps({"classificacio": r["classificacio"]})),
            )
            upserted += 1

        # Record failed docs as ERROR (non-destructive: a later successful parse of the
        # same staged_name flips status back to CLASSIFIED via the success upsert above).
        errored = 0
        for r in failed_rows:
            exp = r["expedient_id"]
            chash = content_hash(r["staged_name"])
            doc_id = f"{exp}:{chash}"
            cur.execute(
                """INSERT INTO documents (doc_id, expedient_id, content_hash, original_filename,
                        original_lineage, staged_kind, staged_name, status, error_message)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,'ERROR',%s)
                   ON CONFLICT (doc_id) DO UPDATE SET status='ERROR',
                        error_message=EXCLUDED.error_message, updated_at=now()""",
                (
                    doc_id, exp, chash, r["original_filename"], r["lineage"], r["kind"],
                    r["staged_name"],
                    "El document no s'ha pogut llegir (format no suportat, corrupte o buit).",
                ),
            )
            cur.execute(
                """INSERT INTO events (expedient_id, doc_id, "user", type, payload_json)
                   VALUES (%s,%s,%s,'DOC_ERROR', %s::jsonb)""",
                (exp, doc_id, user, json.dumps({"original_filename": r["original_filename"]})),
            )
            errored += 1

        # Recompute expedient counters + status.
        for exp in exp_ids:
            cur.execute(
                """UPDATE expedients e SET
                     docs_total = (SELECT count(*) FROM documents WHERE expedient_id=%s),
                     docs_classified = (SELECT count(*) FROM documents WHERE expedient_id=%s AND status='CLASSIFIED'),
                     docs_error = (SELECT count(*) FROM documents WHERE expedient_id=%s AND status='ERROR'),
                     status = CASE
                        WHEN (SELECT count(*) FROM documents WHERE expedient_id=%s AND status='ERROR') > 0 THEN 'NEEDS_REVIEW'
                        ELSE 'READY' END,
                     updated_at = now()
                   WHERE e.expedient_id=%s""",
                (exp, exp, exp, exp, exp),
            )
    conn.commit()

print(
    f"Lakebase upsert complete — {upserted} classified + {errored} error document(s) "
    f"across {len(exp_ids)} expedient(s)."
)
