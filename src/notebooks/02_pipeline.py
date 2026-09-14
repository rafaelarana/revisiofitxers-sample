# Databricks notebook source
# MAGIC %md
# MAGIC # 2) AI Functions pipeline — parse, classify, extract, summarize
# MAGIC
# MAGIC Runs entirely via `spark.sql(...)` on serverless compute (no SQL warehouse needed).
# MAGIC Mirrors `sql/pipeline.sql` (the manual/local version of this MVP) statement for statement —
# MAGIC keep the two in sync if you change one.
# MAGIC
# MAGIC Known gotcha baked in below: `array_remove(array, NULL)` does **not** remove NULLs in this
# MAGIC SQL engine (NULL ≠ NULL) — use `concat_ws(sep, array(...))` directly, which does skip NULLs.

# COMMAND ----------

dbutils.widgets.text("catalog", "revisiofitxers_sample")
dbutils.widgets.text("schema", "mvp")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

VOLUME_ROOT_SQL_LITERAL = f"/Volumes/{catalog}/{schema}"  # volume name appended per-path below

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manifest — lineage back to the original path/filename

# COMMAND ----------

dbutils.widgets.text("volume", "raw_docs")
volume = dbutils.widgets.get("volume")
staging_root = f"/Volumes/{catalog}/{schema}/{volume}/staging"

spark.sql(f"""
CREATE OR REPLACE TABLE manifest AS
SELECT lineage, kind, staged_name, original_filename
FROM read_files('{staging_root}/manifest.json', format => 'json', multiLine => true)
WHERE kind IN ('binary', 'text')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse binary docs (PDF/DOC/DOCX/PPT/PPTX) with `ai_parse_document`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE docs_parsed AS
SELECT
  regexp_extract(path, '([^/]+)$', 1) AS staged_name,
  concat_ws('\\n', transform(try_cast(parsed:document:elements AS ARRAY<VARIANT>), e -> e:content::STRING)) AS text_blocks,
  parsed:error_status AS parse_error
FROM (
  SELECT path, ai_parse_document(content, map('version', '2.0')) AS parsed
  FROM read_files('{staging_root}/binary/', format => 'binaryFile')
)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-extracted text (.xlsx cell dump, .eml body) — unsupported by `ai_parse_document`

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE docs_text_extracted AS
SELECT
  regexp_extract(_metadata.file_path, '([^/]+)$', 1) AS staged_name,
  value AS text_blocks
FROM read_files('{staging_root}/text/', format => 'text', wholetext => true)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union + join back to original lineage/filename

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE docs_all AS
SELECT m.lineage, m.original_filename, m.staged_name, d.text_blocks
FROM docs_parsed d
JOIN manifest m USING (staged_name)
WHERE d.text_blocks IS NOT NULL AND length(d.text_blocks) > 0
UNION ALL
SELECT m.lineage, m.original_filename, m.staged_name, t.text_blocks
FROM docs_text_extracted t
JOIN manifest m USING (staged_name)
WHERE t.text_blocks IS NOT NULL AND length(t.text_blocks) > 0
""")

n_docs = spark.table("docs_all").count()
print(f"docs_all rows: {n_docs}")
assert n_docs > 0, "No documents made it through parsing/extraction — check staging output."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich: título, resum (català, via `ai_gen` — `ai_summarize` is English-tuned),
# MAGIC ## classificació (categorías cerradas del cliente) y metadatos propuestos

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE docs_enriched AS
SELECT
  lineage,
  original_filename,
  staged_name,
  text_blocks,
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
  ai_extract(
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
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado final — columnas tal como las pide el email del cliente + metadatos propuestos
# MAGIC
# MAGIC Nombres de columna sin espacios/acentos: Delta no los admite sin Column Mapping;
# MAGIC `03_export_excel.py` los renombra a las cabeceras en catalán del cliente al exportar.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE resultat_final AS
SELECT
  lineage                                        AS ruta_del_fitxer,
  original_filename                              AS nom_del_fitxer,
  COALESCE(NULLIF(extracted:titol_document::STRING, ''), original_filename) AS titol,
  resum,
  classificacio,
  concat_ws(' | ',
    array(
      CASE WHEN extracted:numero_expedient::STRING IS NOT NULL AND extracted:numero_expedient::STRING != '' THEN concat('Núm. expedient: ', extracted:numero_expedient::STRING) END,
      CASE WHEN extracted:data_document::STRING IS NOT NULL AND extracted:data_document::STRING != '' THEN concat('Data: ', extracted:data_document::STRING) END,
      CASE WHEN extracted:entitat_organisme::STRING IS NOT NULL AND extracted:entitat_organisme::STRING != '' THEN concat('Entitat: ', extracted:entitat_organisme::STRING) END,
      CASE WHEN extracted:proveidor_adjudicatari::STRING IS NOT NULL AND extracted:proveidor_adjudicatari::STRING != '' THEN concat('Proveïdor/Adjudicatari: ', extracted:proveidor_adjudicatari::STRING) END,
      CASE WHEN extracted:import::STRING IS NOT NULL AND extracted:import::STRING != '' THEN concat('Import: ', extracted:import::STRING) END,
      CASE WHEN extracted:tipus_procediment::STRING IS NOT NULL AND extracted:tipus_procediment::STRING != '' THEN concat('Tipus procediment: ', extracted:tipus_procediment::STRING) END
    )
  ) AS informacio_addicional_proposta,
  extracted:numero_expedient::STRING              AS num_expedient,
  extracted:data_document::STRING                 AS data_document,
  extracted:entitat_organisme::STRING             AS entitat_organisme,
  extracted:proveidor_adjudicatari::STRING        AS proveidor_adjudicatari,
  extracted:import::STRING                        AS import_,
  extracted:tipus_procediment::STRING             AS tipus_procediment
FROM docs_enriched
ORDER BY nom_del_fitxer
""")

display(spark.table("resultat_final"))
