# Databricks notebook source
# MAGIC %md
# MAGIC # 3) Export to Excel — the deliverable the customer asked for
# MAGIC
# MAGIC Columns exactly as requested in the customer email (path/name/title/summary/classification)
# MAGIC plus the proposed extra-metadata column, with Catalan headers.

# COMMAND ----------

dbutils.widgets.text("catalog", "revisiofitxers_sample")
dbutils.widgets.text("schema", "mvp")
dbutils.widgets.text("volume", "raw_docs")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

OUTPUT_DIR = f"/Volumes/{catalog}/{schema}/{volume}/output"
OUTPUT_XLSX = f"{OUTPUT_DIR}/RevisioFitxers_MVP_resultats.xlsx"

COLUMN_RENAME = {
    "ruta_del_fitxer": "Ruta del fitxer",
    "nom_del_fitxer": "Nom del fitxer",
    "titol": "Títol",
    "resum": "Resum",
    "classificacio": "Classificació",
    "informacio_addicional_proposta": "Informació addicional (proposta)",
    "num_expedient": "Núm. expedient",
    "data_document": "Data document",
    "entitat_organisme": "Entitat / organisme",
    "proveidor_adjudicatari": "Proveïdor / adjudicatari",
    "import_": "Import",
    "tipus_procediment": "Tipus de procediment",
}
ORDERED_COLS = [
    "Ruta del fitxer", "Nom del fitxer", "Títol", "Resum", "Classificació",
    "Informació addicional (proposta)",
    "Núm. expedient", "Data document", "Entitat / organisme", "Proveïdor / adjudicatari",
    "Import", "Tipus de procediment",
]

# COMMAND ----------

import os
import shutil
import tempfile

import pandas as pd

df = spark.table(f"{catalog}.{schema}.resultat_final").toPandas()
df = df.rename(columns=COLUMN_RENAME)
df = df[[c for c in ORDERED_COLS if c in df.columns]]

# xlsx is a zip container — openpyxl/zipfile need a seekable file handle to write its central
# directory at close(), and UC Volume FUSE mounts don't support seek on an open write handle
# (fails with "OSError: [Errno 5] Input/output error"). Write locally, then copy the finished
# file into the Volume.
with tempfile.TemporaryDirectory() as tmp_dir:
    tmp_xlsx = os.path.join(tmp_dir, "result.xlsx")
    with pd.ExcelWriter(tmp_xlsx, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Classificacio fitxers")
        ws = writer.sheets["Classificacio fitxers"]
        for i, col in enumerate(df.columns, start=1):
            max_len = max((df[col].astype(str).map(len).max() if len(df) else 0), len(col))
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(max_len + 2, 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    shutil.copyfile(tmp_xlsx, OUTPUT_XLSX)

print(f"Written {len(df)} rows -> {OUTPUT_XLSX}")
print("Download via: Catalog Explorer > Volumes, or `databricks fs cp` from the CLI.")
