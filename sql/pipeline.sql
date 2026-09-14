-- RevisioFitxers MVP — clasificación de documentos con Databricks AI Functions
-- Catálogo: revisiofitxers_sample.mvp
-- Entrada: staged files en /Volumes/revisiofitxers_sample/mvp/raw_docs/{binary,text}
--          + manifest.json con la trazabilidad al fichero/ruta original (incl. zip/eml desempaquetados)

USE CATALOG revisiofitxers_sample;
USE SCHEMA mvp;

-- 1) Manifest: lineage (ruta original), staged_name, original_filename
CREATE OR REPLACE TABLE manifest AS
SELECT
  lineage,
  kind,
  staged_name,
  original_filename
FROM read_files('/Volumes/revisiofitxers_sample/mvp/raw_docs/manifest.json', format => 'json', multiLine => true)
WHERE kind IN ('binary', 'text');

-- 2) Parse binary docs (PDF/DOCX) with ai_parse_document
CREATE OR REPLACE TABLE docs_parsed AS
SELECT
  regexp_extract(path, '([^/]+)$', 1) AS staged_name,
  concat_ws('\n', transform(try_cast(parsed:document:elements AS ARRAY<VARIANT>), e -> e:content::STRING)) AS text_blocks,
  parsed:error_status AS parse_error
FROM (
  SELECT path, ai_parse_document(content, map('version', '2.0')) AS parsed
  FROM read_files('/Volumes/revisiofitxers_sample/mvp/raw_docs/binary/', format => 'binaryFile')
);

-- 3) Pre-extracted text (xlsx cell dump, eml body) — read as-is, no ai_parse_document (unsupported formats)
CREATE OR REPLACE TABLE docs_text_extracted AS
SELECT
  regexp_extract(_metadata.file_path, '([^/]+)$', 1) AS staged_name,
  value AS text_blocks
FROM read_files('/Volumes/revisiofitxers_sample/mvp/raw_docs/text/', format => 'text', wholetext => true);

-- 4) Union + join back to original lineage/filename
CREATE OR REPLACE TABLE docs_all AS
SELECT m.lineage, m.original_filename, m.staged_name, d.text_blocks
FROM docs_parsed d
JOIN manifest m USING (staged_name)
WHERE d.text_blocks IS NOT NULL AND length(d.text_blocks) > 0
UNION ALL
SELECT m.lineage, m.original_filename, m.staged_name, t.text_blocks
FROM docs_text_extracted t
JOIN manifest m USING (staged_name)
WHERE t.text_blocks IS NOT NULL AND length(t.text_blocks) > 0;

-- 5) Enrich: título, resumen, clasificación (categorías cerradas del cliente), campos adicionales propuestos
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
      "Resolucio adjudicacio": "Resolución que adjudica un contrato/expediente a un proveedor",
      "Notificacio adjudicatari": "Notificación oficial al adjudicatario informando de la adjudicación",
      "Publicacio adj - PSCP": "Anuncio o publicación de la adjudicación en la Plataforma de Serveis de Contractació Publica (PSCP)",
      "Inf fiscalitza fav doc DR_adjudic": "Informe de fiscalización favorable, fase de adjudicación (DR)",
      "Inf fiscalitza fav foc RD_prepar": "Informe de fiscalización favorable, fase de preparación (RD)"
    }',
    map('version', '2.0', 'instructions', 'Documentos de expedientes de contratación pública / intervención (sector público). Elige la categoría que mejor describe el propósito principal del documento.')
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
    map('version', '2.0', 'instructions', 'Documentos de expedientes de contratación pública / intervención en catalán/castellano. Extrae solo lo que aparezca explícitamente; deja vacío si no aparece.')
  ):response AS extracted
FROM docs_all;

-- 6) Resultado final — columnas tal como las pide el email del cliente + propuesta de metadatos adicionales
-- (nombres de columna sin espacios/acentos: Delta no los admite sin Column Mapping;
--  el export a Excel — 02_export_excel.py — los renombra a las cabeceras en catalán del cliente)
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
ORDER BY nom_del_fitxer;

SELECT * FROM resultat_final;
