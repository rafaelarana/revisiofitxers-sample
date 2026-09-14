-- RevisioFitxers — Lakebase (Postgres) state store schema.
-- Idempotent: safe to run repeatedly (CREATE ... IF NOT EXISTS + ON CONFLICT seeds).
-- Provisioned by scripts/ensure_lakebase.py and by the classify pipeline task on startup.
-- Maps to spec §3.1 (domain model) / §8.3 (state store).

CREATE SCHEMA IF NOT EXISTS revisiofitxers;
SET search_path TO revisiofitxers;

-- 5 fixed categories (verbatim from src/notebooks/02_pipeline.py ai_classify call; spec §3.2).
-- Admin-editable at runtime; the app + pipeline both read labels from here (never hardcode).
CREATE TABLE IF NOT EXISTS taxonomy_categories (
    label        TEXT PRIMARY KEY,
    description  TEXT NOT NULL,
    sort_order   INT  NOT NULL DEFAULT 0,
    active       BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS expedients (
    expedient_id     TEXT PRIMARY KEY,
    title            TEXT,
    description      TEXT,
    owner            TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'DRAFT'
                     CHECK (status IN ('DRAFT','PROCESSING','READY','NEEDS_REVIEW','CLOSED')),
    docs_total       INT NOT NULL DEFAULT 0,
    docs_classified  INT NOT NULL DEFAULT 0,
    docs_error       INT NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at        TIMESTAMPTZ
);
-- Last ingest+classify job run triggered for this expedient (for live processing progress).
ALTER TABLE expedients ADD COLUMN IF NOT EXISTS last_run_id BIGINT;

CREATE TABLE IF NOT EXISTS documents (
    doc_id            TEXT PRIMARY KEY,          -- deterministic: <expedient_id>:<content_hash>
    expedient_id      TEXT NOT NULL REFERENCES expedients(expedient_id) ON DELETE CASCADE,
    content_hash      TEXT NOT NULL,
    original_filename TEXT,
    original_lineage  TEXT,                       -- e.g. "docs.zip > carpeta/annex.pdf"
    staged_kind       TEXT,                       -- binary | text | skipped | duplicate
    staged_name       TEXT,
    status            TEXT NOT NULL DEFAULT 'STAGED'
                      CHECK (status IN ('LANDED','STAGED','PARSED','CLASSIFIED','ERROR')),
    error_message     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (expedient_id, content_hash)
);
CREATE INDEX IF NOT EXISTS idx_documents_expedient ON documents(expedient_id);

-- Latest classification per document (upserted). History table keeps prior versions.
CREATE TABLE IF NOT EXISTS classifications (
    doc_id                  TEXT PRIMARY KEY REFERENCES documents(doc_id) ON DELETE CASCADE,
    titol                   TEXT,
    resum                   TEXT,
    classificacio           TEXT,
    confidence              DOUBLE PRECISION,
    num_expedient           TEXT,
    data_document           TEXT,
    entitat_organisme       TEXT,
    proveidor_adjudicatari  TEXT,
    import_                 TEXT,
    tipus_procediment       TEXT,
    model                   TEXT,
    prompt_version          TEXT,
    source                  TEXT NOT NULL DEFAULT 'AUTO' CHECK (source IN ('AUTO','CORRECTED')),
    correction_instruction  TEXT,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS classification_history (
    history_id              BIGSERIAL PRIMARY KEY,
    doc_id                  TEXT NOT NULL,
    titol                   TEXT,
    resum                   TEXT,
    classificacio           TEXT,
    confidence              DOUBLE PRECISION,
    num_expedient           TEXT,
    data_document           TEXT,
    entitat_organisme       TEXT,
    proveidor_adjudicatari  TEXT,
    import_                 TEXT,
    tipus_procediment       TEXT,
    model                   TEXT,
    prompt_version          TEXT,
    source                  TEXT,
    correction_instruction  TEXT,
    archived_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_clshist_doc ON classification_history(doc_id);

CREATE TABLE IF NOT EXISTS events (
    event_id      BIGSERIAL PRIMARY KEY,
    expedient_id  TEXT,
    doc_id        TEXT,
    "user"        TEXT,
    type          TEXT NOT NULL,
    payload_json  JSONB,
    ts            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_expedient ON events(expedient_id);

-- Seed the 5 categories (idempotent; updates description/order if labels already exist).
INSERT INTO taxonomy_categories (label, description, sort_order) VALUES
  ('Resolucio adjudicacio',              'Resolucion que adjudica un contrato/expediente a un proveedor', 1),
  ('Notificacio adjudicatari',           'Notificacion oficial al adjudicatario informando de la adjudicacion', 2),
  ('Publicacio adj - PSCP',              'Anuncio o publicacion de la adjudicacion en la Plataforma de Serveis de Contractacio Publica (PSCP)', 3),
  ('Inf fiscalitza fav doc DR_adjudic',  'Informe de fiscalizacion favorable, fase de adjudicacion (DR)', 4),
  ('Inf fiscalitza fav foc RD_prepar',   'Informe de fiscalizacion favorable, fase de preparacion (RD)', 5)
ON CONFLICT (label) DO UPDATE
  SET description = EXCLUDED.description, sort_order = EXCLUDED.sort_order, active = TRUE;
