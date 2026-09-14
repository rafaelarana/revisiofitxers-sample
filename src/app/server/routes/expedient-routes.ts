// RevisioFitxers — routes over the Lakebase `revisiofitxers` state store + the landing Volume.
// Phase 3: read/search (list/detail/history/taxonomy). Phase 4: create expedient + upload
// docs to landing/<id>/raw/ (the file_arrival trigger then auto-runs the classify pipeline).
// The app SP is granted USAGE/SELECT/INSERT/UPDATE on the schema + WRITE_VOLUME on the volume
// (scripts/grant_app_access.py, folded into setup.sh). No app-side unzip — the pipeline unpacks.

import { Request, Application, raw } from 'express';
import { z } from 'zod';

// Structural minimum of the AppKit handle this module uses. Kept loose (the concrete
// AppKit PluginMap type is generated per-plugin-set); we only touch these members.
interface AppKit {
  lakebase: {
    query(text: string, params?: unknown[]): Promise<{ rows: Record<string, unknown>[] }>;
  };
  files: (key: string) => {
    upload(filePath: string, contents: Buffer, options?: { overwrite?: boolean }): Promise<void>;
    download(filePath: string): Promise<DownloadResult>;
    exists(filePath: string): Promise<boolean>;
  };
  jobs: (key: string) => {
    runNow(params?: Record<string, unknown>): Promise<{ ok: boolean; error?: string; data?: { run_id?: number } }>;
    getRun(runId: number): Promise<{ ok: boolean; error?: string; data?: JobRun }>;
  };
  server: {
    extend(fn: (app: Application) => void): void;
  };
}

// Web ReadableStream (structural — avoids depending on the global lib type).
interface WebReadable {
  getReader(): { read(): Promise<{ done: boolean; value?: Uint8Array }> } ;
}
// Databricks SDK files download response (subset).
interface DownloadResult {
  'content-type'?: string;
  'content-length'?: number;
  contents?: WebReadable;
}

// Minimal shape of a Databricks job run (only the task-state fields the diagram needs).
interface JobRun {
  tasks?: Array<{
    task_key?: string;
    state?: { life_cycle_state?: string; result_state?: string };
  }>;
}

const SCHEMA = 'revisiofitxers';
// Expedient id: letters/digits/dash/underscore, 1–63 chars (configurable).
const EXPEDIENT_ID_RE = /^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$/;
// Job accessor key: single-job mode (DATABRICKS_JOB_ID) exposes the job under "default".
const INGEST_JOB_KEY = 'default';
// Revisor/Admin emails (comma-separated) that may see ALL users' expedients — spec Q-f,
// pending customer sign-off. Everyone else sees only their own. Empty by default.
const REVISOR_EMAILS = (process.env.REVISOR_EMAILS ?? '')
  .split(',')
  .map((s) => s.trim().toLowerCase())
  .filter(Boolean);

// The signed-in user's email, forwarded by the Databricks Apps OAuth proxy.
function callerEmail(req: Request): string {
  return (
    ((req.headers['x-forwarded-email'] as string) ||
      (req.headers['x-forwarded-user'] as string) ||
      '') as string
  )
    .trim()
    .toLowerCase();
}

// Whether this caller may see every user's expedients (revisor/admin) rather than only
// their own. When the proxy forwards no identity (e.g. local dev), fall back to see-all
// so the app stays usable — but log it, since on the deployed app an email is always present.
function seesAll(email: string): boolean {
  if (email === '') {
    console.warn('[revisiofitxers] no forwarded identity — scoping disabled (see-all fallback)');
    return true;
  }
  return REVISOR_EMAILS.includes(email);
}

// ── Processing-progress model (drives the workflow diagram) ─────────────────
type StageState = 'pending' | 'current' | 'running' | 'done' | 'error' | 'skipped';

interface Stage {
  key: string;
  label: string;
  hint?: string;
  state: StageState;
}

// The two real job tasks, in order, mapped to human labels for the diagram.
const PIPELINE_TASKS: Array<{ key: string; taskKey: string; label: string; hint: string }> = [
  {
    key: 'ingest',
    taskKey: 'autoloader_ingest',
    label: 'Ingesta i normalització',
    hint: 'Descompressió (ZIP/.eml), extracció de text i deduplicació',
  },
  {
    key: 'classify',
    taskKey: 'classify_merge',
    label: 'Classificació amb IA',
    hint: 'ai_parse · ai_classify · ai_extract · resum en català',
  },
];

function mapTaskState(state?: { life_cycle_state?: string; result_state?: string }): StageState {
  if (!state) return 'pending';
  const lc = state.life_cycle_state;
  const rs = state.result_state;
  if (rs === 'FAILED' || rs === 'TIMEDOUT' || lc === 'INTERNAL_ERROR') return 'error';
  if (rs === 'SUCCESS' || lc === 'TERMINATED') return 'done';
  if (lc === 'RUNNING' || lc === 'TERMINATING') return 'running';
  if (lc === 'SKIPPED') return 'skipped';
  return 'pending'; // PENDING, QUEUED, BLOCKED, WAITING_FOR_RETRY, …
}

// High-level expedient lifecycle: Esborrany → Processant → A punt / Cal revisió.
function buildLifecycle(status: string): Stage[] {
  const terminal = status === 'READY' || status === 'NEEDS_REVIEW' || status === 'CLOSED';
  const resultLabel =
    status === 'READY'
      ? 'A punt'
      : status === 'NEEDS_REVIEW'
        ? 'Cal revisió'
        : status === 'CLOSED'
          ? 'Tancat'
          : 'Resultats';
  return [
    { key: 'draft', label: 'Esborrany', state: status === 'DRAFT' ? 'current' : 'done' },
    {
      key: 'processing',
      label: 'Processant',
      state: status === 'DRAFT' ? 'pending' : status === 'PROCESSING' ? 'running' : 'done',
    },
    {
      key: 'result',
      label: resultLabel,
      state: status === 'NEEDS_REVIEW' ? 'error' : terminal ? 'done' : 'pending',
    },
  ];
}

// Per-task stages. Uses live job-run task states while processing; otherwise infers from status.
function buildTaskStages(status: string, run?: JobRun): Stage[] {
  const terminal = status === 'READY' || status === 'NEEDS_REVIEW' || status === 'CLOSED';
  return PIPELINE_TASKS.map((t, i) => {
    let state: StageState;
    if (run?.tasks) {
      state = mapTaskState(run.tasks.find((rt) => rt.task_key === t.taskKey)?.state);
    } else if (terminal) {
      state = 'done';
    } else if (status === 'PROCESSING') {
      state = i === 0 ? 'running' : 'pending'; // no run detail yet — assume first task started
    } else {
      state = 'pending';
    }
    return { key: t.key, label: t.label, hint: t.hint, state };
  });
}

// ── File download / preview ─────────────────────────────────────────────────
const CONTENT_TYPES: Record<string, string> = {
  pdf: 'application/pdf',
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  tif: 'image/tiff',
  tiff: 'image/tiff',
  txt: 'text/plain; charset=utf-8',
  csv: 'text/csv',
  doc: 'application/msword',
  docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
};

function fileExt(name: string): string {
  const m = /\.([A-Za-z0-9]+)$/.exec(name);
  return m ? m[1].toLowerCase() : '';
}

async function readAll(stream: WebReadable): Promise<Buffer> {
  const reader = stream.getReader();
  const chunks: Buffer[] = [];
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    if (value) chunks.push(Buffer.from(value));
  }
  return Buffer.concat(chunks);
}

export async function setupExpedientRoutes(appkit: AppKit) {
  // Sanity check: can we see the state schema? Log clearly if not (SP grant missing).
  try {
    const { rows } = await appkit.lakebase.query(
      `SELECT count(*)::int AS n FROM ${SCHEMA}.taxonomy_categories`,
    );
    console.log(`[revisiofitxers] state store reachable; taxonomy rows: ${rows[0]?.n}`);
  } catch (err) {
    console.warn('[revisiofitxers] cannot read state schema yet:', (err as Error).message);
    console.warn('[revisiofitxers] grant the app SP USAGE/SELECT on the schema (see build-plan Phase 3).');
  }

  appkit.server.extend((app) => {
    // List expedients (optionally filtered by status), scoped to the signed-in user —
    // revisors/admins (REVISOR_EMAILS) see all. FR-S4 per-user visibility (spec Q-f).
    app.get('/api/expedients', async (req, res) => {
      try {
        const status = typeof req.query.status === 'string' ? req.query.status : null;
        const email = callerEmail(req);
        const ownerFilter = seesAll(email) ? null : email;
        const result = await appkit.lakebase.query(
          `SELECT expedient_id, title, description, owner, status,
                  docs_total, docs_classified, docs_error, created_at, updated_at, closed_at
             FROM ${SCHEMA}.expedients
            WHERE ($1::text IS NULL OR status = $1)
              AND ($2::text IS NULL OR lower(owner) = $2)
            ORDER BY updated_at DESC
            LIMIT 500`,
          [status, ownerFilter],
        );
        res.json(result.rows);
      } catch (err) {
        console.error('list expedients failed:', err);
        res.status(500).json({ error: 'Failed to list expedients' });
      }
    });

    // Expedient detail: the expedient + its documents + latest classification per doc.
    app.get('/api/expedients/:id', async (req, res) => {
      try {
        const id = req.params.id;
        const exp = await appkit.lakebase.query(
          `SELECT expedient_id, title, description, owner, status,
                  docs_total, docs_classified, docs_error, created_at, updated_at, closed_at
             FROM ${SCHEMA}.expedients WHERE expedient_id = $1`,
          [id],
        );
        if (exp.rows.length === 0) {
          res.status(404).json({ error: 'Expedient not found' });
          return;
        }
        // Ownership check: only the owner or a revisor/admin may view an expedient.
        const email = callerEmail(req);
        const owner = String(exp.rows[0].owner ?? '').toLowerCase();
        if (!seesAll(email) && owner !== email) {
          res.status(403).json({ error: 'No teniu accés a aquest expedient' });
          return;
        }
        const docs = await appkit.lakebase.query(
          `SELECT d.doc_id, d.original_filename, d.original_lineage, d.staged_kind, d.status,
                  d.error_message,
                  c.titol, c.resum, c.classificacio, c.confidence,
                  c.num_expedient, c.data_document, c.entitat_organisme,
                  c.proveidor_adjudicatari, c.import_, c.tipus_procediment,
                  c.source, c.correction_instruction, c.created_at AS classified_at
             FROM ${SCHEMA}.documents d
             LEFT JOIN ${SCHEMA}.classifications c USING (doc_id)
            WHERE d.expedient_id = $1
            ORDER BY d.original_filename`,
          [id],
        );
        res.json({ expedient: exp.rows[0], documents: docs.rows });
      } catch (err) {
        console.error('expedient detail failed:', err);
        res.status(500).json({ error: 'Failed to load expedient' });
      }
    });

    // Classification history for a document (audit timeline; non-destructive corrections).
    app.get('/api/documents/:docId/history', async (req, res) => {
      try {
        const docId = req.params.docId;
        const result = await appkit.lakebase.query(
          `SELECT history_id, classificacio, titol, source, correction_instruction, archived_at
             FROM ${SCHEMA}.classification_history
            WHERE doc_id = $1
            ORDER BY archived_at DESC`,
          [docId],
        );
        res.json(result.rows);
      } catch (err) {
        console.error('history failed:', err);
        res.status(500).json({ error: 'Failed to load history' });
      }
    });

    // Governed taxonomy (the 5 categories). App + pipeline both read from here.
    app.get('/api/taxonomy', async (_req, res) => {
      try {
        const result = await appkit.lakebase.query(
          `SELECT label, description, sort_order, active
             FROM ${SCHEMA}.taxonomy_categories
            WHERE active
            ORDER BY sort_order`,
        );
        res.json(result.rows);
      } catch (err) {
        console.error('taxonomy failed:', err);
        res.status(500).json({ error: 'Failed to load taxonomy' });
      }
    });

    // Aggregate stats for the dashboard header (counts + classification breakdown).
    app.get('/api/stats', async (_req, res) => {
      try {
        const totals = await appkit.lakebase.query(
          `SELECT
             (SELECT count(*)::int FROM ${SCHEMA}.expedients) AS expedients,
             (SELECT count(*)::int FROM ${SCHEMA}.documents) AS documents,
             (SELECT count(*)::int FROM ${SCHEMA}.documents WHERE status = 'ERROR') AS errors`,
        );
        const byCat = await appkit.lakebase.query(
          `SELECT classificacio AS label, count(*)::int AS n
             FROM ${SCHEMA}.classifications
            WHERE classificacio IS NOT NULL
            GROUP BY classificacio ORDER BY n DESC`,
        );
        res.json({ totals: totals.rows[0], byCategory: byCat.rows });
      } catch (err) {
        console.error('stats failed:', err);
        res.status(500).json({ error: 'Failed to load stats' });
      }
    });

    // ── Phase 4: create expedient ─────────────────────────────────────────────
    app.post('/api/expedients', async (req, res) => {
      try {
        const parsed = z
          .object({
            expedient_id: z.string().regex(EXPEDIENT_ID_RE, 'ID no vàlid'),
            title: z.string().optional(),
            description: z.string().optional(),
          })
          .safeParse(req.body);
        if (!parsed.success) {
          res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'Dades no vàlides' });
          return;
        }
        const { expedient_id, title, description } = parsed.data;
        // owner: the signed-in user (forwarded by the platform), else the SP.
        const owner =
          (req.headers['x-forwarded-email'] as string) ||
          (req.headers['x-forwarded-user'] as string) ||
          'app';
        const existing = await appkit.lakebase.query(
          `SELECT 1 FROM ${SCHEMA}.expedients WHERE expedient_id = $1`,
          [expedient_id],
        );
        if (existing.rows.length > 0) {
          res.status(409).json({ error: 'Aquest expedient ja existeix' });
          return;
        }
        await appkit.lakebase.query(
          `INSERT INTO ${SCHEMA}.expedients (expedient_id, title, description, owner, status)
           VALUES ($1, $2, $3, $4, 'DRAFT')`,
          [expedient_id, title ?? null, description ?? null, owner],
        );
        await appkit.lakebase.query(
          `INSERT INTO ${SCHEMA}.events (expedient_id, "user", type, payload_json)
           VALUES ($1, $2, 'EXPEDIENT_CREATED', '{}'::jsonb)`,
          [expedient_id, owner],
        );
        res.status(201).json({ expedient_id });
      } catch (err) {
        console.error('create expedient failed:', err);
        res.status(500).json({ error: 'No s’ha pogut crear l’expedient' });
      }
    });

    // ── Phase 4: upload a document to landing/<id>/raw/ ───────────────────────
    // One file per request as raw bytes (application/octet-stream), filename in ?filename.
    // The file_arrival trigger on landing/ then auto-runs ingest + classify. NO app-side unzip.
    app.post(
      '/api/expedients/:id/uploads',
      raw({ type: '*/*', limit: '500mb' }),
      async (req, res) => {
        try {
          const id = req.params.id;
          if (!EXPEDIENT_ID_RE.test(id)) {
            res.status(400).json({ error: 'ID no vàlid' });
            return;
          }
          const filename = typeof req.query.filename === 'string' ? req.query.filename : '';
          // Guard against path traversal; keep just the base name.
          const safeName = filename.replace(/[/\\]/g, '_').replace(/^\.+/, '');
          if (!safeName) {
            res.status(400).json({ error: 'Falta el nom del fitxer (?filename=)' });
            return;
          }
          const body = req.body as Buffer;
          if (!Buffer.isBuffer(body) || body.length === 0) {
            res.status(400).json({ error: 'Cos del fitxer buit' });
            return;
          }
          const exp = await appkit.lakebase.query(
            `SELECT status FROM ${SCHEMA}.expedients WHERE expedient_id = $1`,
            [id],
          );
          if (exp.rows.length === 0) {
            res.status(404).json({ error: 'Expedient no trobat' });
            return;
          }
          if (exp.rows[0].status === 'CLOSED') {
            res.status(409).json({ error: 'Expedient tancat (només lectura)' });
            return;
          }
          // Write verbatim into the volume under landing/<id>/raw/; the pipeline unpacks/classifies.
          // The "files" volume maps to the raw_docs volume root, so paths are relative to it.
          await appkit.files('files').upload(`landing/${id}/raw/${safeName}`, body, {
            overwrite: true,
          });
          const owner =
            (req.headers['x-forwarded-email'] as string) ||
            (req.headers['x-forwarded-user'] as string) ||
            'app';
          await appkit.lakebase.query(
            `UPDATE ${SCHEMA}.expedients SET status = 'PROCESSING', updated_at = now()
             WHERE expedient_id = $1 AND status <> 'CLOSED'`,
            [id],
          );
          await appkit.lakebase.query(
            `INSERT INTO ${SCHEMA}.events (expedient_id, "user", type, payload_json)
             VALUES ($1, $2, 'FILES_UPLOADED', $3::jsonb)`,
            [id, owner, JSON.stringify({ filename: safeName, bytes: body.length })],
          );
          res.status(201).json({ filename: safeName, bytes: body.length });
        } catch (err) {
          console.error('upload failed:', err);
          res.status(500).json({ error: 'No s’ha pogut pujar el fitxer' });
        }
      },
    );

    // ── Phase 4: deterministically run ingest+classify ────────────────────────
    // Called by the client once, after an upload batch finishes. Triggers the job via
    // the jobs() plugin (app SP has CAN_MANAGE_RUN) rather than waiting on the flaky
    // file_arrival trigger, so "upload → results" latency is predictable in the demo.
    app.post('/api/expedients/:id/process', async (req, res) => {
      try {
        const id = req.params.id;
        if (!EXPEDIENT_ID_RE.test(id)) {
          res.status(400).json({ error: 'ID no vàlid' });
          return;
        }
        const exp = await appkit.lakebase.query(
          `SELECT owner FROM ${SCHEMA}.expedients WHERE expedient_id = $1`,
          [id],
        );
        if (exp.rows.length === 0) {
          res.status(404).json({ error: 'Expedient no trobat' });
          return;
        }
        const email = callerEmail(req);
        const owner = String(exp.rows[0].owner ?? '').toLowerCase();
        if (!seesAll(email) && owner !== email) {
          res.status(403).json({ error: 'No teniu accés a aquest expedient' });
          return;
        }
        // Job params carry deploy-time defaults (catalog/schema/volume/Lakebase), so no args needed.
        const run = await appkit.jobs(INGEST_JOB_KEY).runNow();
        if (!run.ok) {
          console.error('run ingest job failed:', run.error);
          res.status(502).json({ error: 'No s’ha pogut iniciar el processament' });
          return;
        }
        // Remember the run so the UI can show live per-task progress for this expedient.
        const runId = run.data?.run_id ?? null;
        if (runId != null) {
          await appkit.lakebase.query(
            `UPDATE ${SCHEMA}.expedients SET last_run_id = $1, updated_at = now() WHERE expedient_id = $2`,
            [runId, id],
          );
        }
        res.status(202).json({ started: true, run_id: runId });
      } catch (err) {
        console.error('process trigger failed:', err);
        res.status(500).json({ error: 'No s’ha pogut iniciar el processament' });
      }
    });

    // ── Live processing progress for the workflow diagram ─────────────────────
    // Returns the expedient lifecycle + per-task states (live from the job run while
    // PROCESSING) + doc counters. The client polls this while status === 'PROCESSING'.
    app.get('/api/expedients/:id/progress', async (req, res) => {
      try {
        const id = req.params.id;
        const exp = await appkit.lakebase.query(
          `SELECT owner, status, docs_total, docs_classified, docs_error, last_run_id
             FROM ${SCHEMA}.expedients WHERE expedient_id = $1`,
          [id],
        );
        if (exp.rows.length === 0) {
          res.status(404).json({ error: 'Expedient no trobat' });
          return;
        }
        const row = exp.rows[0];
        const email = callerEmail(req);
        if (!seesAll(email) && String(row.owner ?? '').toLowerCase() !== email) {
          res.status(403).json({ error: 'No teniu accés a aquest expedient' });
          return;
        }
        const status = String(row.status);

        // Pull live task states from the triggering run only while still processing.
        let run: JobRun | undefined;
        const runId = row.last_run_id != null ? Number(row.last_run_id) : null;
        if (status === 'PROCESSING' && runId != null) {
          const r = await appkit.jobs(INGEST_JOB_KEY).getRun(runId);
          if (r.ok) run = r.data;
        }

        res.json({
          status,
          docs: {
            total: Number(row.docs_total ?? 0),
            classified: Number(row.docs_classified ?? 0),
            error: Number(row.docs_error ?? 0),
          },
          lifecycle: buildLifecycle(status),
          tasks: buildTaskStages(status, run),
        });
      } catch (err) {
        console.error('progress failed:', err);
        res.status(500).json({ error: 'No s’ha pogut carregar el progrés' });
      }
    });

    // ── Signed-in user (for the top-right badge) ──────────────────────────────
    app.get('/api/whoami', (req, res) => {
      const email = ((req.headers['x-forwarded-email'] as string) || '').trim();
      const user = ((req.headers['x-forwarded-user'] as string) || '').trim();
      const preferred = ((req.headers['x-forwarded-preferred-username'] as string) || '').trim();
      const name = preferred || (email ? email.split('@')[0] : '') || user || 'Usuari';
      res.json({
        email,
        user,
        name,
        isRevisor: email !== '' && REVISOR_EMAILS.includes(email.toLowerCase()),
      });
    });

    // ── Download / inline-preview a document's file (ownership-scoped) ────────
    // disposition=attachment → download; otherwise inline (for in-app PDF/image preview).
    app.get('/api/expedients/:id/documents/:docId/file', async (req, res) => {
      try {
        const id = req.params.id;
        const docId = req.params.docId;
        const info = await appkit.lakebase.query(
          `SELECT d.staged_name, d.staged_kind, d.original_filename, e.owner
             FROM ${SCHEMA}.documents d
             JOIN ${SCHEMA}.expedients e ON e.expedient_id = d.expedient_id
            WHERE d.doc_id = $1 AND d.expedient_id = $2`,
          [docId, id],
        );
        if (info.rows.length === 0) {
          res.status(404).json({ error: 'Document no trobat' });
          return;
        }
        const row = info.rows[0];
        const email = callerEmail(req);
        if (!seesAll(email) && String(row.owner ?? '').toLowerCase() !== email) {
          res.status(403).json({ error: 'No teniu accés a aquest document' });
          return;
        }
        const stagedName = String(row.staged_name ?? '');
        if (!stagedName) {
          res.status(404).json({ error: 'Fitxer no disponible' });
          return;
        }
        // staged_kind isn't always recorded → try binary then text under staging/<exp>/.
        const kinds = row.staged_kind ? [String(row.staged_kind)] : ['binary', 'text'];
        const vol = appkit.files('files');
        let path: string | null = null;
        for (const k of kinds) {
          const p = `staging/${id}/${k}/${stagedName}`;
          try {
            if (await vol.exists(p)) {
              path = p;
              break;
            }
          } catch {
            /* keep trying */
          }
        }
        if (!path) {
          res.status(404).json({ error: 'Fitxer no trobat al volum' });
          return;
        }
        const dl = await vol.download(path);
        if (!dl.contents) {
          res.status(404).json({ error: 'Sense contingut' });
          return;
        }
        const ext = fileExt(stagedName) || fileExt(String(row.original_filename ?? ''));
        const ctype = CONTENT_TYPES[ext] ?? dl['content-type'] ?? 'application/octet-stream';
        const wantDownload = req.query.disposition === 'attachment';
        const rawName = String(row.original_filename ?? stagedName);
        // ASCII fallback + RFC 5987 UTF-8 name so accented filenames download correctly.
        const asciiName = rawName.replace(/[^\x20-\x7E]/g, '_').replace(/["\\\r\n]/g, '_');
        const utf8Name = encodeURIComponent(rawName);
        const buf = await readAll(dl.contents);
        res.setHeader('Content-Type', ctype);
        res.setHeader(
          'Content-Disposition',
          `${wantDownload ? 'attachment' : 'inline'}; filename="${asciiName}"; filename*=UTF-8''${utf8Name}`,
        );
        res.setHeader('Content-Length', String(buf.length));
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.end(buf);
      } catch (err) {
        console.error('file serve failed:', err);
        res.status(500).json({ error: 'No s’ha pogut obrir el fitxer' });
      }
    });
  });
}
