import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Button,
  Input,
  Label,
  Badge,
  Checkbox,
  Separator,
  Skeleton,
  Empty,
  EmptyHeader,
  EmptyTitle,
  EmptyDescription,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogTrigger,
} from '@databricks/appkit-ui/react';
import { useState, useEffect, useCallback, useRef } from 'react';
import {
  Search,
  FileText,
  AlertTriangle,
  Plus,
  Upload,
  Loader2,
  ChevronRight,
  CheckCircle2,
  Circle,
  CircleDot,
  FolderClosed,
  FolderOpen,
  Inbox,
  FilterX,
  Eye,
  Download,
  X,
} from 'lucide-react';

// File types the browser can render inline (in-app preview panel).
const PREVIEW_EXT = new Set(['pdf', 'png', 'jpg', 'jpeg', 'gif', 'tif', 'tiff', 'txt']);
function previewable(name: string | null): boolean {
  const m = /\.([A-Za-z0-9]+)$/.exec(name ?? '');
  return m ? PREVIEW_EXT.has(m[1].toLowerCase()) : false;
}

interface Expedient {
  expedient_id: string;
  title: string | null;
  description: string | null;
  owner: string;
  status: string;
  docs_total: number;
  docs_classified: number;
  docs_error: number;
  created_at: string;
  updated_at: string;
}

interface DocRow {
  doc_id: string;
  original_filename: string | null;
  original_lineage: string | null;
  status: string;
  error_message: string | null;
  titol: string | null;
  resum: string | null;
  classificacio: string | null;
  num_expedient: string | null;
  data_document: string | null;
  entitat_organisme: string | null;
  proveidor_adjudicatari: string | null;
  import_: string | null;
  tipus_procediment: string | null;
  source: string | null;
}

type StageState = 'pending' | 'current' | 'running' | 'done' | 'error' | 'skipped';
interface Stage {
  key: string;
  label: string;
  hint?: string;
  state: StageState;
}
interface Progress {
  status: string;
  docs: { total: number; classified: number; error: number };
  lifecycle: Stage[];
  tasks: Stage[];
}

const STATUS_VARIANT: Record<string, string> = {
  READY: 'default',
  PROCESSING: 'secondary',
  NEEDS_REVIEW: 'destructive',
  CLOSED: 'outline',
  DRAFT: 'outline',
};
const STATUS_LABEL: Record<string, string> = {
  DRAFT: 'Esborrany',
  PROCESSING: 'Processant',
  READY: 'A punt',
  NEEDS_REVIEW: 'Cal revisió',
  CLOSED: 'Tancat',
};
const STATUS_ORDER = ['DRAFT', 'PROCESSING', 'READY', 'NEEDS_REVIEW', 'CLOSED'];

function StatusBadge({ status }: { status: string }) {
  return (
    <Badge variant={(STATUS_VARIANT[status] ?? 'secondary') as never}>
      {STATUS_LABEL[status] ?? status}
    </Badge>
  );
}

// Upload files one-by-one to the landing volume, then trigger one job run for the batch.
async function uploadFiles(expedientId: string, files: FileList | File[]): Promise<void> {
  const encId = encodeURIComponent(expedientId);
  for (const file of Array.from(files)) {
    const res = await fetch(
      `/api/expedients/${encId}/uploads?filename=${encodeURIComponent(file.name)}`,
      { method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: file },
    );
    if (!res.ok) {
      const msg = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(`${file.name}: ${msg.error ?? res.statusText}`);
    }
  }
  const proc = await fetch(`/api/expedients/${encId}/process`, { method: 'POST' });
  if (!proc.ok) {
    const msg = await proc.json().catch(() => ({ error: proc.statusText }));
    throw new Error(`Processament: ${msg.error ?? proc.statusText}`);
  }
}

// ── Facets ────────────────────────────────────────────────────────────────
type DatePreset = 'all' | 'today' | '7d' | '30d';
interface Facets {
  statuses: Set<string>;
  date: DatePreset;
  errorsOnly: boolean;
  search: string;
}
const EMPTY_FACETS: Facets = { statuses: new Set(), date: 'all', errorsOnly: false, search: '' };

function dateCutoff(preset: DatePreset): number {
  const now = Date.now();
  if (preset === 'today') return now - 24 * 3600 * 1000;
  if (preset === '7d') return now - 7 * 24 * 3600 * 1000;
  if (preset === '30d') return now - 30 * 24 * 3600 * 1000;
  return 0;
}

function applyFacets(list: Expedient[], f: Facets): Expedient[] {
  const cutoff = dateCutoff(f.date);
  const q = f.search.trim().toLowerCase();
  return list.filter((e) => {
    if (f.statuses.size > 0 && !f.statuses.has(e.status)) return false;
    if (cutoff > 0 && new Date(e.created_at).getTime() < cutoff) return false;
    if (f.errorsOnly && (e.docs_error ?? 0) === 0) return false;
    if (q && !e.expedient_id.toLowerCase().includes(q) && !(e.title ?? '').toLowerCase().includes(q))
      return false;
    return true;
  });
}

function FacetsPanel({
  all,
  facets,
  setFacets,
}: {
  all: Expedient[];
  facets: Facets;
  setFacets: (f: Facets) => void;
}) {
  const counts: Record<string, number> = {};
  for (const e of all) counts[e.status] = (counts[e.status] ?? 0) + 1;
  const present = STATUS_ORDER.filter((s) => counts[s]);
  const active =
    facets.statuses.size > 0 || facets.date !== 'all' || facets.errorsOnly || facets.search !== '';

  const toggleStatus = (s: string) => {
    const next = new Set(facets.statuses);
    if (next.has(s)) next.delete(s);
    else next.add(s);
    setFacets({ ...facets, statuses: next });
  };

  return (
    <div className="space-y-4">
      <div className="relative">
        <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
        <Input
          className="pl-8 h-9"
          placeholder="Cerca per ID o títol"
          value={facets.search}
          onChange={(e) => setFacets({ ...facets, search: e.target.value })}
        />
      </div>

      <div className="space-y-2">
        <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
          Estat
        </Label>
        {present.length === 0 ? (
          <p className="text-xs text-muted-foreground">—</p>
        ) : (
          present.map((s) => (
            <label key={s} className="flex items-center gap-2 text-sm cursor-pointer">
              <Checkbox
                checked={facets.statuses.has(s)}
                onCheckedChange={() => toggleStatus(s)}
              />
              <span className="flex-1">{STATUS_LABEL[s] ?? s}</span>
              <span className="text-xs text-muted-foreground tabular-nums">{counts[s]}</span>
            </label>
          ))
        )}
      </div>

      <Separator />

      <div className="space-y-2">
        <Label className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
          Data de creació
        </Label>
        <Select
          value={facets.date}
          onValueChange={(v) => setFacets({ ...facets, date: v as DatePreset })}
        >
          <SelectTrigger className="h-9">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">Qualsevol data</SelectItem>
            <SelectItem value="today">Últimes 24 h</SelectItem>
            <SelectItem value="7d">Últims 7 dies</SelectItem>
            <SelectItem value="30d">Últims 30 dies</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <label className="flex items-center gap-2 text-sm cursor-pointer">
        <Checkbox
          checked={facets.errorsOnly}
          onCheckedChange={(c) => setFacets({ ...facets, errorsOnly: c === true })}
        />
        <span>Només amb errors</span>
      </label>

      {active && (
        <Button variant="ghost" size="sm" className="gap-1 w-full" onClick={() => setFacets({ ...EMPTY_FACETS })}>
          <FilterX className="h-4 w-4" /> Neteja els filtres
        </Button>
      )}
    </div>
  );
}

// ── Expedient / file tree ───────────────────────────────────────────────────
function docIcon(status: string) {
  if (status === 'ERROR') return <AlertTriangle className="h-3.5 w-3.5 text-destructive shrink-0" />;
  return <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />;
}

function TreeNode({
  exp,
  selected,
  onSelect,
}: {
  exp: Expedient;
  selected: boolean;
  onSelect: (id: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [docs, setDocs] = useState<DocRow[] | null>(null);
  const [loadingDocs, setLoadingDocs] = useState(false);

  const loadDocs = useCallback(() => {
    setLoadingDocs(true);
    fetch(`/api/expedients/${encodeURIComponent(exp.expedient_id)}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(r.statusText))))
      .then((d) => setDocs(d.documents as DocRow[]))
      .catch(() => setDocs([]))
      .finally(() => setLoadingDocs(false));
  }, [exp.expedient_id]);

  return (
    <Collapsible
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (o && docs === null) loadDocs();
      }}
    >
      <div
        className={`flex items-center gap-1 rounded-md px-1.5 py-1 text-sm cursor-pointer ${
          selected ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'
        }`}
      >
        <CollapsibleTrigger asChild>
          <button
            className="shrink-0 p-0.5 rounded hover:bg-muted-foreground/10"
            aria-label={open ? 'Replega' : 'Desplega'}
          >
            <ChevronRight
              className={`h-3.5 w-3.5 transition-transform ${open ? 'rotate-90' : ''}`}
            />
          </button>
        </CollapsibleTrigger>
        <button
          className="flex items-center gap-1.5 min-w-0 flex-1 text-left"
          onClick={() => onSelect(exp.expedient_id)}
        >
          {open ? (
            <FolderOpen className="h-4 w-4 shrink-0 text-primary" />
          ) : (
            <FolderClosed className="h-4 w-4 shrink-0 text-muted-foreground" />
          )}
          <span className="truncate flex-1">{exp.expedient_id}</span>
          {exp.docs_error > 0 && <AlertTriangle className="h-3.5 w-3.5 text-destructive shrink-0" />}
          {exp.status === 'PROCESSING' && (
            <Loader2 className="h-3.5 w-3.5 text-primary animate-spin shrink-0" />
          )}
        </button>
      </div>
      <CollapsibleContent>
        <div className="ml-6 border-l pl-2 py-0.5 space-y-0.5">
          {loadingDocs ? (
            <div className="space-y-1 py-1">
              <Skeleton className="h-4 w-full" />
              <Skeleton className="h-4 w-2/3" />
            </div>
          ) : docs && docs.length > 0 ? (
            docs.map((d) => (
              <button
                key={d.doc_id}
                className="flex items-center gap-1.5 w-full text-left rounded px-1.5 py-0.5 text-xs hover:bg-muted min-w-0"
                title={d.classificacio ?? d.original_filename ?? ''}
                onClick={() => onSelect(exp.expedient_id)}
              >
                {docIcon(d.status)}
                <span className="truncate">{d.titol ?? d.original_filename ?? d.doc_id}</span>
              </button>
            ))
          ) : (
            <p className="text-xs text-muted-foreground py-0.5 px-1.5">Sense documents</p>
          )}
        </div>
      </CollapsibleContent>
    </Collapsible>
  );
}

// ── Workflow diagram ────────────────────────────────────────────────────────
function StageIcon({ state }: { state: StageState }) {
  switch (state) {
    case 'done':
      return <CheckCircle2 className="h-5 w-5 text-primary" />;
    case 'running':
      return <Loader2 className="h-5 w-5 text-primary animate-spin" />;
    case 'current':
      return <CircleDot className="h-5 w-5 text-primary" />;
    case 'error':
      return <AlertTriangle className="h-5 w-5 text-destructive" />;
    case 'skipped':
      return <Circle className="h-5 w-5 text-muted-foreground/50" />;
    default:
      return <Circle className="h-5 w-5 text-muted-foreground/50" />;
  }
}

function WorkflowDiagram({ progress }: { progress: Progress }) {
  const isProcessing = progress.status === 'PROCESSING';
  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-medium text-muted-foreground">Flux de l'expedient</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Lifecycle stepper */}
        <div className="flex items-center">
          {progress.lifecycle.map((s, i) => (
            <div key={s.key} className="flex items-center flex-1 last:flex-none">
              <div className="flex flex-col items-center gap-1 shrink-0">
                <StageIcon state={s.state} />
                <span
                  className={`text-xs text-center ${
                    s.state === 'pending'
                      ? 'text-muted-foreground'
                      : s.state === 'error'
                        ? 'text-destructive font-medium'
                        : 'text-foreground font-medium'
                  }`}
                >
                  {s.label}
                </span>
              </div>
              {i < progress.lifecycle.length - 1 && (
                <div
                  className={`h-0.5 flex-1 mx-2 rounded ${
                    s.state === 'done' ? 'bg-primary' : 'bg-border'
                  }`}
                />
              )}
            </div>
          ))}
        </div>

        {/* Live per-task breakdown while processing */}
        {isProcessing && (
          <>
            <Separator />
            <div className="space-y-2">
              {progress.tasks.map((t) => (
                <div key={t.key} className="flex items-start gap-2">
                  <div className="mt-0.5">
                    <StageIcon state={t.state} />
                  </div>
                  <div className="min-w-0">
                    <div className="text-sm text-foreground">
                      {t.label}
                      {t.state === 'running' && (
                        <span className="text-primary text-xs ml-2">en curs…</span>
                      )}
                      {t.state === 'done' && (
                        <span className="text-muted-foreground text-xs ml-2">fet</span>
                      )}
                    </div>
                    {t.hint && <div className="text-xs text-muted-foreground">{t.hint}</div>}
                  </div>
                </div>
              ))}
              <p className="text-xs text-muted-foreground pt-1">
                El processament s'executa en serverless; els resultats apareixeran automàticament.
              </p>
            </div>
          </>
        )}

        {/* Doc summary once we have counts */}
        {progress.docs.total > 0 && (
          <div className="flex items-center gap-3 text-xs text-muted-foreground pt-1">
            <span className="flex items-center gap-1">
              <FileText className="h-3.5 w-3.5" />
              {progress.docs.classified}/{progress.docs.total} classificats
            </span>
            {progress.docs.error > 0 && (
              <span className="flex items-center gap-1 text-destructive">
                <AlertTriangle className="h-3.5 w-3.5" />
                {progress.docs.error} amb errors
              </span>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}

// ── Page ────────────────────────────────────────────────────────────────────
export function ExpedientsPage() {
  const [expedients, setExpedients] = useState<Expedient[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);
  const [facets, setFacets] = useState<Facets>({ ...EMPTY_FACETS });

  const load = useCallback(() => {
    setLoading(true);
    fetch('/api/expedients')
      .then((res) => {
        if (!res.ok) throw new Error(`Error carregant expedients: ${res.statusText}`);
        return res.json() as Promise<Expedient[]>;
      })
      .then(setExpedients)
      .catch((err) => setError(err instanceof Error ? err.message : 'Error'))
      .finally(() => setLoading(false));
  }, []);

  useEffect(load, [load]);

  const filtered = applyFacets(expedients, facets);

  return (
    <div className="flex flex-col md:flex-row gap-6 items-start">
      {/* Left: facets + tree */}
      <aside className="w-full md:w-72 shrink-0 md:sticky md:top-6 self-stretch md:self-start space-y-4">
        <div className="flex items-center justify-between gap-2">
          <h2 className="text-base font-semibold text-foreground">Els meus expedients</h2>
          <NewExpedientDialog onCreated={(id) => { load(); if (id) setSelected(id); }} />
        </div>

        <FacetsPanel all={expedients} facets={facets} setFacets={setFacets} />

        <Separator />

        <div className="max-h-[55vh] overflow-y-auto pr-1 -mr-1">
          {loading ? (
            <div className="space-y-1.5">
              {[0, 1, 2, 3].map((i) => (
                <Skeleton key={i} className="h-7 w-full" />
              ))}
            </div>
          ) : filtered.length === 0 ? (
            <p className="text-sm text-muted-foreground py-2">
              {expedients.length === 0
                ? "Encara no tens expedients. Crea'n un per començar."
                : 'Cap expedient coincideix amb els filtres.'}
            </p>
          ) : (
            <div className="space-y-0.5">
              {filtered.map((e) => (
                <TreeNode
                  key={e.expedient_id}
                  exp={e}
                  selected={selected === e.expedient_id}
                  onSelect={setSelected}
                />
              ))}
            </div>
          )}
        </div>
      </aside>

      {/* Right: detail */}
      <div className="flex-1 min-w-0 w-full">
        {error && (
          <Card className="border-destructive mb-4">
            <CardContent className="pt-6 text-sm text-destructive">{error}</CardContent>
          </Card>
        )}
        {selected ? (
          <ExpedientDetail id={selected} onChanged={load} />
        ) : (
          <Empty className="border rounded-lg py-16">
            <EmptyHeader>
              <Inbox className="h-10 w-10 text-muted-foreground mx-auto" />
              <EmptyTitle>Selecciona un expedient</EmptyTitle>
              <EmptyDescription>
                Tria un expedient de l'arbre de l'esquerra per veure'n els documents i la
                classificació, o crea'n un de nou.
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        )}
      </div>
    </div>
  );
}

function NewExpedientDialog({ onCreated }: { onCreated: (id?: string) => void }) {
  const [open, setOpen] = useState(false);
  const [id, setId] = useState('');
  const [title, setTitle] = useState('');
  const [filesToUpload, setFilesToUpload] = useState<File[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async () => {
    setError(null);
    setBusy(true);
    try {
      const res = await fetch('/api/expedients', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ expedient_id: id.trim(), title: title.trim() || undefined }),
      });
      if (!res.ok) {
        const msg = await res.json().catch(() => ({ error: res.statusText }));
        throw new Error(msg.error ?? 'Error');
      }
      if (filesToUpload.length > 0) await uploadFiles(id.trim(), filesToUpload);
      const createdId = id.trim();
      setOpen(false);
      setId('');
      setTitle('');
      setFilesToUpload([]);
      onCreated(createdId);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error');
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button size="sm" className="gap-1">
          <Plus className="h-4 w-4" /> Nou
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Nou expedient</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="exp-id">ID d'expedient</Label>
            <Input id="exp-id" placeholder="EXP-2026-000123" value={id} onChange={(e) => setId(e.target.value)} />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="exp-title">Títol (opcional)</Label>
            <Input id="exp-title" value={title} onChange={(e) => setTitle(e.target.value)} />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="exp-files">Documents (ZIP o fitxers individuals)</Label>
            <Input
              id="exp-files"
              type="file"
              multiple
              onChange={(e) => setFilesToUpload(Array.from(e.target.files ?? []))}
            />
            {filesToUpload.length > 0 && (
              <p className="text-xs text-muted-foreground">{filesToUpload.length} fitxer(s) seleccionat(s)</p>
            )}
          </div>
          {error && <p className="text-sm text-destructive">{error}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)} disabled={busy}>
            Cancel·la
          </Button>
          <Button onClick={submit} disabled={busy || !id.trim()} className="gap-1">
            {busy && <Loader2 className="h-4 w-4 animate-spin" />}
            Crea{filesToUpload.length > 0 ? ' i puja' : ''}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ExpedientDetail({ id, onChanged }: { id: string; onChanged: () => void }) {
  const [data, setData] = useState<{ expedient: Expedient; documents: DocRow[] } | null>(null);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [preview, setPreview] = useState<{ docId: string; filename: string } | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fileUrl = (docId: string, disposition?: 'attachment') =>
    `/api/expedients/${encodeURIComponent(id)}/documents/${encodeURIComponent(docId)}/file${
      disposition === 'attachment' ? '?disposition=attachment' : ''
    }`;

  const fetchDetail = useCallback(() => {
    return fetch(`/api/expedients/${encodeURIComponent(id)}`)
      .then((res) => {
        if (!res.ok) throw new Error(`Error carregant l'expedient: ${res.statusText}`);
        return res.json();
      })
      .then((d) => {
        setData(d);
        return d.expedient.status as string;
      });
  }, [id]);

  const fetchProgress = useCallback(() => {
    return fetch(`/api/expedients/${encodeURIComponent(id)}/progress`)
      .then((res) => (res.ok ? (res.json() as Promise<Progress>) : null))
      .then((p) => {
        if (p) setProgress(p);
      })
      .catch(() => {});
  }, [id]);

  // Initial load (reset state when switching expedients).
  useEffect(() => {
    setLoading(true);
    setData(null);
    setProgress(null);
    setError(null);
    setPreview(null);
    Promise.all([fetchDetail(), fetchProgress()])
      .catch((err) => setError(err instanceof Error ? err.message : 'Error'))
      .finally(() => setLoading(false));
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
      pollRef.current = null;
    };
  }, [id, fetchDetail, fetchProgress]);

  // While PROCESSING, poll detail + progress until READY/NEEDS_REVIEW.
  useEffect(() => {
    const status = data?.expedient.status;
    if (status === 'PROCESSING') {
      if (!pollRef.current) {
        pollRef.current = setInterval(() => {
          fetchDetail().then(() => onChanged()).catch(() => {});
          fetchProgress();
        }, 4000);
      }
    } else if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, [data?.expedient.status, fetchDetail, fetchProgress, onChanged]);

  const onUpload = async (files: FileList | null) => {
    if (!files || files.length === 0) return;
    setUploading(true);
    setError(null);
    try {
      await uploadFiles(id, files);
      await Promise.all([fetchDetail(), fetchProgress()]);
      onChanged();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error de pujada');
    } finally {
      setUploading(false);
      if (fileInput.current) fileInput.current.value = '';
    }
  };

  if (loading) return <Skeleton className="h-64 w-full" />;
  if (error)
    return (
      <Card className="border-destructive">
        <CardContent className="pt-6 text-sm text-destructive">{error}</CardContent>
      </Card>
    );
  if (!data) return null;

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-3 flex-wrap">
        <h2 className="text-2xl font-bold text-foreground">{data.expedient.expedient_id}</h2>
        <StatusBadge status={data.expedient.status} />
        {data.expedient.title && (
          <span className="text-sm text-muted-foreground">{data.expedient.title}</span>
        )}
        <div className="ml-auto">
          <input
            ref={fileInput}
            type="file"
            multiple
            className="hidden"
            onChange={(e) => onUpload(e.target.files)}
          />
          <Button
            size="sm"
            variant="outline"
            className="gap-1"
            disabled={uploading || data.expedient.status === 'CLOSED'}
            onClick={() => fileInput.current?.click()}
          >
            {uploading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Upload className="h-4 w-4" />}
            Afegeix documents
          </Button>
        </div>
      </div>

      {progress && <WorkflowDiagram progress={progress} />}

      {data.documents.length === 0 && data.expedient.status !== 'PROCESSING' && (
        <p className="text-muted-foreground text-sm">
          Encara no hi ha documents. Fes servir «Afegeix documents» per pujar un ZIP o fitxers.
        </p>
      )}

      <div className={preview ? 'flex flex-col lg:flex-row gap-4 items-start' : ''}>
        {/* Left panel: in-app file preview */}
        {preview && (
          <Card className="w-full lg:w-[55%] lg:sticky lg:top-4 shrink-0 overflow-hidden">
            <CardHeader className="pb-2 flex-row items-center justify-between gap-2 space-y-0">
              <CardTitle className="text-sm font-medium truncate">{preview.filename}</CardTitle>
              <div className="flex items-center gap-1 shrink-0">
                <Button asChild size="sm" variant="ghost" className="gap-1">
                  <a href={fileUrl(preview.docId, 'attachment')} download={preview.filename}>
                    <Download className="h-4 w-4" /> Descarrega
                  </a>
                </Button>
                <Button
                  size="icon"
                  variant="ghost"
                  className="h-8 w-8"
                  aria-label="Tanca la previsualització"
                  onClick={() => setPreview(null)}
                >
                  <X className="h-4 w-4" />
                </Button>
              </div>
            </CardHeader>
            <CardContent className="p-0">
              <iframe
                key={preview.docId}
                src={fileUrl(preview.docId)}
                title={preview.filename}
                className="w-full h-[72vh] border-0 bg-muted"
              />
            </CardContent>
          </Card>
        )}

        {/* Document list */}
        <div className={preview ? 'lg:flex-1 min-w-0 w-full grid gap-3' : 'grid gap-3'}>
          {data.documents.map((d) => {
            const canPreview = previewable(d.original_filename) && d.status !== 'ERROR';
            const label = d.original_filename ?? d.titol ?? d.doc_id;
            return (
              <Card
                key={d.doc_id}
                className={preview?.docId === d.doc_id ? 'border-primary' : undefined}
              >
                <CardHeader className="pb-2">
                  <div className="flex items-start justify-between gap-3">
                    <CardTitle className="text-base font-medium">{d.titol ?? d.original_filename}</CardTitle>
                    {d.classificacio && <Badge variant="secondary">{d.classificacio}</Badge>}
                  </div>
                  {d.original_lineage && (
                    <p className="text-xs text-muted-foreground font-mono truncate">{d.original_lineage}</p>
                  )}
                </CardHeader>
                <CardContent className="space-y-2 text-sm">
                  {d.status === 'ERROR' && (
                    <p className="text-destructive flex items-center gap-1">
                      <AlertTriangle className="h-4 w-4" /> {d.error_message ?? 'Error de processament'}
                    </p>
                  )}
                  {d.resum && <p className="text-muted-foreground">{d.resum}</p>}
                  <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
                    <Meta label="Núm. expedient" value={d.num_expedient} />
                    <Meta label="Data" value={d.data_document} />
                    <Meta label="Entitat" value={d.entitat_organisme} />
                    <Meta label="Adjudicatari" value={d.proveidor_adjudicatari} />
                    <Meta label="Import" value={d.import_} />
                    <Meta label="Procediment" value={d.tipus_procediment} />
                  </dl>
                  <div className="flex items-center gap-2 pt-1">
                    {canPreview && (
                      <Button
                        size="sm"
                        variant={preview?.docId === d.doc_id ? 'secondary' : 'outline'}
                        className="gap-1"
                        onClick={() => setPreview({ docId: d.doc_id, filename: label })}
                      >
                        <Eye className="h-4 w-4" /> Obre
                      </Button>
                    )}
                    <Button asChild size="sm" variant="ghost" className="gap-1">
                      <a href={fileUrl(d.doc_id, 'attachment')} download={label}>
                        <Download className="h-4 w-4" /> Descarrega
                      </a>
                    </Button>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function Meta({ label, value }: { label: string; value: string | null }) {
  if (!value) return null;
  return (
    <div>
      <dt className="text-muted-foreground inline">{label}: </dt>
      <dd className="inline text-foreground">{value}</dd>
    </div>
  );
}
