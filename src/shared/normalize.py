"""
Shared document-normalization logic for RevisioFitxers.

Extracted from src/notebooks/01_stage_and_load.py so the streaming pipeline, the
app backend, and unit tests all import ONE implementation (spec §8.2, build-plan 0.5).

Design goals:
- Pure Python, no dbutils / Spark / Volume dependencies. Callers pass bytes in and
  get structured results out; the caller decides where to persist (Volume, local, etc.).
- Recursively unpacks .zip (incl. nested) and detaches .eml attachments.
- Normalizes into two buckets, mirroring ai_parse_document's support matrix:
    * "binary" -> handed as-is to ai_parse_document downstream (PDF/DOC/DOCX/images/PPT)
    * "text"   -> pre-extracted text for formats ai_parse_document can't read (.xlsx, .eml)
- Content-hash dedup within a single normalization run.
- Preserves lineage back to the original path/filename ("a.zip > folder/b.pdf").

The public entry point is `normalize_files(...)`, which returns a NormalizeResult
holding the staged artifacts (in memory) + a manifest. A thin helper
`write_result_to_dir(...)` persists a result to a filesystem/Volume path in the same
staging/{binary,text}/ + manifest.json layout the pipeline expects.
"""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass, field
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Callable

# Extensions ai_parse_document handles directly (fed through as "binary").
BINARY_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".doc", ".docx", ".ppt", ".pptx"}
# Extensions we pre-extract to text (ai_parse_document can't read them).
XLSX_EXTS = {".xlsx", ".xlsm"}

LINEAGE_SEP = " > "


@dataclass
class StagedItem:
    """One normalized artifact ready to persist."""

    lineage: str  # e.g. "docs.zip > carpeta/annex.pdf"
    kind: str  # "binary" | "text" | "skipped" | "duplicate"
    original_filename: str  # leaf name of the lineage
    staged_name: str | None = None  # deterministic name under binary/ or text/
    content_hash: str | None = None
    # payloads (exactly one is set depending on kind)
    data: bytes | None = None  # for binary
    text: str | None = None  # for text
    duplicate_of: str | None = None  # staged_name of the first occurrence
    reason: str | None = None  # for skipped/errored


@dataclass
class NormalizeResult:
    items: list[StagedItem] = field(default_factory=list)
    n_source_files: int = 0

    @property
    def manifest(self) -> list[dict]:
        """JSON-serializable manifest mirroring the MVP's staging/manifest.json."""
        out = []
        for it in self.items:
            if it.kind == "duplicate":
                out.append({"lineage": it.lineage, "kind": "duplicate", "duplicate_of": it.duplicate_of})
            elif it.kind == "skipped":
                out.append({"lineage": it.lineage, "kind": "skipped", "reason": it.reason})
            else:
                out.append(
                    {
                        "lineage": it.lineage,
                        "kind": it.kind,
                        "staged_name": it.staged_name,
                        "original_filename": it.original_filename,
                    }
                )
        return out

    def counts(self) -> dict[str, int]:
        c: dict[str, int] = {}
        for it in self.items:
            c[it.kind] = c.get(it.kind, 0) + 1
        return c


def content_hash(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()[:12]


def safe_stem(lineage: str, suffix: str) -> str:
    """Deterministic staged filename from a lineage string (collision-resistant, path-safe)."""
    h = hashlib.sha1(lineage.encode()).hexdigest()[:16]
    return f"doc_{h}{suffix}"


def xlsx_to_text(data: bytes) -> str:
    """Dump every cell of every sheet to text (ai_parse_document can't read .xlsx)."""
    import openpyxl  # imported lazily so non-xlsx callers don't need it

    wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True, read_only=True)
    lines: list[str] = []
    for ws in wb.worksheets:
        lines.append(f"### Hoja: {ws.title}")
        for row in ws.iter_rows(values_only=True):
            cells = [str(c) for c in row if c is not None]
            if cells:
                lines.append(" | ".join(cells))
    return "\n".join(lines)


class _Normalizer:
    """Internal walker holding per-run dedup state."""

    def __init__(self) -> None:
        self.items: list[StagedItem] = []
        self._seen_hashes: dict[str, str] = {}  # content_hash -> staged_name

    def _register(self, lineage: str, data: bytes, suffix: str, kind: str, text: str | None = None) -> None:
        h = content_hash(data)
        if h in self._seen_hashes:
            self.items.append(
                StagedItem(
                    lineage=lineage,
                    kind="duplicate",
                    original_filename=_leaf(lineage),
                    content_hash=h,
                    duplicate_of=self._seen_hashes[h],
                )
            )
            return
        staged_name = safe_stem(lineage, suffix)
        item = StagedItem(
            lineage=lineage,
            kind=kind,
            original_filename=_leaf(lineage),
            staged_name=staged_name,
            content_hash=h,
        )
        if kind == "binary":
            item.data = data
        else:  # text
            item.text = text or ""
        self._seen_hashes[h] = staged_name
        self.items.append(item)

    def _skip(self, lineage: str, reason: str) -> None:
        self.items.append(
            StagedItem(lineage=lineage, kind="skipped", original_filename=_leaf(lineage), reason=reason)
        )

    def process_zip(self, lineage: str, data: bytes) -> None:
        try:
            zf = zipfile.ZipFile(io.BytesIO(data))
        except zipfile.BadZipFile:
            self._skip(lineage, "bad-zip")
            return
        with zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                self.process_entry(
                    f"{lineage}{LINEAGE_SEP}{info.filename}",
                    zf.read(info),
                    Path(info.filename).suffix.lower(),
                )

    def process_eml(self, lineage: str, data: bytes) -> None:
        msg = BytesParser(policy=policy.default).parsebytes(data)
        subject = msg.get("subject", "")
        sender = msg.get("from", "")
        date = msg.get("date", "")
        body = msg.get_body(preferencelist=("plain", "html"))
        body_text = body.get_content() if body else ""
        text = f"Asunto: {subject}\nDe: {sender}\nFecha: {date}\n\n{body_text}"
        self._register(f"{lineage} (cuerpo)", text.encode("utf-8"), ".txt", "text", text=text)
        for part in msg.iter_attachments():
            fname = part.get_filename() or "attachment"
            content = part.get_content()
            if isinstance(content, str):
                content = content.encode("utf-8")
            self.process_entry(f"{lineage}{LINEAGE_SEP}{fname}", content, Path(fname).suffix.lower())

    def process_entry(self, lineage: str, data: bytes, suffix: str) -> None:
        suffix = (suffix or "").lower()
        if suffix == ".zip":
            self.process_zip(lineage, data)
        elif suffix == ".eml":
            self.process_eml(lineage, data)
        elif suffix in XLSX_EXTS:
            try:
                text = xlsx_to_text(data)
            except Exception as e:  # noqa: BLE001 — record extraction errors as staged text
                text = f"[ERROR extrayendo xlsx: {e}]"
            self._register(lineage, data, suffix, "text", text=text)
        elif suffix in BINARY_EXTS:
            self._register(lineage, data, suffix, "binary")
        else:
            self._skip(lineage, f"unsupported-ext:{suffix or 'none'}")


def _leaf(lineage: str) -> str:
    """Original filename = leaf of the lineage (strip zip/eml container prefixes)."""
    return Path(lineage.split(LINEAGE_SEP)[-1]).name


def normalize_files(sources: list[tuple[str, bytes]]) -> NormalizeResult:
    """
    Normalize a batch of source files.

    Args:
        sources: list of (relative_path_or_name, raw_bytes). Each is processed by
                 extension: .zip unpacked recursively, .eml detached, .xlsx text-extracted,
                 known binaries passed through, unknowns skipped.

    Returns:
        NormalizeResult with staged items (in memory) + a manifest.
    """
    n = _Normalizer()
    count = 0
    for rel, data in sources:
        count += 1
        n.process_entry(rel, data, Path(rel).suffix.lower())
    return NormalizeResult(items=n.items, n_source_files=count)


def normalize_dir(root: str | Path, *, skip_hidden: bool = True) -> NormalizeResult:
    """Convenience: read every file under `root` recursively and normalize it."""
    root = Path(root)
    sources: list[tuple[str, bytes]] = []
    for path in sorted(root.rglob("*")):
        if path.is_dir():
            continue
        if skip_hidden and path.name.startswith("."):
            continue
        sources.append((str(path.relative_to(root)), path.read_bytes()))
    return normalize_files(sources)


def _manifest_key(entry: dict) -> tuple:
    """Stable identity for a manifest entry: staged_name for staged docs (deterministic
    from lineage, so re-processing the same file is idempotent), else (kind, lineage)."""
    sn = entry.get("staged_name")
    return ("staged", sn) if sn else (entry.get("kind"), entry.get("lineage"))


def write_result_to_dir(
    result: NormalizeResult,
    stage_root: str | Path,
    *,
    clean: bool = True,
    write_file: Callable[[Path, bytes], None] | None = None,
    write_text: Callable[[Path, str], None] | None = None,
    read_text: Callable[[Path], str] | None = None,
) -> dict[str, int]:
    """
    Persist a NormalizeResult in the staging/{binary,text}/ + manifest.json layout.

    `write_file`/`write_text`/`read_text` let callers inject Volume-safe IO; default uses
    plain filesystem access (fine for local + Volume FUSE for these small artifacts).

    When `clean=False` the manifest is *merged* with any existing manifest.json rather
    than overwritten, deduped by `_manifest_key`. This is required for the streaming
    ingest: Auto Loader may split one upload across several micro-batches, each calling
    this per expedient — overwriting would drop every batch's docs but the last, leaving
    them staged-but-unclassified (classify_merge joins parsed docs to the manifest).
    """
    stage_root = Path(stage_root)
    binary_dir = stage_root / "binary"
    text_dir = stage_root / "text"

    if write_file is None:
        def write_file(p: Path, b: bytes) -> None:  # noqa: E306
            p.write_bytes(b)
    if write_text is None:
        def write_text(p: Path, s: str) -> None:  # noqa: E306
            p.write_text(s, encoding="utf-8")
    if read_text is None:
        def read_text(p: Path) -> str:  # noqa: E306
            return p.read_text(encoding="utf-8")

    if clean:
        import shutil

        shutil.rmtree(stage_root, ignore_errors=True)
    binary_dir.mkdir(parents=True, exist_ok=True)
    text_dir.mkdir(parents=True, exist_ok=True)

    for it in result.items:
        if it.kind == "binary":
            write_file(binary_dir / it.staged_name, it.data or b"")
        elif it.kind == "text":
            write_text(text_dir / it.staged_name, it.text or "")

    manifest_path = stage_root / "manifest.json"
    manifest = result.manifest
    if not clean:
        existing: list[dict] = []
        try:
            existing = json.loads(read_text(manifest_path))
        except (FileNotFoundError, OSError, ValueError):
            existing = []
        if existing:
            seen = {_manifest_key(e) for e in existing}
            merged = list(existing)
            for e in manifest:
                if _manifest_key(e) not in seen:
                    merged.append(e)
                    seen.add(_manifest_key(e))
            manifest = merged

    write_text(manifest_path, json.dumps(manifest, indent=2, ensure_ascii=False))
    return result.counts()
