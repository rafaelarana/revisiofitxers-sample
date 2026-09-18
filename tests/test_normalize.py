"""
Unit tests for src/shared/normalize.py.

Self-contained: all fixtures (zip, nested zip, eml w/ attachments, xlsx) are built
in memory, so no sample files or Databricks connection is needed. Run:

    cd ~/Projects/filemanager && python3 -m pytest tests/ -q
"""

from __future__ import annotations

import io
import sys
import zipfile
from email.message import EmailMessage
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from shared.normalize import (  # noqa: E402
    NormalizeResult,
    content_hash,
    normalize_files,
    write_result_to_dir,
)


# --------------------------------------------------------------------------- helpers


def make_pdf(marker: bytes = b"hello") -> bytes:
    """Minimal bytes that look like a PDF (content isn't parsed here, only staged)."""
    return b"%PDF-1.4\n" + marker + b"\n%%EOF"


def make_xlsx(rows: list[list]) -> bytes:
    openpyxl = pytest.importorskip("openpyxl")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Hoja1"
    for r in rows:
        ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def make_zip(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


def make_eml(subject: str, body: str, attachments: dict[str, bytes] | None = None) -> bytes:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = "sender@example.com"
    msg["To"] = "dest@example.com"
    msg.set_content(body)
    for fname, data in (attachments or {}).items():
        msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=fname)
    return msg.as_bytes()


def kinds_by_lineage(result: NormalizeResult) -> dict[str, str]:
    return {it.lineage: it.kind for it in result.items}


# --------------------------------------------------------------------------- tests


def test_individual_binary_and_unknown():
    res = normalize_files([("a.pdf", make_pdf()), ("notes.txt", b"plain text"), ("img.png", b"\x89PNG..")])
    counts = res.counts()
    assert counts.get("binary") == 2  # pdf + png
    assert counts.get("skipped") == 1  # .txt is not in the supported set
    skipped = [it for it in res.items if it.kind == "skipped"][0]
    assert "unsupported-ext" in skipped.reason


def test_xlsx_extracted_to_text():
    xlsx = make_xlsx([["Expedient", "Import"], ["AG-2024-4", "1000"]])
    res = normalize_files([("full.xlsx", xlsx)])
    item = res.items[0]
    assert item.kind == "text"
    assert "AG-2024-4" in item.text
    assert "### Hoja: Hoja1" in item.text


def test_zip_is_unpacked_not_treated_as_blob():
    inner = {"folder/annex.pdf": make_pdf(b"annex"), "sheet.xlsx": make_xlsx([["a", "b"]])}
    res = normalize_files([("docs.zip", make_zip(inner))])
    lineages = kinds_by_lineage(res)
    # the zip itself must NOT appear as a staged artifact; its contents must.
    assert not any(l.endswith("docs.zip") and k in ("binary", "text") for l, k in lineages.items())
    assert lineages["docs.zip > folder/annex.pdf"] == "binary"
    assert lineages["docs.zip > sheet.xlsx"] == "text"


def test_nested_zip_recurses():
    innermost = make_pdf(b"deep")
    inner_zip = make_zip({"deep.pdf": innermost})
    outer_zip = make_zip({"inner.zip": inner_zip, "top.pdf": make_pdf(b"top")})
    res = normalize_files([("outer.zip", outer_zip)])
    lineages = kinds_by_lineage(res)
    assert lineages["outer.zip > top.pdf"] == "binary"
    # the fully-recursed leaf lineage
    assert lineages["outer.zip > inner.zip > deep.pdf"] == "binary"


def test_eml_body_and_attachments_detached():
    attach = {"contract.pdf": make_pdf(b"contract")}
    eml = make_eml("Prova", "Cos del correu", attachments=attach)
    res = normalize_files([("mail.eml", eml)])
    lineages = kinds_by_lineage(res)
    # body becomes a text artifact
    assert lineages["mail.eml (cuerpo)"] == "text"
    body_item = [it for it in res.items if it.lineage == "mail.eml (cuerpo)"][0]
    assert "Cos del correu" in body_item.text
    assert "Asunto: Prova" in body_item.text
    # attachment is detached and staged as binary
    assert lineages["mail.eml > contract.pdf"] == "binary"


def test_content_hash_dedup_within_run():
    same = make_pdf(b"identical")
    res = normalize_files([("first.pdf", same), ("second.pdf", same)])
    kinds = [it.kind for it in res.items]
    assert kinds.count("binary") == 1
    assert kinds.count("duplicate") == 1
    dup = [it for it in res.items if it.kind == "duplicate"][0]
    # duplicate points at the first occurrence's staged_name
    first = [it for it in res.items if it.kind == "binary"][0]
    assert dup.duplicate_of == first.staged_name


def test_bad_zip_is_skipped_not_crash():
    res = normalize_files([("broken.zip", b"this is not a real zip")])
    assert res.items[0].kind == "skipped"
    assert res.items[0].reason == "bad-zip"


def test_manifest_shape_matches_mvp():
    res = normalize_files([("a.pdf", make_pdf()), ("dup.pdf", make_pdf())])
    manifest = res.manifest
    binary_entry = [m for m in manifest if m["kind"] == "binary"][0]
    assert set(binary_entry.keys()) == {"lineage", "kind", "staged_name", "original_filename"}
    dup_entry = [m for m in manifest if m["kind"] == "duplicate"][0]
    assert set(dup_entry.keys()) == {"lineage", "kind", "duplicate_of"}


def test_write_result_to_dir_roundtrip(tmp_path):
    inner = {"annex.pdf": make_pdf(b"annex"), "sheet.xlsx": make_xlsx([["x", "y"]])}
    res = normalize_files([("docs.zip", make_zip(inner)), ("solo.pdf", make_pdf(b"solo"))])
    counts = write_result_to_dir(res, tmp_path / "staging")
    assert (tmp_path / "staging" / "manifest.json").exists()
    assert len(list((tmp_path / "staging" / "binary").glob("*.pdf"))) == 2  # annex + solo
    assert len(list((tmp_path / "staging" / "text").glob("*.xlsx"))) == 1  # inner sheet
    assert counts["binary"] == 2


def test_content_hash_is_stable_and_short():
    h = content_hash(b"abc")
    assert h == content_hash(b"abc")
    assert len(h) == 12


def test_write_result_to_dir_merges_manifest_across_batches(tmp_path):
    # Simulates Auto Loader splitting one upload into two micro-batches, each writing to
    # the same staging root with clean=False. The merged manifest must list BOTH batches'
    # docs (regression: overwriting dropped all but the last batch → staged-but-unclassified).
    stage = tmp_path / "staging"
    b1 = normalize_files([("one.pdf", make_pdf(b"one"))])
    write_result_to_dir(b1, stage, clean=False)
    b2 = normalize_files([("two.pdf", make_pdf(b"two"))])
    write_result_to_dir(b2, stage, clean=False)

    import json
    manifest = json.loads((stage / "manifest.json").read_text())
    staged = {m["original_filename"] for m in manifest if m.get("staged_name")}
    assert staged == {"one.pdf", "two.pdf"}
    assert len(list((stage / "binary").glob("*.pdf"))) == 2

    # Re-writing the same batch is idempotent (dedup by staged_name → no growth).
    write_result_to_dir(b2, stage, clean=False)
    manifest = json.loads((stage / "manifest.json").read_text())
    assert len([m for m in manifest if m.get("staged_name")]) == 2
