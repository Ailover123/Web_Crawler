#!/usr/bin/env python3
import json
import hashlib
import difflib
from pathlib import Path
from datetime import datetime


def load_baseline_snapshot(run_dir: Path) -> dict:
    """
    Reads snapshot files from:
      data/snapshots/baselines/<id>/
      OR
      data/snapshots/observed/<id>/

    Each file is expected to be JSON:
    {
      "url" or "normalized_url": "...",
      "html": "<html>...</html>"
    }
    """
    snapshot = {}

    for html_file in run_dir.rglob("*.html"):
        try:
            data = json.loads(html_file.read_text(encoding="utf-8"))

            url = data.get("normalized_url") or data.get("url")
            if not url:
                continue

            html = data.get("html", "")

            html_hash = hashlib.sha256(
                html.encode("utf-8", errors="ignore")
            ).hexdigest()

            snapshot[url] = {
                "hash": html_hash,
                "html": html,
                "path": str(html_file),
            }
        except Exception:
            # Skip malformed snapshot files
            continue

    return snapshot


def generate_html_diff(
    url: str,
    html_a: str,
    html_b: str,
    out_dir: Path,
    file_prefix: str = "diff"
):
    """
    Generates forensic diff artifacts:
      - diff.patch  (authoritative)
      - diff.html   (human-readable)
      - diff.meta.json
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Unified diff ---
    patch_lines = difflib.unified_diff(
        html_a.splitlines(keepends=True),
        html_b.splitlines(keepends=True),
        fromfile="baseline.html",
        tofile="current.html",
    )

    patch_path = out_dir / f"{file_prefix}.patch"
    patch_path.write_text("".join(patch_lines), encoding="utf-8")

    # --- Highlighted HTML diff ---
    html_diff = difflib.HtmlDiff(tabsize=2, wrapcolumn=80)
    diff_html = html_diff.make_file(
        html_a.splitlines(),
        html_b.splitlines(),
        fromdesc="Baseline",
        todesc="Observed",
    )

    html_path = out_dir / f"{file_prefix}.html"
    html_path.write_text(diff_html, encoding="utf-8")

    # --- Metadata ---
    meta = {
        "url": url,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "diff_type": "html_line_diff",
        "artifacts": {
            "patch": patch_path.name,
            "html": html_path.name,
        },
    }

    meta_path = out_dir / f"{file_prefix}.meta.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    return {
        "patch": str(patch_path),
        "html": str(html_path),
        "meta": str(meta_path),
    }
