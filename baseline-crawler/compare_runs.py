#!/usr/bin/env python3
from pathlib import Path
from compare_utils import load_baseline_snapshot, generate_html_diff


def _safe_dir_name(url: str) -> str:
    """
    Converts a URL into a filesystem-safe directory name.
    """
    return (
        url.replace("://", "_")
           .replace("/", "_")
           .replace("?", "_")
           .replace("&", "_")
    )


def compare_runs(
    baseline_dir: Path,
    observed_dir: Path,
    output_dir: Path | None = None,
):
    """
    Compares:
      baseline snapshot directory
      vs
      observed snapshot directory

    Optionally writes diff artifacts into output_dir.
    """
    snap_a = load_baseline_snapshot(baseline_dir)
    snap_b = load_baseline_snapshot(observed_dir)

    urls_a = set(snap_a.keys())
    urls_b = set(snap_b.keys())

    added = urls_b - urls_a
    deleted = urls_a - urls_b
    common = urls_a & urls_b

    changed = set()
    unchanged = set()

    for url in common:
        if snap_a[url]["hash"] != snap_b[url]["hash"]:
            changed.add(url)

            if output_dir:
                diff_dir = output_dir / _safe_dir_name(url)
                generate_html_diff(
                    url=url,
                    html_a=snap_a[url]["html"],
                    html_b=snap_b[url]["html"],
                    out_dir=diff_dir,
                )
        else:
            unchanged.add(url)

    return {
        "added": sorted(added),
        "deleted": sorted(deleted),
        "changed": sorted(changed),
        "unchanged": sorted(unchanged),
    }
