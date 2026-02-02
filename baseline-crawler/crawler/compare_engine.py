# crawler/compare_engine.py

from pathlib import Path

from crawler.normalizer import normalize_url, normalize_html
from crawler.hasher import sha256
from crawler.storage.baseline_reader import get_baseline_hash
from crawler.storage.mysql import insert_observed_page
from crawler.defacement_sites import get_selected_defacement_rows

from compare_utils import (
    generate_html_diff,
    calculate_defacement_percentage,
    defacement_severity,
)

from crawler.logger import logger

BASELINE_ROOT = Path("baselines")
DIFF_ROOT = Path("diffs")


def _canon(url: str) -> str:
    return normalize_url(url)


class CompareEngine:
    def __init__(self, *, custid: int):
        self.custid = custid
        self._rows = None

    def _load_rows(self):
        if self._rows is None:
            self._rows = get_selected_defacement_rows() or []
            logger.info(f"[COMPARE] Loaded {len(self._rows)} defacement row(s)")
        return self._rows

    def handle_page(self, *, siteid: int, url: str, html: str, base_url: str | None = None):
        rows = self._load_rows()
        if not rows:
            logger.info(f"[COMPARE] No defacement rows to compare. Skipping {url}")
            return

        canon_url = _canon(url)
        canon_url_slash = canon_url if canon_url.endswith("/") else canon_url + "/"
        canon_url_noslash = canon_url.rstrip("/")
        
        observed_hash = sha256(normalize_html(html))

        logger.info(f"[COMPARE] Checking {url}")
        logger.info(f"[COMPARE]   Canonical: {canon_url}")
        logger.info(f"[COMPARE]   Also checking: {canon_url_slash} and {canon_url_noslash}")
        logger.info(f"[COMPARE]   Observed hash: {observed_hash}")

        matched = False
        for row in rows:
            row_canon = _canon(row["url"])
            row_slash = row_canon if row_canon.endswith("/") else row_canon + "/"
            row_noslash = row_canon.rstrip("/")
            
            # Check all variations: exact match or with/without trailing slash
            if canon_url != row_canon and canon_url_slash != row_slash and canon_url_noslash != row_noslash:
                continue

            matched = True
            baseline_id = row["baseline_id"]
            logger.info(f"[COMPARE]   [MATCH] URL matched! baseline_id={baseline_id}")

            # Try both versions of the URL for baseline lookup
            baseline = (
                get_baseline_hash(site_id=siteid, normalized_url=canon_url, base_url=base_url)
                or get_baseline_hash(site_id=siteid, normalized_url=canon_url_slash, base_url=base_url)
                or get_baseline_hash(site_id=siteid, normalized_url=canon_url_noslash, base_url=base_url)
            )

            if not baseline:
                logger.info(f"[COMPARE]   [ERROR] No baseline hash found for {canon_url} or variants")
                continue

            logger.info(f"[COMPARE]   [OK] Baseline hash: {baseline['content_hash']}")

            # ================= UNCHANGED =================
            if observed_hash == baseline["content_hash"]:
                logger.info(f"[COMPARE]   [OK] UNCHANGED (hashes match)")
                try:
                    insert_observed_page(
                        site_id=siteid,
                        baseline_id=baseline_id,
                        normalized_url=canon_url,
                        observed_hash=observed_hash,
                        changed=False,
                        diff_path=None,
                        defacement_score=0.0,
                        defacement_severity="NONE",
                        base_url=base_url,
                    )
                    logger.info(f"[COMPARE]   DB: Inserted observed_page (UNCHANGED) for {url}")
                except Exception as e:
                    logger.info(f"[COMPARE]   [ERROR] Failed to insert unchanged: {e}")
                continue

            # ================= CHANGED =================
            logger.info(f"[COMPARE]   [WARNING] CHANGE DETECTED (hashes differ)")
            baseline_file = (
                BASELINE_ROOT
                / str(self.custid)
                / str(siteid)
                / f"{baseline_id}.html"
            )

            logger.info(f"[COMPARE]   Looking for baseline file: {baseline_file}")
            if not baseline_file.exists():
                logger.info(f"[COMPARE]   [ERROR] Baseline file not found: {baseline_file}")
                continue

            logger.info(f"[COMPARE]   [OK] Baseline file found")
            old_html = baseline_file.read_text(
                encoding="utf-8",
                errors="ignore",
            )

            # 🔑 Calculate defacement percentage
            score = calculate_defacement_percentage(old_html, html)
            severity = defacement_severity(score)

            logger.info(f"[COMPARE]   Defacement: {score}% | Severity: {severity}")

            # 🔒 ONE diff file per baseline page
            diff_dir = DIFF_ROOT / str(self.custid) / str(siteid)
            diff_dir.mkdir(parents=True, exist_ok=True)

            file_prefix = str(baseline_id)

            generate_html_diff(
                url=url,
                html_a=old_html,
                html_b=html,
                out_dir=diff_dir,
                file_prefix=file_prefix,
            )

            try:
                insert_observed_page(
                    site_id=siteid,
                    baseline_id=baseline_id,
                    normalized_url=canon_url,
                    observed_hash=observed_hash,
                    changed=True,
                    diff_path=str(diff_dir / f"{file_prefix}.html"),
                    defacement_score=score,
                    defacement_severity=severity,
                    base_url=base_url,
                )
                logger.info(f"[COMPARE]   DB: Inserted observed_page (CHANGED) for {url}")
            except Exception as e:
                logger.info(f"[COMPARE]   [ERROR] Failed to insert change: {e}")

            logger.info(
                f"[COMPARE] *** DEFACEMENT: {url} | "
                f"Defacement={score}% | Severity={severity}"
            )

        if not matched:
            logger.info(f"[COMPARE]   [SKIP] No matching defacement row found for {url}")
