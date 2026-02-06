from pathlib import Path

from crawler.content_fingerprint import semantic_hash
from crawler.normalizer import get_canonical_id
from crawler.storage.baseline_reader import get_baseline_hash
from crawler.storage.mysql import insert_observed_page, fetch_observed_page
from crawler.defacement_sites import get_selected_defacement_rows
from crawler.logger import logger
from datetime import datetime, timedelta

from compare_utils import (
    generate_html_diff,
    calculate_defacement_percentage,
    defacement_severity,
)

DIFF_ROOT = Path("diffs")


def _canon(url: str) -> str:
    """
    Canonical URL used for DB + compare matching.
    """
    return get_canonical_id(url)


class CompareEngine:
    def __init__(self, *, custid: int):
        self.custid = custid
        self._rows = None

    # --------------------------------------------------
    # Load defacement targets (cached)
    # --------------------------------------------------
    def _load_rows(self):
        if self._rows is None:
            self._rows = get_selected_defacement_rows() or []
            logger.info(f"[COMPARE] Loaded {len(self._rows)} defacement row(s)")
        return self._rows

    # --------------------------------------------------
    # Main entry point (called from Worker)
    # --------------------------------------------------
    def handle_page(
        self,
        *,
        siteid: int,
        url: str,
        html: str,
        base_url: str | None = None,
    ):
        if not html:
            return

        # 🔑 Raw HTML is processed by semantic_hash and calculate_defacement_percentage
        # No pre-normalization needed (and it can cause hash mismatches vs DB)

        rows = self._load_rows()
        if not rows:
            logger.info(f"[COMPARE] No defacement rows configured. Skipping {url}")
            return

        canon_url = _canon(url)
        canon_slash = canon_url if canon_url.endswith("/") else canon_url + "/"
        canon_noslash = canon_url.rstrip("/")

        # Use semantic_hash to match what baseline_store uses (DB consistency)
        observed_hash = semantic_hash(html)

        # --------------------------------------------------
        # Optimization: skip if same content already seen
        # --------------------------------------------------
        # try:
        #     prev = fetch_observed_page(siteid, canon_url)
        #     if prev and prev["observed_hash"] == observed_hash:
        #         logger.info(
        #             f"[COMPARE] [SKIP] No content change since last check "
        #             f"(hash={observed_hash[:8]}...)"
        #         )
        #         return
        # except Exception as e:
        #     logger.warning(f"[COMPARE] Previous state check failed: {e}")

        logger.info(f"[COMPARE] Checking {url}")
        logger.info(f"[COMPARE] Canonical URL: {canon_url}")
        logger.info(f"[COMPARE] Observed hash: {observed_hash}")

        matched = False

        # --------------------------------------------------
        # Match against defacement_sites
        # --------------------------------------------------
        for row in rows:
            row_canon = _canon(row["url"])
            row_slash = row_canon if row_canon.endswith("/") else row_canon + "/"
            row_noslash = row_canon.rstrip("/")

            if (
                canon_url != row_canon
                and canon_slash != row_slash
                and canon_noslash != row_noslash
            ):
                continue

            matched = True
            baseline_id = row["baseline_id"]
            threshold = row.get("threshold") or 0

            logger.info(
                f"[COMPARE] [MATCH] URL matched baseline_id={baseline_id}"
            )

            # --------------------------------------------------
            # Load baseline hash (DB)
            # --------------------------------------------------
            baseline = (
                get_baseline_hash(site_id=siteid, normalized_url=canon_url, base_url=base_url)
                or get_baseline_hash(site_id=siteid, normalized_url=canon_slash, base_url=base_url)
                or get_baseline_hash(site_id=siteid, normalized_url=canon_noslash, base_url=base_url)
            )

            if not baseline:
                logger.warning(
                    f"[COMPARE] No baseline hash found for {canon_url}"
                )
                continue

            baseline_hash = baseline["content_hash"]

            # --------------------------------------------------
            # UNCHANGED
            # --------------------------------------------------
            if observed_hash == baseline_hash:
                logger.info("[COMPARE] UNCHANGED (hash match)")
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
                except Exception as e:
                    logger.warning(f"[COMPARE] DB insert failed (unchanged): {e}")
                continue

            # --------------------------------------------------
            # CHANGED
            # --------------------------------------------------
            logger.warning("[COMPARE] CHANGE DETECTED")

            baseline_path = Path(baseline["baseline_path"])
            if not baseline_path.exists():
                logger.error(
                    f"[COMPARE] Baseline file missing: {baseline_path}"
                )
                continue

            # 🔑 Baseline HTML is from disk (raw)
            old_html = baseline_path.read_text(
                encoding="utf-8",
                errors="ignore",
            )
            # NOTE: We compare RAW vs RAW. semantic_hash logic inside calculate_defacement_percentage handles normalization.

            # --------------------------------------------------
            # Defacement scoring (normalized vs normalized)
            # --------------------------------------------------
            score = calculate_defacement_percentage(old_html, html, threshold)

            if score < threshold:
                severity = "NONE"
                logger.info(f"[COMPARE] {score}% (below threshold {threshold}%) — Ignored")
            else:
                severity = defacement_severity(score)
                logger.warning(f"[COMPARE] *** DEFACEMENT *** Score={score}% Severity={severity}")

            # --------------------------------------------------
            # Diff generation (ONE file per baseline page)
            # --------------------------------------------------
            diff_path = None
            if severity != "NONE":
                diff_dir = DIFF_ROOT / str(self.custid) / str(siteid)
                diff_dir.mkdir(parents=True, exist_ok=True)

                diff_path = diff_dir / f"{baseline_id}.html"

                checked_at_ist = (datetime.now() + timedelta(hours=5, minutes=30)).strftime("%Y-%m-%d %H:%M:%S IST")

                generate_html_diff(
                    url=url,
                    html_a=old_html,
                    html_b=html,
                    out_dir=diff_dir,
                    file_prefix=str(baseline_id),
                    severity=severity,
                    score=score,
                    checked_at=checked_at_ist,
                )

            # --------------------------------------------------
            # Persist result (only if not suppressed)
            # --------------------------------------------------
            if severity != "NONE":
                try:
                    insert_observed_page(
                        site_id=siteid,
                        baseline_id=baseline_id,
                        normalized_url=canon_url,
                        observed_hash=observed_hash,
                        changed=True,
                        diff_path=str(diff_path),
                        defacement_score=score,
                        defacement_severity=severity,
                        base_url=base_url,
                    )
                    logger.info("[COMPARE] DB insert successful (changed)")
                except Exception as e:
                    logger.error(f"[COMPARE] DB insert failed (changed): {e}")

        if not matched:
            logger.info(
                f"[COMPARE] URL not listed in defacement_sites — skipped"
            )
