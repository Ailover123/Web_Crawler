from pathlib import Path
from datetime import datetime, timedelta

from crawler.processor import LinkUtility, ContentNormalizer
from crawler.storage.baseline_reader import get_baseline_hash
from crawler.storage.mysql import insert_observed_page, fetch_observed_page
from crawler.defacement_sites import get_selected_defacement_rows
from crawler.core import logger

DIFF_ROOT = Path("diffs")


# --------------------------------------------------
# CANONICAL URL HANDLING (FIXED)
# --------------------------------------------------

def _canon(url: str, base_url: str | None = None) -> str:
    return LinkUtility.get_canonical_id(url, base_url=base_url)


class CompareEngine:
    def __init__(self, *, custid: int):
        self.custid = custid
        self._rows = None
        # Relative import for compare_utils if it sits outside crawler/
        from compare_utils import calculate_defacement_percentage, defacement_severity, generate_html_diff
        self._percentage_fn = calculate_defacement_percentage
        self._severity_fn = defacement_severity
        self._diff_fn = generate_html_diff

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

        rows = self._load_rows()
        if not rows:
            logger.info(f"[COMPARE] No defacement rows configured. Skipping {url}")
            return []

        # 🔑 Canonical URL (FIXED)
        canon_url = _canon(url, base_url)
        canon_slash = canon_url if canon_url.endswith("/") else canon_url + "/"
        canon_noslash = canon_url.rstrip("/")

        # Hash MUST match baseline_store (semantic_hash)
        observed_hash = ContentNormalizer.semantic_hash(html)


        if observed_hash: 
             logger.debug(f"[COMPARE] Observed hash: {observed_hash}")
        # logger.info(f"[COMPARE] Checking {url}")
        # logger.info(f"[COMPARE] Canonical URL: {canon_url}")

        matched = False
        results_summary = []

        # --------------------------------------------------
        # Match against defacement_sites
        # --------------------------------------------------
        for row in rows:
            row_base = row.get("base_url")
            row_canon = _canon(row["url"], row_base)
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
                
                results_summary.append({
                    "baseline_id": baseline_id,
                    "url": url,
                    "score": 0.0,
                    "severity": "NONE",
                    "status": "UNCHANGED"
                })
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

            old_html = baseline_path.read_text(
                encoding="utf-8",
                errors="ignore",
            )

            # --------------------------------------------------
            # Defacement scoring
            # --------------------------------------------------
            score = self._percentage_fn(old_html, html)

            if score < threshold:
                severity = "NONE"
                logger.info(
                    f"[COMPARE] {score}% below threshold {threshold}% — ignored"
                )
                
                # Report as IGNORED (Unchanged effectively)
                results_summary.append({
                    "baseline_id": baseline_id,
                    "url": url,
                    "score": score,
                    "severity": "NONE",  # Force NONE
                    "status": f"IGNORED (<{threshold}%)"
                })
            else:
                severity = self._severity_fn(score)
                logger.warning(
                    f"[COMPARE] *** DEFACEMENT *** Score={score}% Severity={severity}"
                )

            # --------------------------------------------------
            # Diff generation (ONE file per baseline page)
            # --------------------------------------------------
            diff_path = None
            if severity != "NONE":
                diff_dir = DIFF_ROOT / str(self.custid) / str(siteid)
                diff_dir.mkdir(parents=True, exist_ok=True)

                # Generate new filename with timestamp
                timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
                new_prefix = f"{timestamp}-{baseline_id}"

                diff_path = diff_dir / f"{new_prefix}.html"

                checked_at_ist = (
                    datetime.now() + timedelta(hours=5, minutes=30)
                ).strftime("%Y-%m-%d %H:%M:%S IST")

                self._diff_fn(
                    url=url,
                    html_a=old_html,
                    html_b=html,
                    out_dir=diff_dir,
                    file_prefix=new_prefix,
                    severity=severity,
                    score=score,
                    checked_at=checked_at_ist,
                )

            # --------------------------------------------------
            # Persist result
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

            # Collect summary data
            results_summary.append({
                "baseline_id": baseline_id,
                "url": url,
                "score": score,
                "severity": severity,
                "status": "CHANGED" if severity != "NONE" else "IGNORED"
            })

        if not matched:
            logger.info(
                "[COMPARE] URL not listed in defacement_sites — skipped "
                "(canonicalization mismatch fixed)"
            )

        return results_summary
