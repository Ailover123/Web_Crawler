from pathlib import Path
from datetime import datetime
from crawler.processor import LinkUtility, ContentNormalizer
from crawler.storage.baseline_reader import get_baseline_hash
from crawler.storage.mysql import insert_observed_page, get_selected_defacement_rows
from crawler.core import logger

DIFF_ROOT = Path("diffs")


class CompareEngine:

    def __init__(self, *, custid: int):
        self.custid = custid
        self._rows = None

        from compare_utils import (
            calculate_defacement_percentage,
            defacement_severity,
            generate_html_diff
        )

        self._percentage_fn = calculate_defacement_percentage
        self._severity_fn = defacement_severity
        self._diff_fn = generate_html_diff

    def _load_rows(self):
        if self._rows is None:
            self._rows = get_selected_defacement_rows() or []
            logger.info(f"[COMPARE] Loaded {len(self._rows)} defacement row(s)")
        return self._rows

    def handle_page(self, *, siteid: int, url: str, html: str, base_url: str | None = None,):

        if not html:
            return []

        rows = self._load_rows()
        if not rows:
            return []

        live_canon = LinkUtility.get_canonical_id(url)
        observed_hash = ContentNormalizer.semantic_hash(html)

        logger.info(f"[COMPARE] LIVE CANON: {live_canon}")

        matched = False
        results = []

        for row in rows:

            if row["siteid"] != siteid:
                continue

            row_canon = row["url"]

            if live_canon != row_canon:
                continue

            matched = True
            baseline_id = row["baseline_id"]

            baseline = get_baseline_hash(
                site_id=siteid,
                normalized_url=row_canon
            )

            if not baseline:
                logger.error("[COMPARE] Baseline missing in DB")
                continue

            baseline_hash = baseline["content_hash"]

            if observed_hash == baseline_hash:
                changed = False
                severity = "NONE"
                score = 0.0
                diff_path = None
                logger.info("[COMPARE] UNCHANGED")
            else:
                changed = True
                logger.warning("[COMPARE] CHANGE DETECTED")

                baseline_path = Path(baseline["baseline_path"])
                old_html = baseline_path.read_text(
                    encoding="utf-8",
                    errors="ignore"
                )

                score = self._percentage_fn(old_html, html)
                severity = self._severity_fn(score)

                diff_dir = DIFF_ROOT / str(self.custid) / str(siteid)
                diff_dir.mkdir(parents=True, exist_ok=True)

                timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
                prefix = f"{timestamp}-{baseline_id}"

                diff_path = diff_dir / f"{prefix}.html"

                self._diff_fn(
                    url=url,
                    html_a=old_html,
                    html_b=html,
                    out_dir=diff_dir,
                    file_prefix=prefix,
                    severity=severity,
                    score=score,
                    checked_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )

            insert_observed_page(
                site_id=siteid,
                baseline_id=baseline_id,
                normalized_url=row_canon,
                observed_hash=observed_hash,
                changed=changed,
                diff_path=str(diff_path) if changed else None,
                defacement_score=score,
                defacement_severity=severity
            )

            results.append({
                "baseline_id": baseline_id,
                "url": url,
                "status": "CHANGED" if changed else "UNCHANGED"
            })

        if not matched:
            logger.info(f"[COMPARE] Not monitored: {live_canon}")

        return results
