from pathlib import Path
from datetime import datetime
from crawler.processor import LinkUtility, ContentNormalizer
from crawler.storage.baseline_reader import get_baseline_hash
from crawler.storage.mysql import insert_observed_page, get_selected_defacement_rows
from crawler.core import logger

DIFF_ROOT = Path("diffs")


class CompareEngine:

    DEFAULT_THRESHOLD = 1.0

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

    def handle_page(
        self,
        *,
        siteid: int,
        url: str,
        html: str,
        base_url: str | None = None
    ):
        if not html:
            return []

        rows = self._load_rows()
        if not rows:
            return []

        # ✅ Canonical URL match
        live_canon = LinkUtility.get_canonical_id(url, base_url)
        logger.info(f"[COMPARE] LIVE CANON: {live_canon}")

        # ✅ Normalize LIVE HTML
        normalized_live = ContentNormalizer.normalize_html(html)
        observed_hash = ContentNormalizer.semantic_hash(normalized_live)

        matched = False
        results = []

        for row in rows:

            if int(row["siteid"]) != int(siteid):
                continue

            row_canon = row["url"]

            if live_canon != row_canon:
                continue

            matched = True
            baseline_id = row["baseline_id"]
            
            # Get threshold from DB or use default
            threshold_val = row.get("threshold")
            threshold = float(threshold_val) if threshold_val is not None else self.DEFAULT_THRESHOLD

            baseline = get_baseline_hash(
                site_id=siteid,
                normalized_url=row_canon
            )

            if not baseline:
                logger.error("[COMPARE] Baseline missing in DB")
                continue

            baseline_path = Path(baseline["baseline_path"])

            if not baseline_path.exists():
                logger.error("[COMPARE] Baseline file missing on disk")
                continue

            # ✅ Normalize BASELINE HTML
            old_raw_html = baseline_path.read_text(
                encoding="utf-8",
                errors="ignore"
            )
            normalized_baseline = ContentNormalizer.normalize_html(old_raw_html)

            baseline_hash = ContentNormalizer.semantic_hash(normalized_baseline)

            # =====================================
            # HASH COMPARISON (CLEAN + STABLE)
            # =====================================

            if observed_hash == baseline_hash:
                logger.info(f"[COMPARE] UNCHANGED (Hash Match) - {url}")
                continue

            # =====================================
            # CALCULATE SCORE
            # =====================================
            
            score = self._percentage_fn(
                normalized_baseline,
                normalized_live,
                threshold=threshold
            )

            if score < threshold:
                logger.info(f"[COMPARE] UNCHANGED (Score {score} < {threshold}) - {url}")
                continue

            # =====================================
            # CHANGE DETECTED >= THRESHOLD
            # =====================================
            
            logger.warning(f"[COMPARE] DEFACEMENT DETECTED: {score}% >= {threshold}%")

            severity = self._severity_fn(score)

            diff_dir = DIFF_ROOT / str(self.custid) / str(siteid)
            diff_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%H%M%S%d%m%Y")
            prefix = f"{timestamp}-{baseline_id}"

            diff_path = diff_dir / f"{prefix}.html"

            self._diff_fn(
                url=url,
                html_a=normalized_baseline,
                html_b=normalized_live,
                out_dir=diff_dir,
                file_prefix=prefix,
                severity=severity,
                score=score,
                checked_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

            # =====================================
            # UPSERT OBSERVED STATE
            # =====================================

            insert_observed_page(
                site_id=siteid,
                baseline_id=baseline_id,
                normalized_url=row_canon,
                observed_hash=observed_hash,
                changed=True,
                diff_path=str(diff_path),
                defacement_score=score,
                defacement_severity=severity
            )

            results.append({
                "baseline_id": baseline_id,
                "url": url,
                "status": "CHANGED",
                "score": score,
                "severity": severity
            })

        if not matched:
            logger.info(f"[COMPARE] Not monitored: {live_canon}")

        return results
