import hashlib


def html_hash(html: str) -> str:
    return hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()


def is_significant_change(baseline_html: str, new_html: str) -> bool:
    """
    Initial simple rule:
    - hash mismatch = significant
    Later you can add:
    - DOM-aware diffs
    - noise filtering
    """
    return html_hash(baseline_html) != html_hash(new_html)
