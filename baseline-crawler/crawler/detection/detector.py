"""
Defacement detection logic.

Severity rules:
- HIGH   → external script added or removed
- MEDIUM → DOM structure changed or real content change
- LOW    → text reordering / formatting-only change
- NONE   → no change
"""

import os
import re
from collections import Counter
from crawler.storage.baseline_store import get_baseline
from crawler.normalizer import semantic_normalize_html, dom_structure_fingerprint
from crawler.config import DATA_DIR


def _read_baseline_snapshot(hashval):
    if not hashval:
        return None
    path = os.path.join(
        DATA_DIR, "snapshots", "baselines", f"baseline_{hashval}.html"
    )
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except Exception:
        return None


def _token_multiset(text):
    clean = re.sub(r"<[^>]+>", " ", text)
    words = re.findall(r"\w+", clean.lower())
    return Counter(words)


def detect_defacement(url, observed_data):
    baseline = get_baseline(url)
    if baseline is None:
        return None

    baseline_hash = baseline.get("html_hash")
    observed_hash = observed_data.get("html_hash")

    html_changed = baseline_hash != observed_hash

    # ---- SCRIPT SOURCE CHECK (HIGH) ----
    baseline_scripts = set(baseline.get("script_sources") or [])
    observed_scripts = set(observed_data.get("script_sources") or [])

    scripts_added = list(observed_scripts - baseline_scripts)
    scripts_removed = list(baseline_scripts - observed_scripts)

    if scripts_added or scripts_removed:
        return {
            "defaced": True,
            "severity": "HIGH",
            "severity_reason": "External script source added or removed",
            "severity_rule": "script_src_change",
            "html_changed": html_changed,
            "scripts_added": scripts_added,
            "scripts_removed": scripts_removed,
            "baseline_hash": baseline_hash,
            "observed_hash": observed_hash,
        }

    # ---- NO HTML CHANGE ----
    if not html_changed:
        return {
            "defaced": False,
            "severity": "NONE",
            "html_changed": False,
            "scripts_added": [],
            "scripts_removed": [],
            "baseline_hash": baseline_hash,
            "observed_hash": observed_hash,
        }

    # ---- LOAD SNAPSHOTS ----
    baseline_html = _read_baseline_snapshot(baseline_hash)
    observed_html = observed_data.get("normalized_html")

    if not baseline_html or not observed_html:
        return {
            "defaced": True,
            "severity": "MEDIUM",
            "severity_reason": "HTML hash changed (no snapshot comparison)",
            "severity_rule": "hash_only",
            "html_changed": True,
            "scripts_added": [],
            "scripts_removed": [],
            "baseline_hash": baseline_hash,
            "observed_hash": observed_hash,
        }

    # ---- DOM STRUCTURE CHECK (MEDIUM) ----
    base_dom = dom_structure_fingerprint(baseline_html)
    obs_dom = dom_structure_fingerprint(observed_html)

    if base_dom != obs_dom:
        return {
            "defaced": True,
            "severity": "MEDIUM",
            "severity_reason": "DOM structure changed",
            "severity_rule": "dom_change",
            "html_changed": True,
            "scripts_added": [],
            "scripts_removed": [],
            "baseline_hash": baseline_hash,
            "observed_hash": observed_hash,
        }

    # ---- SEMANTIC TEXT CHECK (LOW) ----
    base_sem = semantic_normalize_html(baseline_html)
    obs_sem = semantic_normalize_html(observed_html)

    if _token_multiset(base_sem) == _token_multiset(obs_sem):
        return {
            "defaced": True,
            "severity": "LOW",
            "severity_reason": "Text reordering / formatting-only change",
            "severity_rule": "semantic_reorder",
            "html_changed": True,
            "scripts_added": [],
            "scripts_removed": [],
            "baseline_hash": baseline_hash,
            "observed_hash": observed_hash,
        }

    # ---- FALLBACK (MEDIUM) ----
    return {
        "defaced": True,
        "severity": "MEDIUM",
        "severity_reason": "Content changed",
        "severity_rule": "content_change",
        "html_changed": True,
        "scripts_added": [],
        "scripts_removed": [],
        "baseline_hash": baseline_hash,
        "observed_hash": observed_hash,
    }
