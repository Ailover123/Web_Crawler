# Responsibilities:
# - fetch trusted baseline for a URL
# - compare observed response against baseline
# - determine whether defacement occurred
# - produce a structured detection result

from crawler.storage.baseline_store import get_baseline
def detect_defacement(url, observed_data):
    # Detect defacement by comparing observed data with trusted baseline.
    # Returns:
    #     None if no baseline exists (cannot detect)
    #     dict with structured detection result otherwise

    # 1. Fetch baseline 
    baseline = get_baseline(url)

    if baseline is None:
        return None

    # 2. Compare HTML hashes
    html_changed = (
        observed_data["html_hash"] != baseline["html_hash"]
    )

    # 3. Compare script sources
    baseline_scripts = set(baseline["script_sources"])
    observed_scripts = set(observed_data["script_sources"])

    scripts_added = list(observed_scripts - baseline_scripts)
    scripts_removed = list(baseline_scripts - observed_scripts)

    # 4. Determine if defacement occurred
    defaced = html_changed or scripts_added or scripts_removed

    # 5. Severity classification (v1 logic)
    if scripts_added or scripts_removed:
        severity = "high"
    elif html_changed:
        severity = "medium"
    else:
        severity = "none"

    # 6. Structured result (explicit, no ambiguity)
    return {
        "defaced": defaced,
        "severity": severity,
        "html_changed": html_changed,
        "scripts_added": scripts_added,
        "scripts_removed": scripts_removed,
        "baseline_hash": baseline["html_hash"],
        "observed_hash": observed_data["html_hash"],
    }
