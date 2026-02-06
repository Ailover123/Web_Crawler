#!/usr/bin/env python3
import difflib
from pathlib import Path
from html import escape
import re
from bs4 import BeautifulSoup, NavigableString, Tag, Comment

DEFAULT_CONTEXT = 5


# ============================================================
# HTML-AWARE NORMALIZATION
# ============================================================

# ============================================================
# IGNORED ATTRIBUTES & TAGS
# ============================================================

IGNORED_ATTR_PATTERNS = {
    # Exact matches
    "nonce", "value", "floatingButtonsClickTracking", 
    "aria-controls", "aria-labelledby", "data-smartmenus-id", 
    "id", "name", "cb", 
    
    # Common dynamic patterns
    "data-csrf", "csrf-token", "authenticity_token", 
    "__VIEWSTATE", "__EVENTVALIDATION", "_token",
}

SUFFIX_PATTERNS = {
    "nonce",  # covers *nonce
}

IGNORED_TAGS = {
    "base", # Frequently changes environment-to-environment or is injected
}

PREFIX_PATTERNS = {
    "data-aos", "data-wow", "data-framer", "data-scroll", "aria-hidden"
}

IGNORED_STYLE_PROPERTIES = {
    "transition", "transform", "animation", "will-change", "opacity",
    "transition-duration", "transition-delay", "transition-timing-function",
    "animation-duration", "animation-delay", "animation-iteration-count"
}

# Tags that trigger a score boost if their content changes
CRITICAL_TAGS = {"title", "h1"}
CRITICAL_BOOST = 1.0  # Percentage points to add if critical content changes

def should_ignore_attr(attr_name: str) -> bool:
    """Check if an attribute should be ignored based on patterns."""
    if attr_name in IGNORED_ATTR_PATTERNS:
        return True
    
    for suffix in SUFFIX_PATTERNS:
        if attr_name.endswith(suffix):
            return True

    for prefix in PREFIX_PATTERNS:
        if attr_name.startswith(prefix):
            return True
            
    return False


def _normalize_style(style_str: str) -> str:
    """
    Parse inline style, remove ignored properties, and return robust sorted string.
    """
    if not style_str:
        return ""
        
    # Split by semicolon to get declarations
    declarations = [d.strip() for d in style_str.split(";") if d.strip()]
    
    valid_decls = {}
    
    for decl in declarations:
        if ":" not in decl:
            continue
        key, val = decl.split(":", 1)
        key = key.strip().lower()
        val = val.strip()
        
        # Filter dynamic animation properties
        is_ignored = False
        for ignored_prop in IGNORED_STYLE_PROPERTIES:
            # Check for exact match or vendor prefix match (e.g. -webkit-transform)
            if key == ignored_prop or key.endswith("-" + ignored_prop):
                is_ignored = True
                break
        
        if not is_ignored:
            # Collapse whitespace in value
            val = " ".join(val.split())
            valid_decls[key] = val
            
    if not valid_decls:
        return ""

    return "; ".join(f"{k}: {v}" for k, v in sorted(valid_decls.items()))


# ============================================================
# HTML-AWARE NORMALIZATION
# ============================================================

def _html_to_semantic_lines(html: str) -> list[str]:
    """
    Convert HTML into semantic, whitespace-stable lines.
    Formatting-only differences are eliminated here.
    Attributes matching IGNORED_ATTR_PATTERNS are skipped.
    Tags matching IGNORED_TAGS are skipped.
    """
    soup = BeautifulSoup(html, "lxml")
    
    # Pre-emptive strip of ignored tags to prevent them from breaking structure/lines
    for tag_name in IGNORED_TAGS:
        for match in soup.find_all(tag_name):
            match.decompose()

    lines = []

    def walk(node, depth=0):

        if isinstance(node, Comment):
            return

        if isinstance(node, NavigableString):
            text = str(node)
            # Aggressive normalization for code blocks to handle minification differences
            if node.parent and node.parent.name in ("script", "style"):
                # Collapse whitespace around operators: "width: 100px" -> "width:100px"
                text = re.sub(r'\s*([{}()\[\]:;,=+\-*/%&|<>!^?~"\'`])\s*', r'\1', text)
            
            # Standard normalization for all text (collapses multiple spaces to one)
            text = " ".join(text.split())

            if text:
                lines.append(text)

        elif isinstance(node, Tag):
            # normalize attributes (sorted and filtered)
            # normalize attributes (sorted and filtered)
            valid_attrs = {}
            CASE_INSENSITIVE_ATTRS = {"charset", "lang", "type", "method", "rel", "media", "http-equiv"}
            
            for k, v in node.attrs.items():
                if should_ignore_attr(k):
                    continue
                
                # Handle list-type attributes (like class) vs string
                if isinstance(v, list):
                    v = " ".join(v)
                
                v = str(v)
                
                # Special handling for style attribute
                if k.lower() == "style":
                    v = _normalize_style(v)
                    if not v:
                        continue
                
                # Case-insensitive normalization for specific attributes
                if k.lower() in CASE_INSENSITIVE_ATTRS:
                    v = v.lower()
                
                # Collapse whitespace
                v = " ".join(v.split())
                
                valid_attrs[k] = v

            attrs = " ".join(
                f'{k}="{v}"'
                for k, v in sorted(valid_attrs.items())
            )

            open_tag = f"<{node.name}{(' ' + attrs) if attrs else ''}>"
            lines.append(open_tag)

            for child in node.children:
                walk(child, depth + 1)

            close_tag = f"</{node.name}>"
            lines.append(close_tag)

    for child in soup.contents:
        walk(child)

    return lines


# ============================================================
# CHANGE RANGE COLLECTION
# ============================================================

def _collect_change_ranges(a_lines, b_lines, context):
    sm = difflib.SequenceMatcher(None, a_lines, b_lines)
    ranges = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            continue

        start = max(0, min(i1, j1) - context)
        end = max(i2, j2) + context
        ranges.append((start, end))

    # merge overlapping ranges
    merged = []
    for r in sorted(ranges):
        if not merged or r[0] > merged[-1][1]:
            merged.append(list(r))
        else:
            merged[-1][1] = max(merged[-1][1], r[1])

    return merged


# ============================================================
# SIDE-BY-SIDE RENDERING
# ============================================================

def _row(cls, a_ln, a_txt, b_ln, b_txt):
    return (
        f"<tr class='{cls}'>"
        f"<td class='ln'>{a_ln}</td>"
        f"<td class='code'>{escape(a_txt)}</td>"
        f"<td class='ln'>{b_ln}</td>"
        f"<td class='code'>{escape(b_txt)}</td>"
        f"</tr>"
    )


def _render_side_by_side_block(a_lines, b_lines, start, end):
    sm = difflib.SequenceMatcher(
        None,
        a_lines[start:end],
        b_lines[start:end],
    )

    rows = []
    a_ln = start + 1
    b_ln = start + 1

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            for k in range(i2 - i1):
                rows.append(
                    _row(
                        "ctx",
                        a_ln,
                        a_lines[start + i1 + k],
                        b_ln,
                        b_lines[start + j1 + k],
                    )
                )
                a_ln += 1
                b_ln += 1

        elif tag == "delete":
            for line in a_lines[start + i1:start + i2]:
                rows.append(_row("del", a_ln, line, "", ""))
                a_ln += 1

        elif tag == "insert":
            for line in b_lines[start + j1:start + j2]:
                rows.append(_row("add", "", "", b_ln, line))
                b_ln += 1

        elif tag == "replace":
            max_len = max(i2 - i1, j2 - j1)
            for k in range(max_len):
                left = a_lines[start + i1 + k] if i1 + k < i2 else ""
                right = b_lines[start + j1 + k] if j1 + k < j2 else ""

                cls = "ctx" if left == right else "mod"

                rows.append(
                    _row(
                        cls,
                        a_ln if left else "",
                        left,
                        b_ln if right else "",
                        right,
                    )
                )

                if left:
                    a_ln += 1
                if right:
                    b_ln += 1

    return rows


# ============================================================
# PUBLIC ENTRY POINT
# ============================================================

def generate_html_diff(
    *,
    url: str,
    html_a: str,
    html_b: str,
    out_dir: Path,
    file_prefix: str,
    context_lines: int = DEFAULT_CONTEXT,
):
    """
    Generates ONE HTML-aware, side-by-side diff file.
    Overwrites the same file on re-run.
    """

    out_dir.mkdir(parents=True, exist_ok=True)

    # HTML-aware normalization
    a_lines = _html_to_semantic_lines(html_a)
    b_lines = _html_to_semantic_lines(html_b)

    ranges = _collect_change_ranges(a_lines, b_lines, context_lines)

    all_rows = []

    for start, end in ranges:
        rows = _render_side_by_side_block(a_lines, b_lines, start, end)
        if rows:
            all_rows.extend(rows)
            all_rows.append("<tr class='sep'><td colspan='4'></td></tr>")

    if not all_rows:
        body = "<p>No changes detected.</p>"
    else:
        body = (
            "<table class='diff'>"
            "<tr class='col-header'>"
            "<th colspan='2'>Baseline</th>"
            "<th colspan='2'>Observed</th>"
            "</tr>"
            + "".join(all_rows)
            + "</table>"
        )

    html_page = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Diff for {escape(url)}</title>
  <style>
    body {{
      font-family: Consolas, Monaco, monospace;
      background: #0f1117;
      color: #e6e6e6;
      margin: 20px;
    }}

    h2 {{
      margin-bottom: 16px;
      font-size: 18px;
      color: #ffffff;
    }}

    table.diff {{
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 13px;
    }}

    th {{
      position: sticky;
      top: 0;
      background: #1a1d24;
      color: #fff;
      text-align: center;
      padding: 6px;
      border-bottom: 2px solid #333;
      z-index: 2;
    }}

    td {{
      padding: 3px 6px;
      vertical-align: top;
      border-bottom: 1px solid #1f2430;
    }}

    .ln {{
      width: 52px;
      color: #9aa4b2;
      background: #141821;
      text-align: right;
    }}

    .code {{
      white-space: pre-wrap;
      word-break: break-word;
      padding-left: 10px;
    }}

    tr.ctx {{ background: #0f1117; }}
    tr.add {{ background: #123d23; }}
    tr.del {{ background: #3a1d1d; }}
    tr.mod {{ background: #3a331d; }}

    tr.sep td {{
      height: 14px;
      background: #000;
      border-bottom: none;
    }}
  </style>
</head>
<body>
  <h2>Changes for {escape(url)}</h2>
  {body}
</body>
</html>
"""

    out_path = out_dir / f"{file_prefix}.html"
    out_path.write_text(html_page, encoding="utf-8")

    return str(out_path)
# ============================================================
# DEFACE­MENT SCORING
# ============================================================

from bs4 import BeautifulSoup, NavigableString, Tag
import difflib






def _extract_critical_content(html: str) -> dict[str, str]:
    """
    Extract text content from critical tags.
    Returns: {'title': 'Page Title', 'h1': 'Header Text'}
    """
    soup = BeautifulSoup(html or "", "lxml")
    content = {}
    
    for tag_name in CRITICAL_TAGS:
        # Find first occurrence (usually the most important one)
        tag = soup.find(tag_name)
        if tag:
            # Normalize whitespace
            text = " ".join(tag.get_text().split())
            content[tag_name] = text
            
    return content


def calculate_defacement_percentage(
    baseline_html: str,
    observed_html: str,
    threshold: float = 1.0,
) -> float:
    """
    Returns defacement percentage (0.0 – 100.0)
    Includes a boost for critical tag changes (title, h1) IF score < threshold.
    """
    base_lines = _html_to_semantic_lines(baseline_html)
    obs_lines = _html_to_semantic_lines(observed_html)

    if not base_lines:
        return 100.0 if obs_lines else 0.0

    # 1. Standard Diff Score
    sm = difflib.SequenceMatcher(None, base_lines, obs_lines)

    changed = 0
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "equal":
            continue
        if tag in ("replace", "delete"):
            changed += (i2 - i1)
        if tag == "insert":
            changed += (j2 - j1)

    pct = (changed / len(base_lines)) * 100
    
    # Optimization: If we already exceeded the threshold, don't pretend to inspect critical tags
    # The user is already alerted.
    if pct >= threshold:
        return round(min(100.0, pct), 2)

    # 2. Critical Content Boost
    base_critical = _extract_critical_content(baseline_html)
    obs_critical = _extract_critical_content(observed_html)
    
    boost = 0.0
    for tag in CRITICAL_TAGS:
        b_text = base_critical.get(tag, "")
        o_text = obs_critical.get(tag, "")
        
        # If content exists in both but differs -> BOOST
        if b_text and o_text and b_text != o_text:
            boost += CRITICAL_BOOST
            
    final_score = min(100.0, pct + boost)
    return round(final_score, 2)


def semantic_hash(html: str) -> str:
    """
    Generate a SHA256 hash of the semantic content of the HTML.
    Stable against whitespace and attribute ordering changes.
    """
    import hashlib
    lines = _html_to_semantic_lines(html)
    content = "\n".join(lines)
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def defacement_severity(score: float) -> str:
    if score < 5:
        return "LOW"
    if score < 20:
        return "MEDIUM"
    if score < 50:
        return "HIGH"
    return "CRITICAL"
