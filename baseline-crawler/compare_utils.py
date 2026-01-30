#!/usr/bin/env python3
import difflib
from pathlib import Path
from html import escape
from bs4 import BeautifulSoup, NavigableString, Tag

DEFAULT_CONTEXT = 5


# ============================================================
# HTML-AWARE NORMALIZATION
# ============================================================

def _html_to_semantic_lines(html: str):
    """
    Convert HTML into semantic, whitespace-stable lines.
    Formatting-only differences are eliminated here.
    """
    soup = BeautifulSoup(html, "lxml")
    lines = []

    def walk(node, depth=0):
        indent = "  " * depth

        if isinstance(node, NavigableString):
            text = " ".join(str(node).split())
            if text:
                lines.append(indent + text)

        elif isinstance(node, Tag):
            # normalize attributes (sorted)
            attrs = " ".join(
                f'{k}="{ " ".join(v) if isinstance(v, list) else v }"'
                for k, v in sorted(node.attrs.items())
            )

            open_tag = f"<{node.name}{(' ' + attrs) if attrs else ''}>"
            lines.append(indent + open_tag)

            for child in node.children:
                walk(child, depth + 1)

            close_tag = f"</{node.name}>"
            lines.append(indent + close_tag)

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


def _html_to_semantic_lines(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    lines = []

    def walk(node, depth=0):
        indent = "  " * depth

        if isinstance(node, NavigableString):
            text = " ".join(str(node).split())
            if text:
                lines.append(indent + text)

        elif isinstance(node, Tag):
            attrs = " ".join(
                f'{k}="{ " ".join(v) if isinstance(v, list) else v }"'
                for k, v in sorted(node.attrs.items())
            )
            lines.append(indent + f"<{node.name}{(' ' + attrs) if attrs else ''}>")

            for child in node.children:
                walk(child, depth + 1)

            lines.append(indent + f"</{node.name}>")

    for c in soup.contents:
        walk(c)

    return lines


def calculate_defacement_percentage(
    baseline_html: str,
    observed_html: str,
) -> float:
    """
    Returns defacement percentage (0.0 – 100.0)
    """
    base_lines = _html_to_semantic_lines(baseline_html)
    obs_lines = _html_to_semantic_lines(observed_html)

    if not base_lines:
        return 100.0 if obs_lines else 0.0

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
    return round(min(100.0, pct), 2)


def defacement_severity(score: float) -> str:
    if score < 5:
        return "LOW"
    if score < 20:
        return "MEDIUM"
    if score < 50:
        return "HIGH"
    return "CRITICAL"
