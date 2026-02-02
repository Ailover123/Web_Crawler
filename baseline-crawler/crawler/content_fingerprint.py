"""Utilities for generating stable fingerprints of HTML content."""

from __future__ import annotations

import hashlib
from typing import Iterable

from bs4 import BeautifulSoup, NavigableString, Tag


def _html_to_semantic_lines(html: str) -> list[str]:
    """Convert HTML into whitespace-stable, semantic lines."""
    soup = BeautifulSoup(html or "", "lxml")
    lines: list[str] = []

    def walk(node, depth: int = 0) -> None:
        indent = "  " * depth

        if isinstance(node, NavigableString):
            text = " ".join(str(node).split())
            if text:
                lines.append(indent + text)
            return

        if isinstance(node, Tag):
            attrs = " ".join(
                f'{key}="{ " ".join(value) if isinstance(value, list) else value }"'
                for key, value in sorted(node.attrs.items())
            )
            lines.append(indent + f"<{node.name}{(' ' + attrs) if attrs else ''}>")

            for child in node.children:
                walk(child, depth + 1)

            lines.append(indent + f"</{node.name}>")

    for child in soup.contents:
        walk(child)

    return lines


def semantic_hash(html: str) -> str:
    """Return a SHA256 fingerprint of the semantic HTML content."""
    lines = _html_to_semantic_lines(html)
    payload = "\n".join(lines)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


__all__: Iterable[str] = ["semantic_hash"]
