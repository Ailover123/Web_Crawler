"""
Detect whether a page requires JavaScript rendering.
Used to escalate React / SPA pages only when necessary.
"""

def needs_js_rendering(html: str) -> bool:
    if not html:
        return True

    h = html.lower()

    # Explicit SPA roots
    if (
        '<div id="root"' in h or
        '<div id="app"' in h or
        '<app-root' in h or
        '<div id="__next"' in h
    ):
        return True

    # Body exists but is empty / shell
    if "<body" in h:
        body_start = h.find("<body")
        body = h[body_start:]

        # No real content indicators
        if (
            "<a " not in body and
            "<p" not in body and
            "<main" not in body and
            "<article" not in body and
            "<section" not in body
        ):
            return True

    return False
