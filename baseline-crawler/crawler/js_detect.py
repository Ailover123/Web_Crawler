"""
Detect whether a page requires JavaScript rendering.
Used to escalate React / SPA pages only when necessary.
"""

def needs_js_rendering(html: str) -> bool:
    if not html:
        return True

    h = html.lower()

    # React / SPA shells
    if '<div id="root"' in h:
        return True

    if '<div id="__next"' in h and len(h) < 4000:
        return True

    # No meaningful crawlable content
    if h.count('<a ') == 0 and h.count('<p') == 0:
        return True

    # Heavy JS signals
    if 'window.__initial_state__' in h:
        return True

    return False
