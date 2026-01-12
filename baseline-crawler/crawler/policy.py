"""
Centralized URL policy for blocking/allowing URLs and classifying assets.

All extension and path-based rules live here. Other modules should import and
use URLPolicy instead of duplicating extension lists or ad-hoc checks.
"""

from urllib.parse import urlparse
import re
from typing import Iterable, Dict
from threading import Lock

try:
    from crawler.config import POLICY as _POLICY_CFG  # optional
except Exception:
    _POLICY_CFG = {}


class URLPolicy:
    """
    Central policy for URL filtering and classification.

    Methods:
    - is_http(url): True for http/https
    - has_fragment(url): True if URL contains a fragment (#...)
    - is_asset(url): True for asset/doc/media/script/style/font extensions
    - is_blocked_path(url): True if URL matches taxonomy/blog/tag/search/feed/admin patterns
    - should_crawl(url): Single gate used by queue/frontier to decide enqueue
    """

    # Asset/document/media/script/style/font extensions
    ASSET_EXTENSIONS = {
        # Images
        ".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".ico", ".bmp", ".tiff",
        # Video/Audio
        ".mp4", ".mp3", ".avi", ".mov", ".mkv", ".webm",
        # Archives
        ".zip", ".rar", ".tar", ".gz", ".7z",
        # Documents
        ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
        # Styles/Scripts
        ".css", ".js",
        # Fonts
        ".woff", ".woff2", ".ttf", ".eot",
        # Executables/Installers
        ".exe", ".msi",
    }

    # Maintain the substrings previously blocked in frontier
    BLOCKED_SUBSTRINGS = [
        "canvas",
        "elementor",
        "tag",
    ]

    # Split path patterns so some can be allowed via config (taxonomy)
    _TAXONOMY_PATTERNS: Iterable[str] = (
        r"/categor(y|ies)/",        # /category/ or /categories/
        r"/author/",                # /author/
        r"/archive[s]?/",           # /archive/ or /archives/
        r"/\d{4}/\d{2}/",          # /YYYY/MM/ date archives
    )
    _SYSTEM_PATTERNS: Iterable[str] = (
        r"/(feed|rss|atom)(/|$)",   # feeds
        r"/(wp-json)(/|$)",         # WP API
        r"/(wp-admin|wp-login)(/|$)", # WP admin/login
        r"[?&](s|q|search)=[^&#]+", # search query params
        # Blog pagination (remove pagination-based pages)
        r"/page/\d+(/|$)",         # /page/2, /page/10/
        r"[?&]page=\d+",           # ?page=2
        r"[?&]paged=\d+",          # ?paged=2 (WP)
        r"/pagination(/|$)",        # /pagination or /pagination/
        # WordPress post type listings like ?post_type=job_listing&p=...
        r"[?&]post_type=[^&#]+",    # block any post_type query
    )
    _TAXONOMY_REGEX = re.compile("(" + ")|(".join(_TAXONOMY_PATTERNS) + ")", re.IGNORECASE)
    _SYSTEM_REGEX = re.compile("(" + ")|(".join(_SYSTEM_PATTERNS) + ")", re.IGNORECASE)

    # Stats with thread-safety
    _lock: Lock = Lock()
    _stats: Dict[str, int] = {
        "evaluations": 0,
        "allowed": 0,
        "blocked_non_http": 0,
        "blocked_fragment": 0,
        "blocked_asset": 0,
        "blocked_path_taxonomy": 0,
        "blocked_path_system": 0,
        "blocked_substring": 0,
    }

    @staticmethod
    def is_http(url: str) -> bool:
        try:
            scheme = urlparse(url).scheme
            return scheme in ("http", "https")
        except Exception:
            return False

    @staticmethod
    def has_fragment(url: str) -> bool:
        try:
            return bool(urlparse(url).fragment)
        except Exception:
            return True

    @classmethod
    def is_asset(cls, url: str) -> bool:
        try:
            path = urlparse(url).path.lower()
            return any(path.endswith(ext) for ext in cls.ASSET_EXTENSIONS)
        except Exception:
            return False

    @classmethod
    def is_blocked_path(cls, url: str) -> bool:
        try:
            parsed = urlparse(url)
            path_and_query = (parsed.path or "") + ("?" + parsed.query if parsed.query else "")
            allow_taxonomy = bool(_POLICY_CFG.get("allow_taxonomy_paths", True))
            # System patterns are always blocked unless overridden explicitly in config (not exposed yet)
            if cls._SYSTEM_REGEX.search(path_and_query):
                return True
            if not allow_taxonomy and cls._TAXONOMY_REGEX.search(path_and_query):
                return True
            return False
        except Exception:
            return False

    @classmethod
    def contains_blocked_substring(cls, url: str) -> bool:
        ul = (url or "").lower()
        return any(s in ul for s in cls.BLOCKED_SUBSTRINGS)

    @classmethod
    def should_crawl(cls, url: str) -> bool:
        """Return True/False using the same logic as eval(), and update counters."""
        allowed, _ = cls.eval(url)
        return allowed

    @classmethod
    def eval(cls, url: str):
        """
        Evaluate a URL and return (allowed: bool, reason: str).
        Reasons are one of the stats keys: 'allowed', 'blocked_non_http', 'blocked_fragment',
        'blocked_asset', 'blocked_path_system', 'blocked_path_taxonomy', 'blocked_substring'.
        Always updates counters exactly once per call.
        """
        with cls._lock:
            cls._stats["evaluations"] += 1

        if not cls.is_http(url):
            with cls._lock:
                cls._stats["blocked_non_http"] += 1
            return False, "blocked_non_http"
        if cls.has_fragment(url):
            with cls._lock:
                cls._stats["blocked_fragment"] += 1
            return False, "blocked_fragment"
        if cls.is_asset(url):
            with cls._lock:
                cls._stats["blocked_asset"] += 1
            return False, "blocked_asset"
        # Distinguish taxonomy vs system
        try:
            parsed = urlparse(url)
            path_and_query = (parsed.path or "") + ("?" + parsed.query if parsed.query else "")
            if cls._SYSTEM_REGEX.search(path_and_query):
                with cls._lock:
                    cls._stats["blocked_path_system"] += 1
                return False, "blocked_path_system"
            allow_taxonomy = bool(_POLICY_CFG.get("allow_taxonomy_paths", True))
            if not allow_taxonomy and cls._TAXONOMY_REGEX.search(path_and_query):
                with cls._lock:
                    cls._stats["blocked_path_taxonomy"] += 1
                return False, "blocked_path_taxonomy"
        except Exception:
            # If parsing fails, fall through and let other checks decide
            pass

        if cls.contains_blocked_substring(url):
            with cls._lock:
                cls._stats["blocked_substring"] += 1
            return False, "blocked_substring"
        with cls._lock:
            cls._stats["allowed"] += 1
        return True, "allowed"

    @classmethod
    def get_stats(cls) -> Dict[str, int]:
        with cls._lock:
            return dict(cls._stats)

    @classmethod
    def reset_stats(cls) -> None:
        with cls._lock:
            for k in cls._stats:
                cls._stats[k] = 0
