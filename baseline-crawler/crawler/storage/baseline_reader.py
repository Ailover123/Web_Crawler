# crawler/baseline_reader.py

from crawler.storage.mysql import get_connection
from crawler.storage.db_guard import DB_SEMAPHORE


from crawler.normalizer import get_canonical_id

def get_baseline_hash(*, site_id: int, normalized_url: str, base_url: str | None = None):
    """
    Fetch baseline row for a given page.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        canonical_url = get_canonical_id(normalized_url, base_url)
        cur.execute(
            """
            SELECT id, content_hash
            FROM baseline_pages
            WHERE site_id=%s AND normalized_url=%s
            """,
            (site_id, canonical_url),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()
