# crawler/baseline_reader.py

from crawler.storage.mysql import get_connection
from crawler.storage.db_guard import DB_SEMAPHORE


def get_baseline_hash(*, site_id: int, normalized_url: str):
    """
    Fetch baseline row for a given page.

    Returns:
        {
            "id": <baseline_id>,
            "content_hash": <sha256>
        }
        or None
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, content_hash
            FROM baseline_pages
            WHERE site_id=%s AND normalized_url=%s
            """,
            (site_id, normalized_url),
        )
        return cur.fetchone()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()
