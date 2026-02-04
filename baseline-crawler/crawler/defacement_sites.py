# crawler/db/defacement_sites.py
from crawler.storage.mysql import get_connection
from crawler.storage.db_guard import DB_SEMAPHORE


def get_selected_defacement_rows():
    """Return defacement_sites rows marked as 'selected'.

    Uses the shared MySQL connection pool and DB_SEMAPHORE, so we must
    release the semaphore after closing the connection.
    """
    conn = get_connection()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT siteid, url, baseline_id, threshold
            FROM defacement_sites
            WHERE action = 'selected'
            """
        )
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()
        DB_SEMAPHORE.release()
