# crawler/storage/crawl_reader.py

from crawler.storage.mysql import get_connection


def iter_crawl_urls(*, siteid: int):
    """
    Yield unique URLs discovered during crawl.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT url, id
            FROM crawl_pages
            WHERE siteid = %s
              AND content_type LIKE 'text/html%%'
            GROUP BY url
            ORDER BY MIN(id) ASC
            """,
            (siteid,),
        )

        rows = cur.fetchall()
        return [url for (url, id) in rows]

    finally:
        cur.close()
        conn.close()
