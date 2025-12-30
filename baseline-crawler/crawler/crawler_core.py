from queue import Queue
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
import time

crawl_queue = Queue(maxsize=10_000)

visited = set()
visited_lock = Lock()

active_workers = 0
active_lock = Lock()

def worker(fetch_fn, extract_fn):
    global active_workers

    while True:
        try:
            url = crawl_queue.get(timeout=2)
        except:
            return  # queue empty → worker exits

        with visited_lock:
            if url in visited:
                crawl_queue.task_done()
                continue
            visited.add(url)

        with active_lock:
            active_workers += 1

        try:
            result = fetch_fn(url)
            if result and result.get("ok", True):
                new_urls = extract_fn(result, url)
                for u in new_urls:
                    if not crawl_queue.full():
                        crawl_queue.put(u)
        finally:
            with active_lock:
                active_workers -= 1
            crawl_queue.task_done()

MIN_WORKERS = 5
MAX_WORKERS = 50

def scale_workers(executor):
    current = executor._max_workers

    queue_pressure = crawl_queue.qsize() / crawl_queue.maxsize

    if queue_pressure > 0.6 and current < MAX_WORKERS:
        executor._max_workers += 2

    elif queue_pressure < 0.2 and current > MIN_WORKERS:
        executor._max_workers -= 1

def start_crawl(seed_urls, fetch_fn, extract_fn):
    for url in seed_urls:
        crawl_queue.put(url)

    with ThreadPoolExecutor(max_workers=MIN_WORKERS) as executor:
        for _ in range(MIN_WORKERS):
            executor.submit(worker, fetch_fn, extract_fn)

        while True:
            scale_workers(executor)

            with active_lock:
                if crawl_queue.empty() and active_workers == 0:
                    break

            time.sleep(3)
