import threading
import queue
from crawler.js_renderer import render_js_sync
from crawler.normalizer import normalize_rendered_html


class JSRenderWorker(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.queue = queue.Queue()
        self.start()

    def run(self):
        while True:
            url, result_event = self.queue.get()
            try:
                html, final_url = render_js_sync(url)
                result_event["html"] = normalize_rendered_html(html)
                result_event["final_url"] = final_url
            except Exception as e:
                result_event["error"] = e
            finally:
                result_event["done"].set()
                self.queue.task_done()

    # ✅ THIS MUST BE INSIDE THE CLASS
    def render(self, url: str, timeout: int = 30) -> tuple[str, str]:
     event = {
        "done": threading.Event(),
        "html": None,
        "final_url": None,
        "error": None,
     }

     self.queue.put((url, event))
     event["done"].wait(timeout=timeout)

     if event["error"]:
        raise event["error"]

     return event["html"], event["final_url"]


    
