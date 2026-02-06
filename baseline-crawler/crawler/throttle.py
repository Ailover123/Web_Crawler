import threading
import time
import logging

logger = logging.getLogger(__name__)

# Thread-safe global domain-wide pauses
SITE_PAUSES = {}               # siteid -> timestamp
SITE_SCALE_DOWN_REQUESTS = {}  # siteid -> bool
PAUSE_LOCK = threading.Lock()

def set_pause(siteid, seconds=5):
    """Signals all workers for this site to pause and triggers a scale-down."""
    if not siteid: return
    with PAUSE_LOCK:
        SITE_PAUSES[siteid] = time.time() + seconds
        if seconds > 0:
            SITE_SCALE_DOWN_REQUESTS[siteid] = True
            logger.warning(f"[THROTTLE] Site {siteid} hit 429. Setting DOMAIN-WIDE PAUSE for {seconds}s and requesting SCALE DOWN.")
        else:
            logger.info(f"[THROTTLE] Site {siteid} pause cleared.")

def should_scale_down(siteid):
    """Returns True if a scale-down was requested for this site."""
    with PAUSE_LOCK:
        return SITE_SCALE_DOWN_REQUESTS.get(siteid, False)

def reset_scale_down(siteid):
    """Resets the scale-down request flag."""
    with PAUSE_LOCK:
        SITE_SCALE_DOWN_REQUESTS[siteid] = False

def get_remaining_pause(siteid):
    """Checks if the site is currently in a global pause."""
    if not siteid: return 0
    with PAUSE_LOCK:
        remaining = SITE_PAUSES.get(siteid, 0) - time.time()
        return max(0, remaining)
