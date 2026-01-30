import threading

# Hard global limit on concurrent DB ops
DB_SEMAPHORE = threading.BoundedSemaphore(5)
