"""Process-wide rolling budget for public market connection attempts."""
import threading
import time
from collections import deque


class ConnectionBudget:
    def __init__(self, limit=240, seconds=300, clock=time.monotonic):
        self.limit, self.seconds, self.clock = limit, seconds, clock
        self.attempts = deque()
        self.lock = threading.Lock()

    def acquire(self, stop):
        while not stop.is_set():
            with self.lock:
                now = self.clock()
                while self.attempts and self.attempts[0] <= now-self.seconds:
                    self.attempts.popleft()
                if len(self.attempts) < self.limit:
                    self.attempts.append(now)
                    return True
                delay = min(1., max(.01, self.attempts[0]+self.seconds-now))
            if stop.wait(delay):
                return False
        return False


PUBLIC_CONNECTION_BUDGET = ConnectionBudget()
