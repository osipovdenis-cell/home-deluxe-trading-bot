"""Bounded diagnostic watch list: retain idle members to avoid subscription churn."""
import time


class WarmSymbols:
    def __init__(self, capacity, seconds=600):
        self.capacity, self.seconds = capacity, seconds
        self.seen = {}

    def select(self, required, watched=(), now=None):
        now = time.monotonic() if now is None else now
        requested = list(dict.fromkeys((*required, *watched)))[:self.capacity]
        for symbol in requested:
            self.seen[symbol] = now
        retained = sorted((s for s, at in self.seen.items()
                           if s not in requested and now-at < self.seconds),
                          key=lambda s: (-self.seen[s], s))
        selected = tuple((requested+retained)[:self.capacity])
        self.seen = {s: self.seen[s] for s in selected}
        return selected
