"""Bounded, short-lived exact user totals with single-flight SQL execution."""
from collections import OrderedDict
from concurrent.futures import Future
from threading import Lock
from time import monotonic


class UserStatsCache:
    def __init__(self, ttl=30.0, capacity=128, clock=monotonic):
        self.ttl, self.capacity, self.clock = ttl, capacity, clock
        self.lock = Lock()
        self.values = OrderedDict()
        self.pending = {}
        self.epochs = {}

    def invalidate_site(self, site):
        with self.lock:
            self.epochs[site] = self.epochs.get(site, 0) + 1
            for key in list(self.values):
                if key[0] == site:
                    self.values.pop(key)

    def get(self, key, load):
        with self.lock:
            generation = self.epochs.get(key[0], 0)
            key = (*key, generation)
            cached = self.values.get(key)
            if cached and cached[0] > self.clock():
                self.values.move_to_end(key)
                return cached[1]
            self.values.pop(key, None)
            future = self.pending.get(key)
            owner = future is None
            if owner:
                future = self.pending[key] = Future()
        if not owner:
            return future.result()
        try:
            result = load()
        except BaseException as error:
            with self.lock:
                self.pending.pop(key, None)
            future.set_exception(error)
            raise
        with self.lock:
            if self.epochs.get(key[0], 0) == generation:
                self.values[key] = (self.clock() + self.ttl, result)
                self.values.move_to_end(key)
            while len(self.values) > self.capacity:
                self.values.popitem(last=False)
            self.pending.pop(key, None)
        future.set_result(result)
        return result


user_stats_cache = UserStatsCache()
