from collections import Counter

import ray


@ray.remote
class DistributedCounter(object):
    """Distributed Ray Actor wrapper around a standard collections Counter."""

    def __init__(self, *args, **kwargs):
        self.counter = Counter(*args, **kwargs)

    def increment_counter(self, other: Counter):
        self.counter += other

    def decrement_counter(self, other: Counter):
        self.counter -= other

    def intersection(self, other: Counter):
        self.counter &= other

    def union(self, other: Counter):
        self.counter |= other

    def increment(self, key, delta):
        self.counter[key] += delta

    def decrement(self, key, delta):
        self.counter[key] -= delta

    def negate(self, key):
        self.counter[key] = -self.counter[key]

    def divide(self, key, divisor):
        self.counter[key] /= divisor

    def multiply(self, key, multiplier):
        self.counter[key] *= multiplier

    def pow(self, key, exponent):
        self.counter[key] **= exponent

    def most_common(self, n=None):
        return self.counter.most_common(n)

    def elements(self):
        return self.counter.elements()

    def update(self, *args, **kwargs):
        self.counter.update(*args, **kwargs)

    def subtract(self, *args, **kwargs):
        self.counter.subtract(*args, **kwargs)

    def clear(self):
        self.counter.clear()

    def counter(self):
        return self.counter

    def get(self, key):
        return self.counter[key]

    def set(self, key, value):
        self.counter[key] = value

    def setdefault(self, key, default):
        return self.counter.setdefault(key, default)

    def pop(self, key):
        return self.counter.pop(key)

    def popitem(self):
        return self.counter.popitem()

    def items(self):
        return self.counter.items()

    def keys(self):
        return self.counter.keys()

    def values(self):
        return self.counter.values()
