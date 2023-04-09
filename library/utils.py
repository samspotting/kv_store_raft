import re
from collections import OrderedDict

def pairwise(t):
    it = iter(t)
    return zip(it,it)

def chunkwise(t, size=2):
    it = iter(t)
    return zip(*[it]*size)

def strip_field_names(string):
    return re.sub(r"[a-zA-Z_]*=", "", string)

def split_and_pad(string, delimiter, num_splits):
    splits = string.split(delimiter)
    if len(splits) < num_splits:
        splits += [None] * (num_splits - len(splits))
    return splits

class LRUCache:
    def __init__(self, capacity):
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key):
        if key not in self.cache:
            return -1
        else:
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)