class KVStorage(object):
    def __init__(self):
        self.kv = {}
    
    def get(self, key):
        return self.kv.get(key, None)

    def set(self, key, value):
        self.kv[key] = value

    def delete(self, key):
        del self.kv[key]
