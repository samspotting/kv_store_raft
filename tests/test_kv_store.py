import unittest
from library.storage import KVStorage

class TestKVStorage(unittest.TestCase):
    def setUp(self):
        self.store = KVStorage()
        self.store.set("foo", "bar")

    def test_get(self):
        self.assertEqual(self.store.get("foo"), "bar")
        self.assertEqual(self.store.get("nonexistent"), None)

    def test_set(self):
        self.store.set("foo", "baz")
        self.assertEqual(self.store.get("foo"), "baz")

    def test_delete(self):
        self.store.delete("foo")
        self.assertEqual(self.store.get("foo"), None)

if __name__ == '__main__':
    unittest.main()
