import unittest
from unittest.mock import MagicMock

from schema.kv_store import KVStoreRequest, KVStoreResponse, Status, Action
from kv_store_handler import KVStoreHandler

class TestProcessor(unittest.TestCase):
    def setUp(self):
        self._kv = MagicMock()
        self._handler = KVStoreHandler(self._kv)
    
    def test_set(self):
        self._handler.receive(KVStoreRequest(Action.SET, "foo", "bar"))
        self._kv.set.assert_called_once_with('foo', 'bar')
    
    def test_delete(self):
        self._handler.receive(KVStoreRequest(Action.DELETE, "foo"))
        self._kv.delete.assert_called_once_with('foo')
    
    def test_get(self):
        self._kv.get.return_value = 'bar'
        result = self._handler.receive(KVStoreRequest(Action.GET, "foo"))
        self.assertEqual(result, KVStoreResponse(Status.SUCCESS, 'bar'))
        self._kv.get.assert_called_once_with('foo')

if __name__ == '__main__':
    unittest.main()