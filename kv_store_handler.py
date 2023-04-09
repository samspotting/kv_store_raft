from library.storage import KVStorage
from schema.kv_store import KVStoreResponse, Status, KVStoreRequest

class KVStoreHandler:
    def __init__(self, kv_storage=None):
        self._kv_storage = kv_storage if kv_storage is not None else KVStorage()

    def receive_msg(self, msg):
        self.receive(KVStoreRequest.deserialize(msg))

    def receive(self, request):
        # execute
        if request.is_get():
            value = self._kv_storage.get(request._key)
            if value is None:
                return KVStoreResponse(Status.NOT_FOUND)
            return KVStoreResponse(Status.SUCCESS, value)
        elif request.is_set():
            self._kv_storage.set(request._key, request._value)
            return KVStoreResponse(Status.SUCCESS)
        elif request.is_delete():
            self._kv_storage.delete(request._key)
            return KVStoreResponse(Status.SUCCESS)

        return KVStoreResponse(Status.FAIL, "bad request")
