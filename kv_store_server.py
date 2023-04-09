import concurrent.futures
from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR
from library.messages import send_message, recv_message
from threading import Thread
from queue import Queue
import os

from schema.kv_store import Status
from schema.kv_store import KVStoreResponse
from schema.kv_store import KVStoreRequest
from schema.raft_rpc import ResultState
from service_discovery import ServiceDiscovery
from kv_store_handler import KVStoreHandler
from raft_runtime import RaftRuntime
from library.logging import get_logger
from library.utils import LRUCache

logger = get_logger(os.path.basename(__file__))

class KVStoreServer:
    """
    1 connection thread
    1 outbound network thread
    n processing threads
    """
    def __init__(self, addr, raft, handler, workers):
        self._handler = handler
        self.outbound_queue = Queue()
        self.addr = addr
        self.workers = workers
        self.raft = raft
        self.running = False
        self.request_cache = LRUCache(1024)
        self.request_id_to_socket = LRUCache(1024)

    @classmethod
    def build_distributed_store(cls, server_number, addr, server_config, handler=None, workers=10):
        raft = RaftRuntime.build_runtime(
            node_num=server_number,
            server_config=server_config)
        handler = handler if handler is not None else KVStoreHandler()
        return KVStoreServer(addr, raft=raft, handler=handler, workers=workers)

    @classmethod
    def build_non_distributed_store(addr, handler=None, workers=1):
        handler = handler if handler is not None else KVStoreHandler()
        return KVStoreServer(addr, handler=handler, workers=workers)

    def start(self):
        self.raft.start()
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, True)
        self.sock.bind(self.addr)
        self.sock.listen()

        self.running = True
        Thread(target=self.send_responses).start()
        Thread(target=self.execute_message).start()

        logger.info(f"Serving traffic at {self.addr}")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            while self.running:
                try:
                    client, _ = self.sock.accept()
                    executor.submit(self.read_request, client)
                except OSError as e:
                    logger.error(f"Socket closed: {e}")
                    break
                except Exception as e:
                    logger.error(f"Unexpected exception occurred: {e}")
                    break

    def stop(self):
        self.running = False
        self.sock.close()
        self.outbound_queue.put(None)
        self.raft.stop()

    def send_responses(self):
        while self.running:
            outbound_pair = self.outbound_queue.get()
            if outbound_pair is not None:
                sock, response = outbound_pair
                msg = response.serialize()
                send_message(sock, msg)

    def read_request(self, sock):
        try:
            while self.running:
                msg = recv_message(sock)
                request = KVStoreRequest.deserialize(msg)
                cached_response = self.request_cache.get(request.id)
                if cached_response != -1:
                    self.outbound_queue.put((sock, cached_response))
                    continue
                log_entry = str(request) if request is not None else None
                redirect = self.raft.redirect_to_leader()
                if redirect is not None:
                    response = KVStoreResponse(Status.REDIRECT, redirect)
                    logger.warning(f"Not leader, redirecting to: {redirect}")
                    self.outbound_queue.put((sock, response))
                elif request.is_get():
                    logger.info("Received GET")
                    response = self._handler.receive(request)
                    self.outbound_queue.put((sock, response))
                else:
                    logger.info("Received DELETE or SET")
                    self.request_id_to_socket.put(request.id, sock)
                    result, _ = self.raft.log_append(log_entry)
                    if result == ResultState.FAIL:
                        logger.error("Failed applying command to state machine")
                        response = KVStoreResponse(Status.FAIL, "Raft failed to append to its log.")
                        self.outbound_queue.put((sock, response))
        except IOError:
            sock.close()

    def execute_message(self):
        while self.running:
            msg = self.raft.get_msg_to_execute()
            if msg is None:
                continue
            request = KVStoreRequest.from_serialized_msg(msg)
            response = self._handler.receive(request)
            sock = self.request_id_to_socket.get(request.id)
            # only reply if original request came to this host
            if sock != -1:
                self.request_cache.put(request.id, response)
                self.outbound_queue.put((sock, response))

def start_kvserver(server_number):
    service_discovery = ServiceDiscovery()
    kv_store = KVStoreServer.build_distributed_store(
        server_number,
        service_discovery.get_addr(server_number),
        service_discovery.get_server_config())
    kv_store.start()

if __name__ == '__main__':
    import sys
    start_kvserver(int(sys.argv[1]))
