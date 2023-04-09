from queue import Queue
from socket import socket, AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR
from threading import Thread
import os

from library.logging import get_logger

logger = get_logger(os.path.basename(__file__))

class RaftNetworking:
    """ ascii format for send/receive, ascii encoded binary as wire format """
    def __init__(self, addr, node_num_to_addrs):
        # My own address
        self._msgs_inbound = Queue()
        self._msgs_outbound = {n: Queue() for n in node_num_to_addrs}
        self.node_num_to_addrs = node_num_to_addrs
        self._sock = self.build_socket(addr)

        Thread(target=self._msg_receiver).start()
        for destination in node_num_to_addrs:
            Thread(target=self._msg_sender, args=[destination]).start()

    @classmethod
    def build_socket(cls, addr):
        logger.info(f"Initialized {addr}")
        sock = socket(AF_INET, SOCK_DGRAM)
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        sock.bind(addr)
        return sock
        
    def inbound_queue_empty(self):
        return self._msgs_inbound.empty()

    def end(self):
        self._msgs_inbound.put(None)
	
    # Send a msg to a specific node number (returns immediately)
    def send(self, destination, msg):
        self._msgs_outbound[destination].put(msg)

    # Receive and return any msg sent to me (blocks)
    def receive(self):
        return self._msgs_inbound.get()

    def _msg_sender(self, destination):
        while True:
            obj = self._msgs_outbound[destination].get()
            try:
                self._sock.sendto(obj.serialize(), self.node_num_to_addrs[destination])
            except OSError:
                logger.error("Error: Not connected")

    def _msg_receiver(self):
        while True:
            msg, addr = self._sock.recvfrom(8192)
            self._msgs_inbound.put(msg)
