from time import sleep
from threading import Thread
import os
from queue import Queue
import uuid

from raft_handler import RaftHandler
from raft_networking import RaftNetworking
from service_discovery import ServiceDiscovery
from library.logging import get_logger

logger = get_logger(os.path.basename(__file__))

class RaftRuntime:
    def __init__(self, raft_networking):
        self._running = False
        self._raft_handler = None
        self._raft_networking = raft_networking
        self._execute_msg_queue = Queue()

    @classmethod
    def build_runtime(cls, node_num=None, server_config=None):
        peers_server_config = {n: addr for n, addr in server_config.items() if n != node_num}
        peers = sorted(peers_server_config.keys())
        addr = server_config[node_num]
        raft_networking = RaftNetworking(addr, peers_server_config)
        runtime = RaftRuntime(raft_networking)
        handler = RaftHandler.build_new(node_num, peers, raft_networking.send, runtime.put_msg_to_execute)
        runtime.add_raft_handler(handler)
        return runtime
    
    def add_raft_handler(self, handler):
        self._raft_handler = handler

    def start(self):
        assert(self._raft_handler is not None)
        self._running = True
        Thread(target=self.election_loop).start()
        Thread(target=self.process_messages).start()
        Thread(target=self.process_heartheat).start()

    def stop(self):
        self._running = False
        self._raft_networking.end()
        self._execute_msg_queue.put(None)

    def restart(self):
        self._raft_handler.restart()

    def running(self):
        return self._running

    def election_loop(self):
        while self._running:
            self._raft_handler.check_election_timeout()
            sleep(0.001)

    def process_heartheat(self):
        while self._running:
            self._raft_handler.handle_heartbeat()
            sleep(0.005)

    def process_messages(self):
        while self._running:
            msg = self._raft_networking.receive()
            if msg is None:  # touch, used to end blocked threads on queue.get()
                continue
            self._raft_handler.receive(msg)
    
    def get_msg_to_execute(self):
        return self._execute_msg_queue.get()
    
    def put_msg_to_execute(self, msg):
        return self._execute_msg_queue.put(msg)
    
    def log_append(self, msg):
        return self._raft_handler.log_append(msg)
    
    def redirect_to_leader(self):
        return self._raft_handler.redirect_to_leader()

def console(node_num):
    service_discovery = ServiceDiscovery()
    runtime = RaftRuntime.build_runtime(
        node_num=node_num,
        server_config=service_discovery.get_server_config())
    runtime.start()

    while True:
        print(f"Node {runtime._raft_handler.node_num} {'RUNNING' if runtime.running() else 'NOT RUNNING'} as {runtime._raft_handler.get_status()}")
        cmd = input("on/off/leader/follower/req/r/state > ")
        if cmd == "on":
            runtime.start()
        elif cmd == "off":
            runtime.stop()
        elif cmd == "leader":
            runtime._raft_handler._state.become_leader()
        elif cmd == "candidate":
            runtime._raft_handler._state.become_candidate()
        elif cmd == "follower":
            runtime._raft_handler._state.become_follower()
        elif cmd == "req":
            runtime._raft_handler.log_append(str(uuid.uuid4()) + "test")
        elif cmd == "r":
            runtime._raft_handler.restart()
        elif cmd == "state":
            print(runtime._raft_handler._state)

if __name__ == '__main__':
    import sys
    console(int(sys.argv[1]))
