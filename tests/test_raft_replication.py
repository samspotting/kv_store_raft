import unittest
import time
from copy import deepcopy

from tests.raft_networking_mock import RaftNetworkingMock
from raft_handler import RaftHandler
from schema.raft_state import RaftState
from schema.raft_log import LogEntry

class TestRaftReplication(unittest.TestCase):

    CLUSTER_SIZE = 5

    def setUp(self):
        self._raft_networking_lst = RaftNetworkingMock.init_n_servers(self.CLUSTER_SIZE)
        self._raft_handlers = []
        self._running = True

        self.entries = [
            LogEntry(1, 'x<3'),
            LogEntry(1, 'y<1'),
            LogEntry(1, 'y<9'),
            LogEntry(2, 'x<2'),
            LogEntry(3, 'x<0'),
            LogEntry(3, 'y<7'),
            LogEntry(3, 'x<5'),
            LogEntry(3, 'x<4')
        ]
        all_entries = len(self.entries) + 1
        cluster_to_cutoff = {
            0: all_entries,
            1: 5,
            2: all_entries,
            3: 2,
            4: -1
        }
        self.logs = {
            n: self.entries[:cluster_to_cutoff[n]] for n in range(self.CLUSTER_SIZE)
        }
        for server, raft_networking in enumerate(self._raft_networking_lst):
            peers = [peer for peer in range(len(self._raft_networking_lst)) if peer != server]
            state = RaftState(server, peers)
            state._log.logs = deepcopy(self.logs[server])
            state.current_term = 3
            self._raft_handlers.append(
                RaftHandler(
                    state,
                    send_message_callback=raft_networking.send))

    def receive(self, n):
        while not self._raft_networking_lst[n].inbound_queue_empty():
            msg = self._raft_networking_lst[n].receive()
            if msg == "":  # touch, used to end blocked threads on queue.get()
                continue
            self._raft_handlers[n].receive(msg)
    
    def step(self):
        for n in range(self.CLUSTER_SIZE):
            self.receive(n)
        for n in range(self.CLUSTER_SIZE):
            self.receive(n)

    def test_replicate_message_no_change(self):
        self._raft_handlers[0]._state.become_leader()
        for handler in self._raft_handlers:
            handler._state.next_index={1: 8, 2: 8, 3: 8, 4: 8}
            handler._state.match_index={1: 0, 2: 0, 3: 0, 4: 0}
        self._raft_handlers[0].handle_heartbeat()
        self.step()

        for n, handler in enumerate(self._raft_handlers):
            if n != len(self._raft_handlers) - 1:
                assert(str(handler._state._log.logs) == str(self.logs[handler._state.node_num]))
            else:
                assert(str(handler._state._log.logs) == str(self.entries))

    def test_replicate_message_next(self):
        self._raft_handlers[0]._state.become_leader()
        for handler in self._raft_handlers:
            handler._state.next_index={1: 6, 2: 9, 3: 3, 4: 8}
            handler._state.match_index={1: 5, 2: 8, 3: 2, 4: 7}
        self._raft_handlers[0].handle_heartbeat()
        self.step()
        self.step()

        for handler in self._raft_handlers:
            assert(str(handler._state._log.logs) == str(self.entries))
