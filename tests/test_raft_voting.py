import unittest
import time

from tests.raft_networking_mock import RaftNetworkingMock
from raft_handler import RaftHandler
from schema.raft_state import RaftState
from schema.raft_log import LogEntry

class TestRaftVoting(unittest.TestCase):

    CLUSTER_SIZE = 5

    def setUp(self):
        self._raft_networking_lst = RaftNetworkingMock.init_n_servers(self.CLUSTER_SIZE)
        self._raft_handlers = []
        self._running = True

        entries = [
            LogEntry(1, 'x<3'),
            LogEntry(1, 'y<1'),
            LogEntry(1, 'y<9'),
            LogEntry(2, 'x<2'),
            LogEntry(3, 'x<0'),
            LogEntry(3, 'y<7'),
            LogEntry(3, 'x<5'),
            LogEntry(3, 'x<4')
        ]
        all_entries = len(entries) + 1
        cluster_to_cutoff = {
            0: all_entries,
            1: 5,
            2: all_entries,
            3: 2,
            4: -1
        }
        for server, raft_networking in enumerate(self._raft_networking_lst):
            peers = [peer for peer in range(len(self._raft_networking_lst)) if peer != server]
            state = RaftState(server, peers)
            state._log.logs = entries[:cluster_to_cutoff[server]]
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

    def test_replicate_message(self):
        self._raft_handlers[0].request_vote()
        self.step()
        assert(self._raft_handlers[0]._state.is_leader())

        self._raft_handlers[1].request_vote()
        self.step()
        assert(self._raft_handlers[1]._state.is_candidate())

        self._raft_handlers[2].request_vote()
        self.step()
        assert(self._raft_handlers[2]._state.is_leader())
        assert(self._raft_handlers[2]._state._received_votes_from == set([0,1,3,4]))

        self._raft_handlers[3].request_vote()
        self.step()
        assert(self._raft_handlers[3]._state.is_candidate())
        assert(len(self._raft_handlers[3]._state._received_votes_from) == 0)

        self._raft_handlers[4].request_vote()
        self.step()
        assert(self._raft_handlers[4]._state.is_leader())
        assert(self._raft_handlers[4]._state._received_votes_from == set([1,3]))
