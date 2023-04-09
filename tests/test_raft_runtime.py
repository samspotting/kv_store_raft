import unittest
import time
import uuid

from tests.raft_networking_mock import RaftNetworkingMock
from raft_runtime import RaftRuntime
from raft_handler import RaftHandler

class TestRaftRuntime(unittest.TestCase):

    CLUSTER_SIZE = 5

    def setUp(self):
        raft_networking_lst = RaftNetworkingMock.init_n_servers(self.CLUSTER_SIZE)
        self.raft_runtimes = []
        for server, raft_networking in enumerate(raft_networking_lst):
            peers = [peer for peer in range(len(raft_networking_lst)) if peer != server]
            raft_handler = RaftHandler.build_new(
                node_num=server,
                peers=peers,
                send_message_callback=raft_networking.send,
                execute_message_callback=None)
            raft_runtime = RaftRuntime(
                    raft_networking=raft_networking)
            raft_runtime.add_raft_handler(raft_handler)
            self.raft_runtimes.append(raft_runtime)

    def test_replicate_message(self):
        for runtime in self.raft_runtimes:
            runtime.start()
        self.raft_runtimes[0]._raft_handler._state.become_leader()
        id = str(uuid.uuid4())
        self.raft_runtimes[0]._raft_handler.log_append(id + "test")
        time.sleep(1)
        assert(
            str(self.raft_runtimes[0]._raft_handler._state) ==
            'RaftState(_commit_index=1, _current_term=1, _implicit_leader=None, _last_applied=0, _log=[1>' + id + 'test], _match_index={1: 1, 2: 1, 3: 1, 4: 1}, _next_index={1: 2, 2: 2, 3: 2, 4: 2}, _node_num=0, _peers=[1, 2, 3, 4], _received_votes_from=set(), _status=Status.LEADER, _voted_for=None, )'
        )
        assert(
            str(self.raft_runtimes[1]._raft_handler._state) ==
            'RaftState(_commit_index=1, _current_term=1, _implicit_leader=0, _last_applied=0, _log=[1>' + id + 'test], _match_index={}, _next_index={}, _node_num=1, _peers=[0, 2, 3, 4], _received_votes_from=set(), _status=Status.FOLLOWER, _voted_for=None, )'
        )
        id2 = str(uuid.uuid4())
        self.raft_runtimes[0]._raft_handler.log_append(id2 + "test")
        time.sleep(1)
        assert(
            str(self.raft_runtimes[0]._raft_handler._state) ==
            'RaftState(_commit_index=2, _current_term=1, _implicit_leader=None, _last_applied=0, _log=[1>' + id + 'test,1>' + id2 + 'test], _match_index={1: 2, 2: 2, 3: 2, 4: 2}, _next_index={1: 3, 2: 3, 3: 3, 4: 3}, _node_num=0, _peers=[1, 2, 3, 4], _received_votes_from=set(), _status=Status.LEADER, _voted_for=None, )'
        )
        assert(
            str(self.raft_runtimes[1]._raft_handler._state) ==
            'RaftState(_commit_index=2, _current_term=1, _implicit_leader=0, _last_applied=0, _log=[1>' + id + 'test,1>' + id2 + 'test], _match_index={}, _next_index={}, _node_num=1, _peers=[0, 2, 3, 4], _received_votes_from=set(), _status=Status.FOLLOWER, _voted_for=None, )'
        )

    def tearDown(self):
        for runtime in self.raft_runtimes:
            runtime.stop()
