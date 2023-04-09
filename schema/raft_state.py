from enum import Enum
from time import perf_counter
import os
import random

from schema.base_schema import BaseSchema
from schema.raft_log import Logs
from library.logging import get_logger

logger = get_logger(os.path.basename(__file__))

class Status(Enum):
    LEADER = 1
    FOLLOWER = 2
    CANDIDATE = 3

class RaftState(BaseSchema):
    KEY = -5
    NON_REPR_FIELDS = set(['_election_timeout'])

    def __init__(self, node_num, peers):
        self._log = Logs()
        self._current_term = 1
        self._status = Status.FOLLOWER
        self._voted_for = None
        self._commit_index = 0
        self._last_applied = 0
        self._next_index = {}
        self._match_index = {}
        self._received_votes_from = set()
        self._peers = peers
        self._node_num = node_num
        self.reset_election_timeout()
        self._implicit_leader = None

    def __repr__(self):
        fields = ""
        for key, value in sorted(self.__dict__.items()):
            if key == "_election_timeout":
                continue
            fields += f"{key}={value}, "
        return self.__class__.__name__ + "(" + fields.rstrip(",") + ")"

    def is_leader(self):
        return self.status == Status.LEADER

    def is_follower(self):
        return self.status == Status.FOLLOWER

    def is_candidate(self):
        return self.status == Status.CANDIDATE

    def reset_election_timeout(self):
        self._election_timeout = perf_counter() + random.uniform(0.15, 0.3)

    def election_timeout(self):
        return perf_counter() >= self._election_timeout

    def become_leader(self):
        self.status = Status.LEADER
        self.voted_for = None
        self.next_index = {x: self.log.size() + 1 for x in self.peers}
        self.match_index = {x: 0 for x in self.peers}

    def become_follower(self):
        self.status = Status.FOLLOWER
        self.voted_for = None
        self.reset_election_timeout()

    def become_candidate(self):
        self.current_term += 1
        self.status = Status.CANDIDATE
        self.voted_for = self.node_num
        self.received_votes_from = set()
        self.reset_election_timeout()

    # setters and getters
    @property
    def log(self):
        return self._log
    
    @log.setter
    def log(self, new_log):
        logger.info(f"Node {self._node_num} change {self._log} -> {new_log} for 'log'")
        self._log = new_log

    @property
    def current_term(self):
        return self._current_term
    
    @current_term.setter
    def current_term(self, new_term):
        logger.info(f"Node {self._node_num} change {self._current_term} -> {new_term} for 'current_term'")
        self._current_term = new_term
    
    @property
    def status(self):
        return self._status
    
    @status.setter
    def status(self, new_status):
        logger.info(f"Node {self._node_num} change {self._status} -> {new_status} for 'status'")
        self._status = new_status
    
    @property
    def voted_for(self):
        return self._voted_for
    
    @voted_for.setter
    def voted_for(self, new_voted_for):
        logger.info(f"Node {self._node_num} change {self._voted_for} -> {new_voted_for} for 'voted_for'")
        self._voted_for = new_voted_for
    
    @property
    def commit_index(self):
        return self._commit_index
    
    @commit_index.setter
    def commit_index(self, new_commit_index):
        if new_commit_index == self._commit_index:
            return
        logger.info(f"Node {self._node_num} change {self._commit_index} -> {new_commit_index} for 'commit_index'")
        self._commit_index = new_commit_index
    
    @property
    def last_applied(self):
        return self._last_applied
    
    @last_applied.setter
    def last_applied(self, new_last_applied):
        logger.info(f"Node {self._node_num} change {self._last_applied} -> {new_last_applied} for 'last_applied'")
        self._last_applied = new_last_applied
    
    @property
    def next_index(self):
        return self._next_index
    
    @next_index.setter
    def next_index(self, new_next_index):
        logger.info(f"Node {self._node_num} change {self._next_index} -> {new_next_index} for 'next_index'")
        self._next_index = new_next_index
    
    @property
    def match_index(self):
        return self._match_index
    
    @match_index.setter
    def match_index(self, new_match_index):
        logger.info(f"Node {self._node_num} change {self._match_index} -> {new_match_index} for 'match_index'")
        self._match_index = new_match_index
    
    @property
    def received_votes_from(self):
        return self._received_votes_from
    
    @received_votes_from.setter
    def received_votes_from(self, new_received_votes_from):
        logger.info(f"Node {self._node_num} change {self._received_votes_from} -> {new_received_votes_from} for 'received_votes_from'")
        self._received_votes_from = new_received_votes_from
    
    @property
    def peers(self):
        return self._peers
    
    @peers.setter
    def peers(self, new_peers):
        logger.info(f"Node {self._node_num} change {self._peers} -> {new_peers} for 'peers'")
        self._peers = new_peers
    
    @property
    def node_num(self):
        return self._node_num
    
    @node_num.setter
    def node_num(self, new_node_num):
        logger.info(f"Node {self._node_num} change {self._node_num} -> {new_node_num} for 'node_num'")
        self._node_num = new_node_num
    
    @property
    def implicit_leader(self):
        return self._implicit_leader
    
    @implicit_leader.setter
    def implicit_leader(self, new_implicit_leader):
        if new_implicit_leader == self._implicit_leader:
            return
        logger.info(f"Node {self._node_num} change {self._implicit_leader} -> {new_implicit_leader} for 'implicit_leader'")
        self._implicit_leader = new_implicit_leader
