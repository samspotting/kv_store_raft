import os
import time
from collections import namedtuple

from raft_core import RaftCore
from schema.raft_state import RaftState
from schema.raft_log import LogEntry
from schema.raft_rpc import AppendEntries
from schema.raft_rpc import AppendEntriesResponse
from schema.raft_rpc import RequestVote
from schema.raft_rpc import RequestVoteResponse
from schema.raft_rpc import ResultState
from library.logging import get_logger
from library.utils import LRUCache

logger = get_logger(os.path.basename(__file__))

RaftResult = namedtuple('MyNamedTuple', ['raft_result_state', 'prev_index'])

class RaftHandler:
    REQUEST_TIMEOUT = 0.5

    def __init__(self, state, send_message_callback, execute_message_callback=None, core=None):
        self._core = core if core is not None else RaftCore()
        self._state = state
        self._send_message_callback = send_message_callback
        self._execute_message_callback = execute_message_callback
        self._request_cache = LRUCache(1024)

    @classmethod
    def build_new(self, node_num, peers, send_message_callback, execute_message_callback):
        state = RaftState(node_num, peers)
        return RaftHandler(state, send_message_callback, execute_message_callback)
        
    @property
    def node_num(self):
        return self._state.node_num
        
    @property
    def peers(self):
        return self._state.peers

    def get_status(self):
        return self._state.status

    def restart(self):
        self._state = RaftState(self._state.node_num, self._state.peers)

    def receive(self, msg):
        if AppendEntries.is_type(msg):
            self.handle_append_entries(AppendEntries.deserialize(msg))
        elif AppendEntriesResponse.is_type(msg):
            self.handle_append_entries_response(AppendEntriesResponse.deserialize(msg))
        elif RequestVote.is_type(msg):
            request_vote = RequestVote.deserialize(msg)
            logger.info(f"<<<Received {request_vote}")
            self.handle_request_vote(request_vote)
        elif RequestVoteResponse.is_type(msg):
            request_vote_response = RequestVote.deserialize(msg)
            logger.info(f"<<<Received {request_vote_response}")
            self.handle_request_vote_response(request_vote_response)
        else:
            raise ValueError("Invalid Request Type")

    def check_election_timeout(self):
        if not self._state.is_leader() and self._state.election_timeout():
            self.request_vote()

    def redirect_to_leader(self):
        if not self._state.is_leader():
            return self._state.implicit_leader
        return None

    def log_append(self, msg):
        if not self._state.is_leader():
            return ResultState.FAIL, None
        id = msg[:36]
        result = self._request_cache.get(id)
        if result == -1:
            result = self.handle_client_log_append(msg)
            self._request_cache.put(id, result)
        else:
            if result.raft_result_state == ResultState.PENDING_REPLICATION:
                result = self.get_or_wait_for_raft_result(result.prev_index, self.REQUEST_TIMEOUT)
                if result.raft_result_state == ResultState.COMMITED:
                    self._request_cache.put(id, result)
        return result.raft_result_state, None

    def get_or_wait_for_raft_result(self, prev_index, timeout):
        start_time = time.time()
        while time.time() - start_time < timeout and\
                self._state.commit_index < prev_index:
            time.sleep(0.01)
        if self._state.commit_index >= prev_index:
            return RaftResult(ResultState.COMMITED, prev_index)
        else:
            return RaftResult(ResultState.PENDING_REPLICATION, prev_index)

    def handle_client_log_append(self, msg):
        # Client adds a log entry (received by leader)
        entries = [LogEntry(self._state.current_term, msg)]
        prev_index = self._state.log.size()
        prev_term = self._state.log.get_last_term()
        success = self._core.append_entries(
            log=self._state.log,
            prev_index=prev_index,
            prev_term=prev_term,
            entries=entries)
        if success:
            return self.get_or_wait_for_raft_result(prev_index, self.REQUEST_TIMEOUT)
        else:
            return RaftResult(ResultState.FAIL, prev_index)

    def request_vote(self):
        self._state.become_candidate()
        for peer in self.peers:
            request_vote = RequestVote(
                term=self._state.current_term,
                candidate_id=self.node_num,
                last_log_index=self._state.log.size(),
                last_log_term=self._state.log.get_last_term()
            )
            self._send_message_callback(peer, request_vote)

    def handle_request_vote(self, request_vote):
        self._term_check(request_vote.term)
        vote_for_requester = self._state.voted_for is None or self._state.voted_for == request_vote.candidate_id
        requester_term_gt = (request_vote.last_log_term or 0) > (self._state.log.get_last_term() or 0)
        requester_term_tie = (
            request_vote.last_log_term == self._state.log.get_last_term() and
            request_vote._last_log_index >= self._state.log.size())
        requester_term_check = requester_term_gt or requester_term_tie
        vote_granted = vote_for_requester and requester_term_check
        if vote_granted:
            self._state.voted_for = request_vote.candidate_id
        request_vote_response = RequestVoteResponse(
            follower_id=self.node_num,
            term=self._state.current_term,
            vote_granted=vote_granted
        )
        logger.info(f">>>Sending vote for {request_vote.candidate_id}: {request_vote_response}. Reason: term_check={requester_term_check} not_voted_others: {vote_for_requester}")
        self._send_message_callback(request_vote.candidate_id, request_vote_response)

    def handle_request_vote_response(self, request_vote_response):
        self._term_check(request_vote_response.term)
        if request_vote_response.vote_granted:
            self._state.received_votes_from.add(request_vote_response.follower_id)
            if len(self._state.received_votes_from) + 1 > (len(self.peers) + 1) // 2:
                self._state.become_leader()
                self.handle_heartbeat()

    def _term_check(self, request_term):
        if request_term > self._state.current_term:
            self._state.become_follower()
            self._state.current_term = request_term
            return True
        return False

    def handle_append_entries(self, append_entries):
        # Update to the log (received by a follower)
        if self._state.is_leader() and append_entries.term == self._state.current_term:
            raise Exception("Received a vote from a leader with the same term")
        self._term_check(append_entries.term)
        if self._state.is_follower():
            self._state.reset_election_timeout()
            success = self._core.append_entries(
                    log=self._state.log,
                    prev_index=append_entries.prev_index,
                    prev_term=append_entries.prev_term,
                    entries=append_entries.entries)
            append_entries_response = None
            if success:
                self._state.implicit_leader = append_entries.leader_id
                if append_entries.leader_commit >= self._state.commit_index:
                    self._state.commit_index = min(
                        append_entries.leader_commit,
                        append_entries.prev_index + len(append_entries.entries))
                    if self._execute_message_callback is not None:
                        for idx in range(self._state.last_applied + 1, self._state.commit_index + 1):
                            command = self._state.log.get(idx).item
                            logger.info(f"Following adding command {idx} for execution: {command}")
                            self._execute_message_callback(command)
                            self._state.last_applied = idx
                append_entries_response = AppendEntriesResponse(
                    success=True,
                    follower_id=self.node_num,
                    match_index=append_entries.prev_index + len(append_entries.entries)
                )
                if append_entries.entries:
                    logger.info(f"{self._state.node_num} log changed")
            else:
                append_entries_response = AppendEntriesResponse(
                    success=False,
                    follower_id=self.node_num)

            if append_entries.entries or not success:
                logger.info(f"<<<Received {append_entries}, responding >>> {append_entries_response}")
            self._send_message_callback(append_entries.leader_id, append_entries_response)

    def handle_append_entries_response(self, response):
        if response.success:
            if response.match_index > self._state.match_index.get(response.follower_id, 0):
                from_match_index = str(self._state.match_index[response.follower_id])
                from_next_index = str(self._state.next_index[response.follower_id])
                self._state.match_index[response.follower_id] = response.match_index
                self._state.next_index[response.follower_id] = response.match_index + 1
                logger.info(f"match index for {response.follower_id}: {from_match_index} -> {self._state.match_index[response.follower_id]}")
                logger.info(f"next index for {response.follower_id}: {from_next_index} -> {self._state.next_index[response.follower_id]}")
                candidate = self._core.get_candidate_commit_index(
                    self._state.match_index,
                    self._state.log,
                    self._state.current_term,
                    len(self.peers))
                if candidate and candidate > self._state.commit_index:
                    self._state.commit_index = candidate

                    if self._execute_message_callback is not None:
                        for idx in range(self._state.last_applied + 1, self._state.commit_index + 1):
                            command = self._state.log.get(idx).item
                            logger.info(f"Following adding command {idx} for execution: {command}")
                            self._execute_message_callback(command)
                            self._state.last_applied = idx
        else:
            self._core.step_back_index(response.follower_id, self._state.next_index, self._state.match_index)

    def handle_heartbeat(self):
        if self._state.is_leader():
            for follower in self.peers:
                entries_to_send = self._state.log.size() - self._state.next_index[follower] + 1
                entries = self._state.log.get_n(self._state.next_index[follower], entries_to_send)
                prev_index = self._state.next_index[follower] - 1
                prev_term = self._state.log.get(prev_index).term if self._state.next_index[follower] > 1 else None
                append_entries = AppendEntries(
                    term=self._state.current_term,
                    leader_id=self.node_num,
                    prev_term=prev_term,
                    prev_index=prev_index,
                    leader_commit=self._state.commit_index,
                    entries=entries)
                self._send_message_callback(follower, append_entries)
