from enum import Enum
import re

from schema.base_schema import BaseSchema
from schema.raft_log import LogEntry, log_entries_list_to_str, str_to_log_entries_list
from library.utils import split_and_pad, strip_field_names, chunkwise

class AppendEntries(BaseSchema):
    KEY = 0

    def __init__(self, term, leader_id, prev_term, prev_index, leader_commit, entries):
        assert(term is not None and leader_id is not None and prev_index is not None and leader_commit is not None)
        self._term = term
        self._leader_id = leader_id
        self._prev_term = prev_term
        self._prev_index = prev_index
        self._leader_commit = leader_commit
        self._entries = entries

    @property
    def term(self):
        return self._term

    @property
    def leader_id(self):
        return self._leader_id

    @property
    def prev_term(self):
        return self._prev_term

    @property
    def prev_index(self):
        return self._prev_index

    @property
    def leader_commit(self):
        return self._leader_commit

    @property
    def entries(self):
        return self._entries

class AppendEntriesResponse(BaseSchema):
    KEY = 1

    def __init__(self, success, follower_id, match_index = None):
        assert(
            follower_id is not None and
            (
                (success == True and match_index is not None) or
                success == False
            )
        )
        self._success = success
        self._follower_id = follower_id
        self._match_index = match_index

    @property
    def success(self):
        return self._success

    @property
    def follower_id(self):
        return self._follower_id

    @property
    def match_index(self):
        return self._match_index

class RequestVote(BaseSchema):
    KEY = 2

    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        assert(
            term is not None and
            candidate_id is not None and
            last_log_index is not None
        )
        self._term = term
        self._candidate_id = candidate_id
        self._last_log_index = last_log_index
        self._last_log_term = last_log_term

    @property
    def term(self):
        return self._term

    @property
    def candidate_id(self):
        return self._candidate_id

    @property
    def last_log_index(self):
        return self._last_log_index

    @property
    def last_log_term(self):
        return self._last_log_term

class RequestVoteResponse(BaseSchema):
    KEY = 3

    def __init__(self, follower_id, term, vote_granted):
        assert(
            follower_id is not None and
            term is not None and
            vote_granted is not None
        )
        self._follower_id = follower_id
        self._term = term
        self._vote_granted = vote_granted

    @property
    def follower_id(self):
        return self._follower_id

    @property
    def term(self):
        return self._term

    @property
    def vote_granted(self):
        return self._vote_granted

class ResultState(Enum):
    COMMITED = 1
    PENDING_REPLICATION = 2
    FAIL = 3
    REDIRECT = 4
