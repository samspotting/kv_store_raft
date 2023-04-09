import unittest

from raft_core import RaftCore
from schema.raft_log import LogEntry, Logs

class TestRaftComponent(unittest.TestCase):
    def setUp(self):
        self.log = Logs([
            LogEntry(1, "x"),
            LogEntry(1, "x"),
            LogEntry(2, "x"),
            LogEntry(2, "x"),
            LogEntry(3, "x"),
            LogEntry(4, "x"),
            LogEntry(4, "x"),
            LogEntry(4, "x"),
            LogEntry(4, "x")
        ])

    def test_majority_with_current_term(self):
        current_term = 3
        match_index = {1: 5, 2: 4, 3: 5, 4: 9}
        expected = 5
        result = RaftCore.get_candidate_commit_index(match_index, self.log, current_term, 4)
        self.assertEqual(expected, result)

        current_term = 2
        match_index = {1: 3, 2: 3, 3: 3, 4: 4}
        expected = 3
        result = RaftCore.get_candidate_commit_index(match_index, self.log, current_term, 4)
        self.assertEqual(expected, result)

    def test_majority_without_current_term(self):
        current_term = 3
        match_index = {1: 4, 2: 3, 3: 4, 4: 9}
        expected = None
        result = RaftCore.get_candidate_commit_index(match_index, self.log, current_term, 4)
        self.assertEqual(expected, result)

    def test_no_majority_with_current_term(self):
        current_term = 3
        match_index = {1: 5, 2: 3, 3: 3, 4: 9}
        expected = None
        result = RaftCore.get_candidate_commit_index(match_index, self.log, current_term, 4)
        self.assertEqual(expected, result)

    def test_no_match_index_values(self):
        current_term = 3
        match_index = {}
        expected = None
        result = RaftCore.get_candidate_commit_index(match_index, self.log, current_term, 4)
        self.assertEqual(expected, result)

if __name__ == '__main__':
    unittest.main()
