import unittest
import copy
from raft_core import RaftCore
from schema.raft_log import Logs, LogEntry

def build_terms_only(lst_of_terms):
    return Logs([LogEntry(term, None) for term in lst_of_terms])

class TestAppendEntries(unittest.TestCase):
    def test_empty_log(self):
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([]), 0, None, [ LogEntry(8, "x") ]),
            True)

    def test_empty_entries(self):
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,4,4,5,5,6,6]), 10, 6, []),
            False)
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,4,4,5,5,6,6,6,6]), 10, 6, []),
            True)

    def test_cases(self):
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,4,4,5,5,6,6]), 10, 6, [ LogEntry(8, "x") ]),
            False)
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,4]), 10, 6, [ LogEntry(8, "x") ]),
            False)
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,4,4,5,5,6,6,6,6]), 10, 6, [ LogEntry(8, "x") ]),
            True)
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,4,4,5,5,6,6,6,7,7]), 10, 6, [ LogEntry(8, "x") ]),
            True)
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,4,4,4,4]), 10, 6, [ LogEntry(8, "x") ]),
            False)
        self.assertEqual(
            RaftCore.append_entries(build_terms_only([1,1,1,2,2,2,3,3,3,3,3]), 10, 6, [ LogEntry(8, "x") ]),
            False)

    def test_idempotent(self):
        logs1 = build_terms_only([1,1,1,4,4,5,5,6,6,6,6])
        RaftCore.append_entries(logs1, 10, 6, [ LogEntry(8, "x") ])
        logs1_2 = copy.deepcopy(logs1)
        RaftCore.append_entries(logs1_2, 10, 6, [ LogEntry(8, "x") ])
        self.assertEqual(
            logs1,
            logs1_2)
        logs2 = build_terms_only([1,1,1,4,4,5,5,6,6,6,6])
        RaftCore.append_entries(logs2, 10, 6, [ LogEntry(8, "x") ])
        logs2_2 = copy.deepcopy(logs2)
        RaftCore.append_entries(logs2_2, 10, 6, [ LogEntry(8, "x") ])
        self.assertEqual(
            logs2,
            logs2_2)

if __name__ == '__main__':
    unittest.main()
