import unittest
from raft_core import RaftCore

class TestStepBackIndex(unittest.TestCase):
    def test_step_back_index(self):
        next_index = {1: 5, 2: 3, 3: 6, 4: 7}
        match_index = {1: 4, 2: 1, 3: 5, 4: 6}

        RaftCore.step_back_index(1, next_index, match_index)
        self.assertEqual(next_index[1], 4)
        self.assertEqual(match_index[1], 3)

        RaftCore.step_back_index(2, next_index, match_index)
        self.assertEqual(next_index[2], 2)
        self.assertEqual(match_index[2], 1)

        RaftCore.step_back_index(3, next_index, match_index)
        self.assertEqual(next_index[3], 5)
        self.assertEqual(match_index[3], 4)

        RaftCore.step_back_index(4, next_index, match_index)
        self.assertEqual(next_index[4], 6)
        self.assertEqual(match_index[4], 5)

    def test_step_back_index_smaller_match_index(self):
        next_index = {1: 9}
        match_index = {1: 4}

        RaftCore.step_back_index(1, next_index, match_index)
        self.assertEqual(next_index[1], 8)
        self.assertEqual(match_index[1], 4)

    def test_step_back_index_no_decrement(self):
        next_index = {1: 1}
        match_index = {1: 0}

        RaftCore.step_back_index(1, next_index, match_index)
        self.assertEqual(next_index[1], 1)
        self.assertEqual(match_index[1], 0)
