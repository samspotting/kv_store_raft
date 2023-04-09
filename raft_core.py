import os

from library.logging import get_logger

logger = get_logger(os.path.basename(__file__))

def add_inverse_idx_ending_at_one(lst):
    n = len(lst)
    result = []
    for i in range(n):
        result.append((lst[i], n - i))
    return result

class RaftCore:
    @classmethod
    def append_entries(cls, log, prev_index, prev_term, entries):
        assert(prev_index >= 0)
        if prev_index == 0:
            assert(prev_term == None)
            if entries:
                log.add_at(1, entries)
            return True
        if prev_index > log.size():
            logger.warning(f"append_entries fail - Missing entry at idx {prev_index}")
            return False
        if not entries:
            if log.get(prev_index).term == prev_term:
                return True
            logger.warning(f"append_entries fail - Prev term mismatch")
            return False
        if log.get(prev_index).term == prev_term:
            if prev_index < log.size():
                next_term = entries[0].term
                if log.get(prev_index + 1).term != next_term:
                    logger.warning(f"append_entries - Trimming from {prev_index + 1}")
                    log.trim(prev_index + 1)
            log.add_at(prev_index + 1, entries)
            return True
        logger.warning("append_entries fail - Prev term mismatch")
        return False

    @classmethod
    def get_candidate_commit_index(cls, match_index, log, current_term, num_peers):
        match_index_candidates = sorted([x for x in match_index.values()])
        match_index_candidates_with_idx = add_inverse_idx_ending_at_one(match_index_candidates)
        majority_count = ((num_peers + 1) // 2) + 1
        majority_candidates = [x[0] for x in match_index_candidates_with_idx if x[1] >= majority_count]
        candidate = None
        if majority_candidates:
            for log_idx in range(majority_candidates[-1], 0, -1):
                if log.get(log_idx).term == current_term:
                    candidate = log_idx
                    break
        return candidate

    @classmethod
    def step_back_index(cls, follower_id, next_index, match_index):
        if next_index.get(follower_id, 0) > 1:
            next_index[follower_id] -= 1
        prev_match_index = str(match_index)
        match_index[follower_id] = min(next_index[follower_id] - 1, match_index[follower_id])
        logger.info(f"Leader, match index change from {prev_match_index} to {str(match_index)}")
