
class LogEntry:
    def __init__(self, term, item):
        self.term = term
        self.item = item

    @classmethod
    def from_str(cls, s):
        components = s.split(">")
        if len(components) == 2:
            return LogEntry(int(components[0]), components[1])
        else:
            return LogEntry(components[0], None)

    def __repr__(self):
        return f"{self.term}>{self.item}" if self.item else f"{self.term}"

    def __eq__(self, other):
        if isinstance(other, LogEntry):
            return self.term == other.term and self.item == other.item
        else:
            return False

def log_entries_list_to_str(log_entries):
    return ",".join([str(x) for x in log_entries])

def str_to_log_entries_list(log_entries_str):
    return [LogEntry.from_str(x) for x in log_entries_str.split(",") if x != ""]

class Logs:
    """ idx starts at 1 """
    def __init__(self, entries=None):
        self.logs = entries or []

    def get(self, idx):
        return self.logs[idx - 1]

    def get_n(self, idx, n):
        return self.logs[idx - 1 : idx - 1 + n]

    def get_last(self):
        return self.logs[-1]

    def size(self):
        return len(self.logs)

    def trim(self, idx):
        del self.logs[idx - 1:]

    def add_at(self, idx, entries):
        self.logs[idx - 1 : idx - 1 + len(entries)] = entries

    def get_last_term(self):
        return self.logs[-1].term if self.logs else None

    def __repr__(self):
        return "[" + ','.join([str(x) for x in self.logs]) + "]"

    def __eq__(self, other):
        if isinstance(other, Logs):
            return self.logs == other.logs
        else:
            return False
