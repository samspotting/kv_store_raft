from queue import Queue

class RaftNetworkingMock:
    def __init__(self, node_num, msg_queue):
        self._node_num = node_num
        self._msg_queue = msg_queue

    @classmethod
    def init_n_servers(cls, total_nodes):
        queue = {n: Queue() for n in range(total_nodes)}
        raft_networking_lst = [
            RaftNetworkingMock(n, queue) for n in range(total_nodes)
        ]
        return raft_networking_lst

    def inbound_queue_empty(self):
        return self._msg_queue[self._node_num].empty()
	
    def send(self, destination, obj):
        self._msg_queue[destination].put(obj.serialize())

    def receive(self):
        return self._msg_queue[self._node_num].get()

    def end(self):
        self._msg_queue[self._node_num].put(None)
