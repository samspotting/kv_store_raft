import random

class ServiceDiscovery:
    SERVERS = {
        0: ('localhost', 15000),
        1: ('localhost', 16000),
        2: ('localhost', 17000),
        3: ('localhost', 18000),
        4: ('localhost', 19000),
    }

    def get_addr(self, n):
        return self.SERVERS[n]

    def get_server_config(self):
        return self.SERVERS

    def get_total_servers(self):
        return len(self.SERVERS)

    def select_random_server(self):
        next_server = random.choice(list(self.SERVERS.keys()))
        return self.SERVERS[next_server]

    def select_next_server(self, addr):
        prev_n = None
        for n, next_addr in self.SERVERS.items():
            if next_addr == addr:
                prev_n = n
        if prev_n is None:
            raise Exception("ServiceDiscovery is in bad state")
        return self.SERVERS[(prev_n + 1) % len(self.SERVERS)]
