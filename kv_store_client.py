from socket import socket, AF_INET, SOCK_STREAM, SOL_SOCKET, SO_REUSEADDR, gaierror, herror, error

from schema.kv_store import KVStoreRequest
from schema.kv_store import KVStoreResponse
from library.messages import send_message, recv_message
from service_discovery import ServiceDiscovery

class KVStoreClient:
    def __init__(self, service_discovery):
        self.sock = None
        self.service_discovery = service_discovery
        self.server_addr = self.service_discovery.select_random_server()
        self._connect_server()

    def _connect_new_server(self, idx):
        self.server_addr = self.service_discovery.get_addr(idx)
        self._connect_server(tries=1)

    def _new_socket(self):
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)

    def _connect_server(self, tries=None):
        tries = self.service_discovery.get_total_servers() if tries is None else tries
        success = False
        while not success and tries > 0:
            try:
                self._new_socket()
                self.sock.connect(self.server_addr)
                success = True
            except (ConnectionRefusedError, TimeoutError, gaierror, herror, error) as e:
                self.server_addr = self.service_discovery.select_next_server(self.server_addr)
                self.sock.close()
            tries -= 1
        if not success:
            raise Exception("Failed to connect to a live host")

    def end(self):
        self.sock.close()

    def send_message(self, request):
        msg = request.serialize()
        send_message(self.sock, msg)
        
        response_msg = recv_message(self.sock)
        response = KVStoreResponse.deserialize(response_msg)
        if response.is_redirect():
            print(f"Redirected to: {response}")
            self._connect_new_server(int(response.msg))
            send_message(self.sock, msg)
            redirected_response_msg = recv_message(self.sock)
            redirected_response = KVStoreResponse.deserialize(redirected_response_msg)
            return redirected_response
        return response

def start_kvclient():
    service_discovery = ServiceDiscovery()
    client = KVStoreClient(service_discovery)
    while True:
        msg = input("KV >")
        if not msg:
            break
        request = KVStoreRequest.build_from_msg(msg)
        response = client.send_message(request)
        print(response)
    client.end()

if __name__ == '__main__':
    start_kvclient()
