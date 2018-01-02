import threading
import collections
import socket

import hiredis

class Redis(threading.Thread):
    def __init__(self, address='localhost', port=6379, *args, **kwargs):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((address, port))
        self.channels = collections.defaultdict(list)
        self.is_alive = True
        self.__reader = hiredis.Reader()

        super().__init__()

    def _process_commands(self, addr, *args):
        command = args[0].decode('utf-8').lower()

        if command == 'subscribe':
            for elem in args[1:]:
                elem = elem.decode('utf-8')
                self.channels[elem] = addr
        elif command == 'published':
            if args[1].decode('utf-8') in self.channels:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sck:
                    sck.connect(addr)
                    sck.sendall(args[2])

    def start(self):
        self.server.listen()
        super().start()

    def run(self):
        while self.is_alive:
            conn, addr = self.server.accept()
            with conn:
                data = conn.recv(65536)
                if not data:
                    continue

                self.__reader.feed(data)
                data = self.__reader.gets()
                self._process_commands(addr, *data)

    def stop(self):
        self.is_alive = False
        self.join()