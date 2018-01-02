# -*- coding: utf8 -*-
#
# Copyright (C) 2011 Sebastian Rahlf <basti at redtoad dot de>
#
# This program is release under the MIT license. You can find the full text of
# the license in the LICENSE file.

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

        self.__curconn = None
        self.__reader = hiredis.Reader()

        super().__init__()

    def _process_commands(self, conn, addr, *args):
        command = args[0].decode('utf-8').lower()

        if command == 'ping':
            try:
                conn.sendall(b'PONG')
            except socket.timeout:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sck:
                    sck.connect(addr)
                    sck.sendall(b'PONG')
        elif command == 'subscribe':
            sub_count = 0
            for elem in args[1:]:
                elem = elem.decode('utf-8')
                self.channels[elem] = addr
                sub_count += 1

            try:
                conn.sendall(sub_count)
            except socket.timeout:
                pass
        elif command == 'published':
            if args[1].decode('utf-8') in self.channels:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sck:
                    sck.connect(addr)
                    sck.sendall(args[2])

    def start(self):
        self.is_alive = True
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
                self._process_commands(conn, addr, *data)

    def stop(self):
        self.is_alive = False
        self.clear_all()
        self.join()

    def clear_all(self):
        self.channels.clear()
