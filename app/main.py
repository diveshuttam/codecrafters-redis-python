import socket
import threading
import time
import argparse

class RedisServer:
    def __init__(self, port=6379, role="master"):
        self.port = port
        self.role = role  # Store the role of the server
        self.redis_dict = {}

    def _handle_set(self, args):
        if len(args) == 2:
            key = args[0]
            value = args[1]
            self.redis_dict[key] = (value, None)
            return b"+OK\r\n"
        elif len(args) == 4 and args[2].decode().upper() == "PX":
            key = args[0]
            value = args[1]
            expiry = args[3]
            expiry_time = time.time()*1000 + int(expiry)
            self.redis_dict[key] = (value, expiry_time)
            return b"+OK\r\n"
        return b"-ERR wrong number of arguments for 'set' command\r\n"

    def _handle_get(self, args):
        key = args[0]
        value = self.redis_dict.get(key)
        if value:
            if value[1] and value[1] < time.time()*1000:
                del self.redis_dict[key]
                return b"$-1\r\n"
            return b"$" + bytes(str(len(value[0])), 'utf-8') + b"\r\n" + value[0] + b"\r\n"
        else:
            return b"$-1\r\n"

    def _handle_del(self, args):
        return b":0\r\n"
    
    def _handle_info(self, args):
        # Extend the INFO command to support returning the role based on the server's role
        if args and args[0].decode().lower() == "replication":
            info_response = f"role:{self.role}\r\n"
            return b"$" + bytes(str(len(info_response)), 'utf-8') + b"\r\n" + bytes(info_response, 'utf-8') + b"\r\n"
        else:
            return b"$0\r\n"
        
    def _parse_data(self, data):
        if data.startswith(b'*'):
            lines = data.split(b'\r\n')
            command = lines[2].upper()
            args = lines[4:-1]
            args = [arg for i, arg in enumerate(args) if i % 2 == 0]
            return command.decode(), args
        elif data.startswith(b'+'):
            return data[1:].decode().strip(), []
        else:
            raise ValueError("Unsupported RESP type")

    def _command_dispatcher(self):
        return {
            "SET": self._handle_set,
            "GET": self._handle_get,
            "DEL": self._handle_del,
            "ECHO": lambda args: b"$" + bytes(str(len(args[0])), 'utf-8') + b"\r\n" + args[0] + b"\r\n",
            "PING": lambda args: b"+PONG\r\n",
            "INFO": self._handle_info,  # Add the INFO command handler here
        }

    def _handle_client(self, client_socket):
        dispatcher = self._command_dispatcher()
        while True:
            data = client_socket.recv(1024)
            if not data:
                break
            command, args = self._parse_data(data)
            handler = dispatcher.get(command)
            if handler:
                response = handler(args)
                client_socket.sendall(response)
            else:
                client_socket.sendall(b"-ERR unknown command\r\n")

    def start(self):
        server_socket = socket.create_server(("localhost", self.port), reuse_port=True)
        server_socket.listen(10)
        while True:
            client_socket, addr = server_socket.accept()
            thread = threading.Thread(target=self._handle_client, args=(client_socket,))
            thread.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--replicaof", nargs='*', help="Start as a replica of the specified master")
    args = parser.parse_args()
    role = 'slave' if args.replicaof else 'master'  # Determine the role based on the --replicaof flag
    server = RedisServer(args.port, role)
    server.start()