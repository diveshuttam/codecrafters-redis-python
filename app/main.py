import socket
import threading
import time
import argparse
import random
import string
import base64



class RedisServer:
    def __init__(self, port=6379, role="master", master_host=None, master_port=None):
        self.port = port
        self.role = role  # Store the role of the server
        self.master_replid = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb' # Hardcoded master replication ID
        self.master_repl_offset = 0  # Replication offset initialized to 0
        self.replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.replication_offset = 0
        self.master_host = master_host
        self.master_port = master_port
        if role == 'slave':
            self._connect_to_master()
        self.redis_dict = {}
    
    def _connect_to_master(self):
        self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.master_socket.connect((self.master_host, int(self.master_port)))
        
        # Send PING command to master
        self.master_socket.sendall(b"*1\r\n$4\r\nPING\r\n")
        # Wait for PING response
        ping_response = self.master_socket.recv(1024)
        print(f"PING response: {ping_response}")  # For debugging
        
        # Send REPLCONF listening-port <PORT>
        replconf_listen_cmd = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(self.port))}\r\n{self.port}\r\n".encode()
        self.master_socket.sendall(replconf_listen_cmd)
        # Wait for REPLCONF listening-port response
        replconf_listen_response = self.master_socket.recv(1024)
        print(f"REPLCONF listening-port response: {replconf_listen_response}")  # For debugging
        
        # Send REPLCONF capa psync2
        replconf_capa_cmd = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        self.master_socket.sendall(replconf_capa_cmd)
        # Wait for REPLCONF capa response
        replconf_capa_response = self.master_socket.recv(1024)
        print(f"REPLCONF capa response: {replconf_capa_response}")  # For debugging
        
        # Send PSYNC ? -1
        psync_cmd = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        self.master_socket.sendall(psync_cmd)
        # For this stage, we're not handling the response to PSYNC


    def _generate_random_id(self, length=40):
        # Generates a random string of upper and lowercase letters and digits.
        characters = string.ascii_letters + string.digits
        random_id = ''.join(random.choice(characters) for _ in range(length))
        return random_id

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
        # Extend the INFO command to include master_replid and master_repl_offset
        if args and args[0].decode().lower() == "replication":
            info_response = f"role:{self.role}\r\nmaster_replid:{self.master_replid}\r\nmaster_repl_offset:{self.master_repl_offset}\r\n"
            return b"$" + bytes(str(len(info_response)), 'utf-8') + b"\r\n" + bytes(info_response, 'utf-8') + b"\r\n"
        else:
            return b"$0\r\n"

        
    def _handle_replconf(self, args):
        # For now, we ignore the arguments and simply return +OK\r\n
        return b"+OK\r\n"

    def _handle_psync(self, args):
        # Construct the FULLRESYNC response
        fullresync_response = f"+FULLRESYNC {self.replication_id} {self.replication_offset}\r\n"
        response = bytes(fullresync_response, 'utf-8')
        # Send the empty RDB file to the replica
        response += self._send_empty_rdb()
        return response

    def _send_empty_rdb(self):
        # Base64 representation of the empty RDB file
        empty_rdb_base64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
        # Decode the Base64 string to get the binary content of the RDB file
        empty_rdb_content = base64.b64decode(empty_rdb_base64)
        # Prepare the RDB file content in the required format
        rdb_file_message = f"${len(empty_rdb_content)}\r\n".encode() + empty_rdb_content
        # Send the RDB file content to the replica
        return rdb_file_message

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
            "INFO": self._handle_info,
            "REPLCONF": self._handle_replconf,
            "PSYNC": self._handle_psync,
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
    parser.add_argument("--replicaof", help="Start as a replica of the specified master. Expects 'host port'.")
    args = parser.parse_args()
    role = 'slave' if args.replicaof else 'master'  # Determine the role based on the --replicaof flag
        # Split the --replicaof argument into host and port
    if args.replicaof:
        master_host, master_port = args.replicaof.split()
    else:
        master_host, master_port = (None, None)
    server = RedisServer(args.port, role, master_host, master_port)
    server.start()