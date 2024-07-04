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
        self.role = role
        self.master_replid = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
        self.master_repl_offset = 0
        self.replication_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.replication_offset = 0
        self.master_host = master_host
        self.master_port = master_port
        self.slave_addresses = []  # List to store slave connections
        self.slave_connections = []  # List to store slave connections
        self.redis_dict = {}
        self.bytes_read = 0
        self.handshake_success = False
        if role == 'slave':
            self._connect_to_master()
         
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
        # Wait for PSYNC response
        # psync_response = self.master_socket.recv(1024)
        # print(f"PSYNC response: {psync_response}")
        # Parse the PSYNC response
        # response_lines = psync_response.split(b'\r\n')
        # print("response", response_lines)

        # start processing the commands from master in a separate thread
        master_thread = threading.Thread(target=self._handle_master, args=(self.master_socket,))
        master_thread.start()
        
    def _handle_master(self, master_socket):
        rest = b""
        while True:
            if rest != b"":
                print("rest: ", rest)
                data = rest
                rest = b""
            else:
                data = master_socket.recv(1024)
                if not data:
                    break

            rdbdata = b'$88\r\nREDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2'

            # if data starts with rdbdata, then skip it
            if data == rdbdata:
                print("skipping rdbdata")
                self.handshake_success = True
                continue

            if data.startswith(rdbdata):
                print("skipping rdbdata")
                rest = data[len(rdbdata):] 
                data = b''
                self.handshake_success = True
                continue

            print("master thread data", data)
            if not data:
                continue


            command, args, rest = self._parse_data(data)
            
            if(command is None):
                continue
            print("Command for slave: ", command)
            handler = self._command_dispatcher().get(command)
            if handler:
                if "client_socket" in handler.__code__.co_varnames:
                    response = handler(args, master_socket)
                else:
                    response = handler(args)
                
                # if response is a tuple, it means we need to send the response to the master
                print("obj", response)
                if isinstance(response, tuple):
                    print("yes here")
                    response, _ = response
                    master_socket.sendall(response)
                if(self.handshake_success == True):
                    self.bytes_read += (len(data) - len(rest))
                
            else:
                print("unknown command, data", data)
                master_socket.sendall(b"-ERR unknown command\r\n")


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

        
    def _handle_replconf(self, args, client_socket):
        # Parse the arguments to get the configuration parameter and its value
        print("replconf args", args)
        config_param = args[0].decode().lower()
        config_value = args[1].decode().lower()
        print("config_param", config_param)
        print("config_value", config_value)
        # Check if the configuration parameter is "listening-port"
        if config_param == "listening-port":
            # Update the port number to the new value
            port = int(config_value)
            # save this in the saves connections
            self.slave_addresses.append((client_socket.getpeername()[0], port))
            # Send a success response
            return b"+OK\r\n"
        elif config_param == "capa":
            return b"+OK\r\n"

        # handle GETACK, reply with REPLCONF ACK 0 (encoded correctly)
        elif config_param == "getack":
            # get the number of bytes form self.bytes_read
            bytes_read = self.bytes_read
            response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(str(bytes_read))}\r\n{bytes_read}\r\n"
            return bytes(response, 'utf-8'), None
        
        # Send an error response for unsupported configuration parameters
        print("Unsupported CONFIG parameter", config_param, config_value)
        return b"-ERR Unsupported CONFIG parameter\r\n"

    def _connect_to_slaves(self):
        # Get a slave connection ready set the save connected setting as true
        slave_address = self.slave_addresses.pop(0)
        slave_host, slave_port = slave_address
        print("address", slave_address)
        # Connect to the slave and keep it handy
        slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        slave_socket.connect((slave_host, slave_port))
        self.slave_connections.append(slave_socket)


    def _handle_psync(self, args, client_socket):
        # Construct the FULLRESYNC response
        fullresync_response = f"+FULLRESYNC {self.replication_id} {self.replication_offset}\r\n"
        response = bytes(fullresync_response, 'utf-8')
        # Send the empty RDB file to the replica
        response += self._send_empty_rdb()
        
        

        # Connect to the slave
        # self._connect_to_slaves()

        # for now just putting the same socket (client_socket) in the list
        self.slave_connections.append(client_socket)

        return response

    def _handle_fullresync(self, args):
        # Parse the replication ID and offset from the arguments
        self.replication_id = args[0].decode()
        self.replication_offset = int(args[1].decode())
        # Send the empty RDB file to the replica
        return self._send_empty_rdb()

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
        print("inside parse data", data)
        if data.startswith(b'*'):
            lines = data.split(b'\r\n')
            total_terms = int(lines[0][1:])
            
            print("lines", lines)
            print("total terms", total_terms)
            command = lines[2].upper()
            args = lines[2:total_terms*2+1:2][1:]
            rest = lines[total_terms*2+1:]
            print("args: ", args)
            print("rest: ", rest)

            # join rest into a binary single string
            return command.decode(), args, b"\r\n".join(rest) if len(rest)>0 else b""
        elif data.startswith(b'+'):
            # split data till \r\n
            print("not a command, just a response", data.split(b'\r\n')[0])
            return None, [], data[len(data.split(b'\r\n')[0]) + 2:]
        else:
            print("data: ", data)
            raise ValueError("Unsupported RESP type")
    
    def _handle_wait(self, args):
        # send a getack command to all the number of slaves in arguments (if I am master)
        num = int(args[0].decode())
        tms = int(args[1].decode())

        ctime = time.time()*1000
        exptime = ctime + tms

        
        if(self.role == "master"):
            for slave in range(min(num, len(self.slave_connections))):
                # send "REPLCONF GETACK *"
                self.slave_connections[slave].sendall(b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n")
                # check the response
                response = self.slave_connections[slave].recv(1024)
        

        
        
        return b":" + bytes(str(len(self.slave_connections)), 'utf-8') + b"\r\n"

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
            "FULLRESYNC": self._handle_fullresync,
            # for wait return back the number of replicas (len slave_connections)
            "WAIT": self._handle_wait
        }

    def _handle_client(self, client_socket):
        dispatcher = self._command_dispatcher()
        rest = b""
        while True:
            if rest != b"":
                print("rest: ", rest)
                data = rest
                rest = b""
            else:
                data = client_socket.recv(1024)

            if not data:
                break
            command, args, rest = self._parse_data(data)
            if command is None:
                continue
            handler = dispatcher.get(command)
            print("Command: ", command, "role: ", self.role)
            if handler:
                ## if handler has an argument named "client_socket", pass it
                if "client_socket" in handler.__code__.co_varnames:
                    response = handler(args, client_socket)
                else:    
                    response = handler(args)

                client_socket.sendall(response)
                    
                # replicate appropriate commands to the slave
                if(role == "master"):
                    print("replicating to slave")
                    for slave in self.slave_connections:
                        if(command == "SET"):
                            slave.sendall(data)
            else:
                client_socket.sendall(b"-ERR unknown command\r\n")

    def replicate_to_slave(self, data, command, slave):
        slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        slave_socket.connect((slave[0], slave[1]))

        if command == "SET":
            print("replicating SET command")
            slave_socket.sendall(data)
            print("sent to slave")
        slave_socket.close()

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
    print(f"Starting server on port {args.port} as {role}")
    server.start()