import socket
import threading
import time
import argparse
import random
import string
import base64


class RedisServer:
    def __init__(self, port=6379, role="master", master_host=None, master_port=None, config={}):
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
        self.pending_count = 0
        self.config = config

        # read the rdb file
        self._rdb_parsing()

        if role == 'slave':
            self._connect_to_master()
    
    def _rdb_parsing(self):
        # Read the RDB file
        with open(f"{self.config['dir']}/{self.config['dbfilename']}", "rb") as file:
            rdb_content = file.read()
        
        # Parse the RDB file
        self._parse_rdb(rdb_content)
    
    def _parse_rdb(self, rdb_content):
        # Check if the RDB file is empty
        if rdb_content == b"":
            return
        
        # Check if the RDB file is a valid Redis RDB file
        if rdb_content[:5] != b"REDIS":
            raise ValueError("Invalid RDB file")
        
        # Parse the RDB file
        # Get the Redis version
        redis_version = rdb_content[5:12].decode('utf-8')
        print(f"Redis version: {redis_version}")
        
        # Get the Redis bits
        redis_bits = rdb_content[12:22].decode('ascii')
        print(f"Redis bits: {redis_bits}")
        
        # Get the ctime
        ctime = rdb_content[22:30].decode('ascii')
        print(f"ctime: {ctime}")
        
        # Get the used memory
        used_memory = rdb_content[30:38]
        print(f"used memory: {used_memory}")
        
        # Get the AOF base
        aof_base = rdb_content[38:46]
        print(f"AOF base: {aof_base}")
        
        # Get the data section
        data = rdb_content[46:]
        print(f"Data section: {data}")
        
        # Parse the data section
        self._parse_data_section(data)
    
    def _parse_data_section(self, data):
        # Initialize the variables
        key = None
        value = None
        expiry = None
        
        # Parse the data section
        while data:
            # Get the data type
            data_type = data[0]
            data = data[1:]
            
            # Parse the key
            key, data = self._parse_key(data)
            print(f"Key: {key}")
            
            # Parse the expiry
            expiry, data = self._parse_expiry(data)
            print(f"Expiry: {expiry}")
            
            # Parse the value
            value, data = self._parse_value(data)
            print(f"Value: {value}")
            
            # Store the key, value, and expiry in the dictionary
            self.redis_dict[key] = (value, expiry)
    
    def _parse_key(self, data):
        # Get the key length
        key_length = int(data[0])
        data = data[1:]
        
        # Get the key
        key = data[:key_length]
        data = data[key_length:]
        
        return key, data
    
    def _parse_expiry(self, data):
        # Get the expiry length
        expiry_length = int(data[0])
        data = data[1:]
        
        # Get the expiry
        expiry = data[:expiry_length]
        data = data[expiry_length:]
        
        return expiry, data
    
    def _parse_value(self, data):
        # Get the value length
        value_length = int(data[0])
        data = data[1:]
        
        # Get the value
        value = data[:value_length]
        data = data[value_length:]
        
        return value, data

         
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
        
        elif config_param == "ack":
            self.count += 1
            print("ack received increasing count", self.count)
            return b""
        
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

        # testing : for now sending to all slaves
        num = len(self.slave_connections) 
        count = 0
        # send the getack command to all the slaves
        self.count = 0
        print("in wait, num, slaves", num, len(self.slave_connections))
        if(self.role == "master"):
            if(self.pending_count == 0):
                return b":" + bytes(str(len(self.slave_connections)), 'utf-8') + b"\r\n"
            
            for slave in range(min(num, len(self.slave_connections))):
                # send "REPLCONF GETACK *"
                # non blocking
                self.slave_connections[slave].setblocking(0)
                self.slave_connections[slave].sendall(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
            
            # wait for the response from all the slaves
            doneslaves = set()
            while (time.time()*1000) < exptime and count < num:
                for slave in range(min(num, len(self.slave_connections))):
                    # self.slave_connections[slave].setblocking(1)
                    try:
                        if slave in doneslaves:
                            continue
                        print("waiting for slave", slave, self.slave_connections[slave].getpeername())
                        response = self.slave_connections[slave].recv(1024)
                        print("response from slave", response)
                        
                        if response:
                            doneslaves = doneslaves.union({slave})
                            self.pending_count -= 1
                            self.count += 1
                    except BaseException as e:
                        print("exception", e)
                        pass

                    sleep = (exptime-time.time()*1000)/1000.0/num/2.0
                    if(sleep > 0):
                        print("sleeping for", sleep)
                        time.sleep(sleep)

               
            # return count as an integer
            # :7\r\n
            print("wait response", self.count)
            return b":" + bytes(str(self.count), 'utf-8') + b"\r\n"
        
    def _handle_config(self, args):
        print("config args", args)
        if args[0].decode().lower() == "get":
            # get the args[1] and return the value
            config_param = args[1].decode().lower()
            # the response has to be reurned as an array and not bulk stirng *2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n
            if config_param == "dir":
                response = f"*2\r\n$3\r\ndir\r\n${len(self.config['dir'])}\r\n{self.config['dir']}\r\n"
                return bytes(response, 'utf-8')
            elif config_param == "dbfilename":
                response = f"*2\r\n$9\r\ndbfilename\r\n${len(self.config['dbfilename'])}\r\n{self.config['dbfilename']}\r\n"
                return bytes(response, 'utf-8')
            else:
                return b"-ERR unsupported CONFIG parameter\r\n"

        return b"-ERR unsupported CONFIG parameter\r\n"
    
    def _handle_keys(self, args):
        # return all the keys in the redis_dict in form of an array
        # redis-cli KEYS "*"
        # 1) "baz"
        # 2) "foo"
        
        response = "*"+str(len(self.redis_dict)) + "\r\n"
        for key in self.redis_dict.keys():
            response += f"${len(key)}\r\n{key}\r\n"
        return bytes(response, 'utf-8')


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
            "WAIT": self._handle_wait,
            "CONFIG": self._handle_config,
            "KEYS": self._handle_keys
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
                    for slave in self.slave_connections:
                        if(command == "SET"):
                            print("replicating to slave", slave.getpeername())
                            self.pending_count += 1
                            slave.sendall(data)
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
    parser.add_argument("--dir", type=str, default="/tmp/redis-files")
    parser.add_argument("--dbfilename", type=str, default="dump.rdb")
    
    args = parser.parse_args()
    config = args.__dict__
    print('config', config)

    role = 'slave' if args.replicaof else 'master'  # Determine the role based on the --replicaof flag
        # Split the --replicaof argument into host and port
    if args.replicaof:
        master_host, master_port = args.replicaof.split()
    else:
        master_host, master_port = (None, None)
    server = RedisServer(args.port, role, master_host, master_port, config)
    print(f"Starting server on port {args.port} as {role}")
    server.start()