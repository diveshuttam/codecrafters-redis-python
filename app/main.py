# Uncomment this to pass the first stage
import socket
import threading
import time

global redis_dict
redis_dict = {}

def handle_set(args):
    # Handle SET command
    # Example: SET key value
    # args[0] is the key, args[1] is the value
    print(len(args))
    if len(args) == 2:
        key = args[0]
        value = args[1]
        redis_dict[key] = (value, None)
        print("SET command with args:", args)
        return b"+OK\r\n"
    
    # handle expiry
    elif len(args) == 4:
        print('here on')
        if args[2].decode().upper() == "PX":
            print("here")
            key = args[0]
            value = args[1]
            expiry = args[3]
            # assume expiry is in milliseconds

            expiry_time = time.time()*1000 + int(expiry)
            redis_dict[key] = (value, expiry_time)
            print("SET command with args:", args)
            return b"+OK\r\n"
    
    return b"-ERR wrong number of arguments for 'set' command\r\n"
    
def handle_get(args):
    # Handle GET command
    print("GET command with args:", args)
    key = args[0]
    value = redis_dict.get(key)
    # discard expired keys
    if value:
        if value[1] and value[1] < time.time()*1000:
            del redis_dict[key]
            return b"$-1\r\n"
        return b"$" + bytes(str(len(value[0])), 'utf-8') + b"\r\n" + value[0] + b"\r\n"
    else:
        return b"$-1\r\n"  # Example: pretend the key does not exist

def handle_del(args):
    # Handle DEL command
    print("DEL command with args:", args)
    return b":0\r\n"  # Example: pretend the key does not exist

def parse_data(data):
    """Parse a simple RESP string."""
    if data.startswith(b'*'):
        # Array type, split lines and parse each
        lines = data.split(b'\r\n')
        # print(lines)
        command = lines[2].upper()  # Assuming the command is the first bulk string
        # print(command)
        args = lines[4:-1]  # Assuming the rest are arguments (excluding the last empty line)
        # for cases like [b'foo', b'$3', b'bar'] remove the b'$3' etc
        args = [arg for i, arg in enumerate(args) if i % 2 == 0]
        return command.decode(), args
    elif data.startswith(b'+'):
        # Simple string
        return data[1:].decode().strip(), []
    # Add more cases here for other RESP types like Errors (-), Integers (:), Bulk Strings ($)
    else:
        raise ValueError("Unsupported RESP type")

def command_dispatcher():
    # Maps command names to their handler functions
    return {
        "SET": handle_set,
        "GET": handle_get,
        "DEL": handle_del,
        "ECHO" : lambda args: b"$" + bytes(str(len(args[0])), 'utf-8') + b"\r\n" + args[0] + b"\r\n",
        "PING": lambda args: b"+PONG\r\n",
    }

def handle_client(client_socket):
    dispatcher = command_dispatcher()
    while True:
        data = client_socket.recv(1024)
        print("Received", data)
        if not data:
            break
        command, args = parse_data(data)
        print(command, args)        
        handler = dispatcher.get(command)
        if handler:
            response = handler(args)
            client_socket.sendall(response)
        else:
            client_socket.sendall(b"-ERR unknown command\r\n")

def main(port=6379):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", port), reuse_port=True)
    
    server_socket.listen(10)
    while True:
        client_socket, addr = server_socket.accept()
        # read packets from client_socket and send responses
        print("Connection from", addr)

        # handle client in a separate thread
        thread = threading.Thread(target=handle_client, args=(client_socket,))
        thread.start()
        # thread.join() 

if __name__ == "__main__":
    # parse arguments
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379)
    args = parser.parse_args()
    main(args.port)
