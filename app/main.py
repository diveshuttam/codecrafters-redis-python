# Uncomment this to pass the first stage
import socket
import threading

def handle_set(args):
    # Handle SET command
    print("SET command with args:", args)
    return b"+OK\r\n"

def handle_get(args):
    # Handle GET command
    print("GET command with args:", args)
    return b"$-1\r\n"  # Example: return nil for any GET for simplicity

def handle_del(args):
    # Handle DEL command
    print("DEL command with args:", args)
    return b":0\r\n"  # Example: pretend the key does not exist

def parse_data(data):
    # Simple parser for commands based on RESP
    # This is a very basic implementation and needs to be expanded for full RESP support
    parts = data.decode().strip().split()
    command = parts[0].upper()  # Command is always uppercase
    args = parts[1:]
    return command, args

def command_dispatcher():
    # Maps command names to their handler functions
    return {
        "SET": handle_set,
        "GET": handle_get,
        "DEL": handle_del,
        "ECHO" : lambda args: b"$" + str(len(args[0])).encode() + b"\r\n" + args[0].encode() + b"\r\n",
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
        handler = dispatcher.get(command)
        if handler:
            response = handler(args)
            client_socket.sendall(response)
        else:
            client_socket.sendall(b"-ERR unknown command\r\n")

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    server_socket.listen(1)
    while True:
        client_socket, addr = server_socket.accept()
        # read packets from client_socket and send responses
        print("Connection from", addr)

        # handle client in a separate thread
        thread = threading.Thread(target=handle_client, args=(client_socket,))
        thread.start()
        # thread.join() 

if __name__ == "__main__":
    main()
