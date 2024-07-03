# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    
    server_socket.listen(1)
    while True:
        client_socket, addr = server_socket.accept()
        print("Connection from", addr)
        client_socket.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
