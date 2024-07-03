# Uncomment this to pass the first stage
import socket
import threading



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

        def handle_client(client_socket):
            while True:
                data = client_socket.recv(1024)
                print("Received", data)
                if not data:
                    break
                client_socket.sendall(b"+PONG\r\n")
        
        # handle client in a separate thread
        thread  = threading.Thread(target=handle_client, args=(client_socket,)).start()
        thread.join() 

if __name__ == "__main__":
    main()
