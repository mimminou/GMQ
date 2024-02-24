import socket, time

def send_tcp_message(host, port, message):
    # Create a TCP socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Connect to the server
        s.connect((host, port))
        print(f"Connected to {host}:{port}")

        # Send the message
        s.sendall(message.encode())
        print(f"Sent message: {message}")

        # Receive and print data from the server continuously
        while True:
            data = s.recv(1024)
            if not data:
                break
            print(f"Received data: {data.decode()}")

# Define the host, port, and message
host = 'localhost'
port = 8085
# Call the function to send the message


def handleMsg(keyword):
    keyword = keyword.lower()
    queue = keyword

    if keyword == "pub1":
        return '{"method" : "pub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go", "body": "someString"}}'

    elif keyword == "sub1":
        return '{ "method" : "sub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go", "body": ""}}'


    elif keyword == "pub2":
        return '{"method" : "pub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go2", "body": "someString"}}'

    elif keyword == "sub2":
        return '{ "method" : "sub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go2", "body": ""}}'

    else:
        return None


while True :
    msg = handleMsg(input("input a message: "))
    send_tcp_message(host, port, msg)
    time.sleep(2)

