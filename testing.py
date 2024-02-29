import socket, time, asyncio

def handleMsg(keyword):
    keyword = keyword.lower()

    if keyword == "pq1c1":
        return '{"method" : "pub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go", "body": "someString"}}'

    elif keyword == "sq1c1":
        return '{ "method" : "sub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go", "body": ""}}'

    elif keyword == "pq1c2":
        return '{ "method" : "pub", "chan" : 2, "msg" : {"id" : "UUID", "routingkey" : "go", "body": ""}}'

    elif keyword == "sq1c2":
        return '{ "method" : "sub", "chan" : 2, "msg" : {"id" : "UUID", "routingkey" : "go", "body": ""}}'

    elif keyword == "pq2c1":
        return '{"method" : "pub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go2", "body": "someString"}}'

    elif keyword == "sq2c1":
        return '{ "method" : "sub", "chan" : 1, "msg" : {"id" : "UUID", "routingkey" : "go2", "body": ""}}'

    elif keyword == "pq2c2":
        return '{"method" : "pub", "chan" : 2, "msg" : {"id" : "UUID", "routingkey" : "go2", "body": "someString"}}'

    elif keyword == "sq2c2":
        return '{ "method" : "sub", "chan" : 2, "msg" : {"id" : "UUID", "routingkey" : "go2", "body": ""}}'

    elif keyword == "help":
        print("available inputs : pq1c1, sq1c1, pq1c2, sq1c2, pq2c1, sq2c1, pq2c2, sq2c2")
        print("example : pq1c1 : publish to queue 1 on channel 1 \n sq2c2 : subscribe to queue 2 on channel 2")
        return "help"

    elif keyword == "exit":
        return "exit"
    else:
        return None


async def send_tcp_message(reader, writer, message):
    print(f"Sending message: {message}")
    writer.write(message.encode())
    await writer.drain()
    print("Message sent.")

async def main():
    host = 'localhost'
    port = 8085
    reader, writer = await asyncio.open_connection(host, port)

    while True:
        msg = handleMsg(input("Input a message: ").lower())
        if msg == "exit":
            break
        if msg is None or msg == "help":
            continue
        await send_tcp_message(reader, writer, msg)
        await asyncio.sleep(1)  # Wait for 2 seconds before sending the next message

    writer.close()
    await writer.wait_closed()

asyncio.run(main())

#
# def send_tcp_message(host, port, message):
#     # Create a TCP socket
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         # Connect to the server
#         s.connect((host, port))
#         print(f"Connected to {host}:{port}")
#
#         # Send the message
#         s.sendall(message.encode())
#         print(f"Sent message: {message}")
#
#         # Receive and print data from the server continuously
#         while True:
#             data = s.recv(1024)
#             if not data:
#                 break
#             print(f"Received data: {data.decode()}")
#
# # Define the host, port, and message
# host = 'localhost'
# port = 8085
# # Call the function to send the message
#

#
# while True :
#     msg = handleMsg(input("input a message: "))
#     if msg is None:
#         continue
#     send_tcp_message(host, port, msg)
#     time.sleep(2)
#
