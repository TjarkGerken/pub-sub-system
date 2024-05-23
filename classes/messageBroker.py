import queue
import socket
import threading


class MessageBroker:
    __messageQueue = queue.Queue()
    __udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    __subscribers_uv = []
    __subscribers_temp = []

    def __init__(self):
        print("MessageBroker initialized")
        threading.Thread(target=self.run_listener).start()
        threading.Thread(target=self.run_broadcast).start()

    def handle_client(self, data, addr):
        # print(f"Received message | {data.decode()} from {addr}")

        confirmation_message = "Message received successfully"
        self.__udpSocket.sendto(confirmation_message.encode(), addr)
        self.__messageQueue.put(data.decode())

    def run_listener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind(("127.0.0.1", 5004))

        while True:
            data, addr = server.recvfrom(1024)
            client_thread = threading.Thread(target=self.handle_client, args=(data, addr))
            client_thread.start()

    def run_broadcast(self):
        while True:
            if self.__messageQueue.empty():
                continue
            message = self.__messageQueue.get()
            # print(f"Broadcasting message: {message}")
