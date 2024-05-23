import json
import queue
import select
import socket
import threading
import ast
import time

from configuration import RETRY_DURATION_IN_SECONDS


class MessageBroker:
    __messageQueue = queue.Queue()
    __subscribers_uv = []
    __subscribers_temp = []

    def __init__(self):
        self.__sensor_udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__subscription_udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__broadcast_udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.__sensor_udp_socket.bind(("127.0.0.1", 5004))
        self.__subscription_udp_socket.bind(("127.0.0.1", 6000))
        self.__broadcast_udp_socket.bind(("127.0.0.1", 6200))

        threading.Thread(target=self.run_broadcast).start()
        threading.Thread(target=self.run_sensor_listener).start()
        threading.Thread(target=self.run_subscription_listener).start()

    def handle_sensor_message(self, data, addr):
        # print(f"Received message | {data.decode()} from {addr}")

        confirmation_message = "Message received successfully"
        self.__sensor_udp_socket.sendto(confirmation_message.encode(), addr)
        self.__messageQueue.put(data.decode())

    def run_sensor_listener(self):
        while True:
            data, addr = self.__sensor_udp_socket.recvfrom(1024)
            handle_connection_thread = threading.Thread(target=self.handle_sensor_message, args=(data, addr))
            handle_connection_thread.start()

    def run_subscription_listener(self):

        while True:
            data, addr = self.__subscription_udp_socket.recvfrom(1024)
            handle_connection_thread = threading.Thread(target=self.handle_subscription_message, args=(data, addr))
            handle_connection_thread.start()

    def handle_subscription_message(self, data, addr):
        data = data.decode()
        if data:
            if data == "SUBSCRIBE_UV":
                self.__subscribers_uv.append(addr)
                message = "UV-Index ☀️"
            elif data == "SUBSCRIBE_TEMP":
                self.__subscribers_temp.append(addr)
                message = "Temperature 🌡️️"
            confirmation_message = "Subscribed to " + message
            self.__subscription_udp_socket.sendto(confirmation_message.encode(), addr)

    def run_broadcast(self):
        while True:
            if self.__messageQueue.empty():
                continue
            message = self.__messageQueue.get()

            message = ast.literal_eval(message)
            if message["sensor_type"] == "U":
                self.broadcast_message_to_list(self.__subscribers_uv, message)
            elif message["sensor_type"] == "S":
                self.broadcast_message_to_list(self.__subscribers_temp, message)
            # print(f"Broadcasting message: {message}")
            # https://stackoverflow.com/questions/603852/how-do-you-udp-multicast-in-python

    def broadcast_message_to_list(self, broadcast_list, message):
        for subscriber in broadcast_list:
            threading.Thread(target=self.broadcast_message, args=(message, subscriber)).start()
            # self.broadcast_message(message, subscriber)

    def broadcast_message(self, message, subscriber):
        start_time = time.time()
        while True:
            self.__broadcast_udp_socket.sendto(str(message).encode(), subscriber)
            ready = select.select([self.__broadcast_udp_socket], [], [], 5)
            if ready[0]:
                data, addr = self.__sensor_udp_socket.recvfrom(1024)
                # print(f"[SUCCESS]| MB | RESPONSE RECEIVED")
                break
            elif time.time() - start_time > RETRY_DURATION_IN_SECONDS:
                print(f"[ERROR]| MB | Response timeout")
                break
