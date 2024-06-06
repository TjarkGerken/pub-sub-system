import queue
import select
import socket
import threading
import ast
import time

from classes.udpsocket import UdpSocket
from configuration import RETRY_DURATION_IN_SECONDS
from utils.logger import logger

class MessageBroker:
    __messageQueue = queue.Queue()
    __subscribers_uv = []
    __subscribers_temp = []

    def __init__(self):
        self.__sensor_udp_socket = UdpSocket(5004, "MB_SENSOR")
        self.__subscription_udp_socket = UdpSocket(6000, "MB_SUBSCRIPTION")
        self.__broadcast_udp_socket = UdpSocket(6200, "MB_BROADCAST")

        threading.Thread(target=self.run_sensor_listener).start()
        threading.Thread(target=self.run_subscription_listener).start()
        threading.Thread(target=self.run_broadcast).start()

    def handle_sensor_message(self, data, addr):
        self.__messageQueue.put(data)

    def run_sensor_listener(self):
        while True:
            result = self.__sensor_udp_socket.listen()
            if result is not None:
                data, addr = result
                handle_connection_thread = threading.Thread(target=self.handle_sensor_message, args=(data, addr))
                handle_connection_thread.start()

    def run_subscription_listener(self):
        while True:
            result = self.__subscription_udp_socket.listen()
            if result is not None:
                data, addr = result
                threading.Thread(target=self.handle_subscription_message, args=(data, addr)).start()

    def handle_subscription_message(self, data, addr):
        message = None
        if data:
            if data == "SUBSCRIBE_UV":
                self.__subscribers_uv.append(addr)
                message = "UV-Index ☀️"
            elif data == "SUBSCRIBE_TEMP":
                self.__subscribers_temp.append(addr)
                message = "Temperature 🌡️️"
            confirmation_message = "Successfully Subscribed to " + message
            logger.info(confirmation_message)

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

    def broadcast_message_to_list(self, broadcast_list, message):
        for subscriber in broadcast_list:
            threading.Thread(target=self.broadcast_message, args=(message, subscriber)).start()

    def broadcast_message(self, message, subscriber):
        start_time = time.time()
        while True:
            self.__broadcast_udp_socket.three_way_send(str(message), subscriber)
