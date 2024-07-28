import ast
import queue
import threading
import time

from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.logger import logger


class MessageBroker:
    __messageQueue = queue.Queue()
    __subscribers_uv = []
    __subscribers_temp = []

    def __init__(self):
        self.__sensor_udp_socket = ReceivingCommunicationProtocolSocket("MB_SENSOR", 5004)
        self.__subscription_socket = ReceivingCommunicationProtocolSocket("MB_SUBSCRIPTION", 6000)
        self.__broadcast_udp_socket = SendingCommunicationProtocolSocket("MB_BROADCAST", 6200)

        threading.Thread(target=self.run_sensor_listener).start()
        threading.Thread(target=self.run_subscription_listener).start()
        threading.Thread(target=self.run_subscription_handler).start()
        # threading.Thread(target=self.run_broadcast).start()

    def run_sensor_listener(self):
        self.__sensor_udp_socket.listener()

    def run_subscription_listener(self):
        self.__subscription_socket.listener()

    def run_subscription_handler(self):
        while True:
            if self.__subscription_socket.message_queue.empty():
                continue
            result = self.__subscription_socket.message_queue.get()
            threading.Thread(target=self.handle_subscription_message, args=(result,)).start()

    def handle_subscription_message(self, data):
        message = None
        if data:
            if data == "SUBSCRIBE_UV":
                self.__subscribers_uv.append(addr)
                message = "UV-Index ☀️"
            elif data == "SUBSCRIBE_TEMP":
                self.__subscribers_temp.append(addr)
                message = "Temperature 🌡️️"
            confirmation_message = "[MB_SUBSCRIPTION] | Successfully Subscribed to " + message
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

    def broadcast_message(self, message, subscriber):
        start_time = time.time()
        while True:
            self.__broadcast_udp_socket.send_message(str(message), subscriber)

    def broadcast_message_to_list(self, broadcast_list, message):
        for subscriber in broadcast_list:
            threading.Thread(target=self.broadcast_message, args=(message, subscriber)).start()


if __name__ == "__main__":
    MessageBroker()
