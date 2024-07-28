import ast
import queue
import threading
import time

from classes.CommunicationProtocol.ReceivingCommunicationProtocolSocket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.SendingCommunicationProtocolSocket import SendingCommunicationProtocolSocket


class MessageBroker:
    __messageQueue = queue.Queue()
    __subscribers_uv = []
    __subscribers_temp = []

    def __init__(self):
        self.__sensor_udp_socket = ReceivingCommunicationProtocolSocket("MB_SENSOR", 5004)
        self.__subscription_udp_socket = ReceivingCommunicationProtocolSocket("MB_SUBSCRIPTION", 6000)
        self.__broadcast_udp_socket = SendingCommunicationProtocolSocket("MB_BROADCAST", 6200)

        threading.Thread(target=self.run_sensor_listener).start()
        threading.Thread(target=self.run_subscription_listener).start()
        # threading.Thread(target=self.run_broadcast).start()

    def run_sensor_listener(self):
        while True:
            result = self.__sensor_udp_socket.listener()
            if result:
                self.__messageQueue.put(result)

    def run_subscription_listener(self):
        while True:
            while not self.__sensor_udp_socket.message_queue.empty():
                data = self.__sensor_udp_socket.message_queue.get()
                # print(data)
            """
            while True:
                result = self.__subscription_udp_socket.listen()
                if result is not None:
                    data, addr = result
                    threading.Thread(target=self.handle_subscription_message, args=(data, addr)).start()
            """
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
            print(confirmation_message)

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
