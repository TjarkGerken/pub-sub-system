import json
import queue
import threading
import time
from json import JSONDecodeError

from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.logger import logger


class Subscriber:
    __messageQueue = queue.Queue()

    def __init__(self, subscriber_port: int, subscriber_type: str):
        if subscriber_type not in ["U", "S", "B"]:
            raise ValueError("Sensor  type must be either 'U' or 'S' or 'B'")
        self.__subscriber_port = subscriber_port
        self.__subscriber_type = subscriber_type
        self.__subscriber_id = f"SUBSCRIBER_{subscriber_type}_{subscriber_port}"

        # Socket
        self.__subscription_socket = SendingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port + 1)
        self.__subscription_udp_socket = ReceivingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port)

        # Subscribe to the message broker on the appropriate ports
        self.initiate_subscription()

        threading.Thread(target=self.run_subscriber).start()
        threading.Thread(target=self.run_logger).start()

    def initiate_subscription(self):
        # Send subscription request to the message broker based on the type
        # Listen for the response from the message broker with a port and store it in the subscriptions
        subscriptions = []
        address = ("127.0.0.1", self.__subscriber_port)
        if self.__subscriber_type == "U" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_UV")
        if self.__subscriber_type == "S" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_TEMP")

        for subscription in subscriptions:
            self.__subscription_socket.send_message(subscription, ("127.0.0.1", 6000))

    def run_subscriber(self):
        # For all the ports in the subscriptions list, listen for the messages
        # If a message is received, put it in the messageQueue
        while True:
            # result = self.__subscription_udp_socket.listener()
            result = None
            if result is not None:
                data, addr = result
                handle_connection_thread = threading.Thread(target=self.handle_incoming_message, args=(data, addr))
                handle_connection_thread.start()

    def run_logger(self):
        sensor_value = ""
        while True:
            if self.__messageQueue.empty():
                continue

            message = self.__messageQueue.get()
            message = message.replace("'", '"')

            try:
                message = json.loads(message)
            except JSONDecodeError as e:
                logger.critical(f"[ERROR] | {self.__subscriber_id} | {e} | {message}")
                continue

            if message["sensor_type"] == "U":
                sensor_value = f"☀️  {message['uv_index']} UV in {message['location']}"
            elif message["sensor_type"] == "S":
                sensor_value = f"🌡️  {message['temperature']}  °C in {message['location']}"

            logger.info(f"[SUCCESS] | {self.__subscriber_id} | {sensor_value}")
            time.sleep(0.1)  # TODO: use Threading?

    def handle_incoming_message(self, data, addr):
        self.__messageQueue.put(data)


if __name__ == "__main__":
    Subscriber(7002, "B")
