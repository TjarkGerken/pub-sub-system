import json
import queue
import signal
import sys
import threading
import time
from json import JSONDecodeError

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.StoppableThread import StoppableThread
from utils.logger import logger


class Subscriber:
    def __init__(self, subscriber_port: int, subscriber_type: str):
        if subscriber_type not in ["U", "S", "B"]:
            raise ValueError("Sensor  type must be either 'U' or 'S' or 'B'")

        # Subscriber info
        self.__subscriber_port = subscriber_port
        self.__subscriber_type = subscriber_type
        self.__subscriber_id = f"SUBSCRIBER_{subscriber_type}_{subscriber_port}"
        self.__database_file = f"database/{self.__subscriber_id}.db"
        self.__actions = []

        # Socket
        self.__subscription_socket = SendingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port + 1)
        self.__subscription_udp_socket = ReceivingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port,
                                                                              self.__database_file)

        try:
            # Subscribe to the message broker on the appropriate ports
            self.initiate_subscription()
        except KeyboardInterrupt:
            # Because subscriber is not initialized yet, we need to stop it manually
            self.stop()
            sys.exit(0)

        self.__logger_thread = StoppableThread(target=self.run_logger)
        self.__subscription_listener_thread = StoppableThread(target=self.__subscription_udp_socket.listener)

        self.__logger_thread.start()
        self.__subscription_listener_thread.start()

        # Store all threads and sockets in a list to stop them gracefully later
        self.__actions.append(self.__subscription_udp_socket)
        self.__actions.append(self.__subscription_socket)
        self.__actions.append(self.__logger_thread)
        self.__actions.append(self.__subscription_listener_thread)

        logger.info("Subscriber Started")

    def initiate_subscription(self):
        # Send subscription request to the message broker based on the type
        # Listen for the response from the message broker with a port and store it in the subscriptions
        subscriptions = []
        address = ("127.0.0.1", self.__subscriber_port)
        if self.__subscriber_type == "U" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_UV;{address}")
        if self.__subscriber_type == "S" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_TEMP;{address}")

        for subscription in subscriptions:
            self.__subscription_socket.send_message(subscription, ("127.0.0.1", 6000))

    def run_logger(self):
        sensor_value = ""
        while not self.__logger_thread.stopped():
            if self.__subscription_udp_socket.message_queue.empty():
                continue

            message = self.__subscription_udp_socket.message_queue.get()
            try:
                message = message.replace("'", '"')
                message = json.loads(message)
            except JSONDecodeError as e:
                logger.critical(f"[ERROR] | {self.__subscriber_id} | {e} | {message}")
                continue

            if message["sensor_type"] == "U":
                sensor_value = f"{message['uv_index']} UV in {message['location']}"
            elif message["sensor_type"] == "S":
                sensor_value = f"{message['temperature']} °C in {message['location']}"

            logger.info(f"[{self.__subscriber_id}]\tSuccessfully received message: {sensor_value}")
            self.__subscription_udp_socket.delete_message_from_db(message)
            time.sleep(0.1)  # TODO: use Threading?

    def stop(self):
        """
        Stops all running tasks of the sensor gracefully to shut the message broker down

        :return: None
        """
        logger.info(f"Shutting down {self.__subscriber_id}")

        counter = 0
        for action in self.__actions:
            counter += 1
            if isinstance(action, StoppableThread):
                logger.info(f"Stopping thread ({counter}/{len(self.__actions)}) (Thread Name: {action.name})")
                action.stop()
                action.join()
            elif isinstance(action, CommunicationProtocolSocketBase):
                logger.info(f"Stopping thread ({counter}/{len(self.__actions)}) (Socket Name: {action.uid})")
                action.stop()

        return None


def handle_signal(sig, frame):
    subscriber.stop()
    sys.exit(0)


if __name__ == "__main__":
    subscriber = Subscriber(6202, "B")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while True:
        time.sleep(0.1)
