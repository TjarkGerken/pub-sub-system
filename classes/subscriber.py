import json
import os
import queue
import threading
import time
from json import JSONDecodeError
from typing import Literal

from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.logger import logger


class Subscriber:
    def __init__(self, subscriber_port: int, subscriber_type: str):
        if subscriber_type not in ["U", "S", "B"]:
            raise ValueError("Sensor  type must be either 'U' or 'S' or 'B'")

        self.__subscriber_port = subscriber_port
        self.__subscriber_type = subscriber_type
        self.__subscriber_id = f"SUBSCRIBER_{subscriber_type}_{subscriber_port}"
        self.__database_file = f"database/{self.__subscriber_id}.db"
        self.__subscriptions = []
        self.__config_file_path = f"config/{self.__subscriber_id}.json"

        # Socket
        self.__subscription_socket = SendingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port + 1)
        self.__subscription_udp_socket = ReceivingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port,
                                                                              self.__database_file)

        # Subscribe to the message broker on the appropriate ports
        self.initiate_subscription()

        threading.Thread(target=self.run_logger).start()
        threading.Thread(target=self.__subscription_udp_socket.listener).start()

        logger.info("Subscriber Started")

    def get_subscriptions_from_config(self):
        if os.path.exists(self.__config_file_path):
            with open(self.__config_file_path, "r") as config_file:
                config_data = json.load(config_file)
                return config_data.get("subscriptions", [])

    def save_subscriptions_to_config(self):
        config_data = {
            "subscriber_id": self.__subscriber_id,
            "subscriptions": self.__subscriptions
        }
        os.makedirs(os.path.dirname(self.__config_file_path), exist_ok=True)
        with open(self.__config_file_path, "w") as config_file:
            json.dump(config_data, config_file, indent=4)

    def initiate_subscription(self):
        # Send subscription request to the message broker based on the type
        # Listen for the response from the message broker with a port and store it in the subscriptions
        subs = self.get_subscriptions_from_config()
        if not subs and not isinstance(subs, list):
            subs = []
            if self.__subscriber_type == "U" or self.__subscriber_type == "B":
                subs.append(f"UV")
            if self.__subscriber_type == "S" or self.__subscriber_type == "B":
                subs.append(f"TEMP")

        for sub in subs:
            self.subscribe(sub)

    def subscribe(self, subscription_type: Literal["UV", "TEMP"]):
        if subscription_type in self.__subscriptions:
            logger.critical(f"[ERROR] | {self.__subscriber_id} | Subscription for {subscription_type} already exists")
        address = ("127.0.0.1", self.__subscriber_port)
        response = self.__subscription_socket.send_message(f"SUBSCRIBE_{subscription_type};{address}",
                                                           ("127.0.0.1", 6000))
        if subscription_type not in self.__subscriptions and response:
            self.__subscriptions.append(subscription_type)
            self.save_subscriptions_to_config()

    def unsubscribe(self, subscription_type: Literal["UV", "TEMP"]):
        if subscription_type not in self.__subscriptions:
            logger.critical(f"[ERROR] | {self.__subscriber_id} | Subscription for {subscription_type} not found")
        address = ("127.0.0.1", self.__subscriber_port)
        response = self.__subscription_socket.send_message(f"UNSUBSCRIBE_{subscription_type};{address}",
                                                           ("127.0.0.1", 6000))
        if subscription_type in self.__subscriptions and response:
            self.__subscriptions.remove(subscription_type)
            self.save_subscriptions_to_config()

    def run_logger(self):
        sensor_value = ""
        while True:
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
            #time.sleep(0.01)  # TODO: use Threading?


if __name__ == "__main__":
    Subscriber(6202, "B")
