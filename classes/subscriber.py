"""
Subscriber class for communicating with the message broker
"""

import json
import os
import signal
import sys
import time
from json import JSONDecodeError
from typing import Literal, Union

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.StoppableThread import StoppableThread
from utils.logger import logger


class Subscriber:
    """
    A class to represent a subscriber that receives its data from a message broker by subscribing to different topics.

    Attributes
    ----------
    __subscriber_port : int
        The port number for the subscriber
    __subscriber_type : str
        The type of the subscriber ('U', 'S', or 'B')
    __subscriber_id : str
        A unique identifier for the subscriber
    __subscriptions : list
        A list of topics subscribed to
    _database_file : str
        The path to the database file
    __config_file_path : str
        The path to the configuration file for the subscriber
    __actions : list
        The list of actions (threads and sockets) to be stopped gracefully
    __subscription_socket: SendingCommunicationProtocolSocket
        The socket used for sending subscription messages
    _subscription_udp_socket: ReceivingCommunicationProtocolSocket
        The socket used for receiving subscription messages
    __logger_thread : StoppableThread
        The thread for logging messages
    __subscription_listener_thread : StoppableThread
        The thread for listening to subscription messages
    """

    def __init__(self, subscriber_port: int, subscriber_type: str, log: bool = True, ignore_startup: bool = False) \
            -> None:
        """
        Initializes the subscriber with the given port and type (topic to subscribe to)

        :param subscriber_port: The port number for the subscriber
        :param subscriber_type: The topic to subscribe to ('U', 'S', or 'B')
        :param log: A flag to enable or disable logging
        :param ignore_startup: A flag to ignore the startup configuration
        """
        if subscriber_type not in ["U", "S", "B"]:
            raise ValueError("Sensor  type must be either 'U' or 'S' or 'B'")

        self.log = log
        self.ignore_startup = ignore_startup

        # Subscriber info
        self.__subscriber_port = subscriber_port
        self.__subscriber_type = subscriber_type
        self.__subscriber_id = f"SUBSCRIBER_{subscriber_type}_{subscriber_port}"
        self.__subscriptions = []
        self._database_file = f"database/{self.__subscriber_id}.db"
        self.__config_file_path = f"config/{self.__subscriber_id}.json"
        self.__actions = []

        # Socket
        self.__subscription_socket = SendingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port + 1)
        self._subscription_udp_socket = ReceivingCommunicationProtocolSocket(self.__subscriber_id, subscriber_port,
                                                                             self._database_file)

        try:
            # Subscribe to the message broker on the appropriate ports
            self.initiate_subscription()
        except KeyboardInterrupt:
            # Because subscriber is not initialized yet, we need to stop it manually
            self.stop()
            sys.exit(0)

        self.__logger_thread = StoppableThread(target=self.run_logger)
        self.__subscription_listener_thread = StoppableThread(target=self._subscription_udp_socket.listener)

        self.__logger_thread.start()
        self.__subscription_listener_thread.start()

        # Store all threads and sockets in a list to stop them gracefully later
        self.__actions.append(self._subscription_udp_socket)
        self.__actions.append(self.__subscription_socket)
        self.__actions.append(self.__logger_thread)
        self.__actions.append(self.__subscription_listener_thread)

        logger.info("Subscriber Started")

    def get_subscriptions_from_config(self) -> Union[list, None]:
        """
        Retrieves the current subscriptions from the configuration file.
        :return: A list of subscriptions (topics) if the configuration file exists, otherwise None.
        """
        if self.ignore_startup:
            return None
        # Check if config file exists
        if os.path.exists(self.__config_file_path):
            with open(self.__config_file_path, "r") as config_file:
                # Retrieve values for config fields set in config file
                config_data = json.load(config_file)
                return config_data.get("subscriptions", [])

        return None

    def save_subscriptions_to_config(self) -> None:
        """
        Saves the current configuration of the subscriber to a configuration file to restore from
        :return: None
        """
        config_data = {"subscriber_id": self.__subscriber_id, "subscriptions": self.__subscriptions}

        # Ensure that the directory in which the configuration will be saved exists
        os.makedirs(os.path.dirname(self.__config_file_path), exist_ok=True)

        # Save config as json
        with open(self.__config_file_path, "w") as config_file:
            json.dump(config_data, config_file, indent=4)

        return None

    def initiate_subscription(self) -> None:
        """
        Initiate the subscription process by sending subscription requests to the message broker.
        :return: None
        """
        # Retrieve topics the subscriber wants to subscribe to
        subs = self.get_subscriptions_from_config()

        # If no subscriptions were found in configuration, subscribe to default
        if not subs and not isinstance(subs, list):
            subs = []
            if self.__subscriber_type == "U" or self.__subscriber_type == "B":
                subs.append(f"UV")
            if self.__subscriber_type == "S" or self.__subscriber_type == "B":
                subs.append(f"TEMP")

        # Send subscription request to message broker for each topic the subscriber wants to subscribe to
        for sub in subs:
            self.subscribe(sub)

        return None

    def subscribe(self, subscriber_type: Literal["UV", "TEMP"]) -> None:
        """
        Send a subscription request to the message broker.

        :param subscriber_type: The topic the subscriber wants to subscribe to ('UV' or 'TEMP')

        :return: None
        """
        # Check if subscriber already subscribed to topic
        if subscriber_type in self.__subscriptions:
            logger.critical(f"[ERROR] | {self.__subscriber_id} | Subscription for {subscriber_type} already exists")
            return None

        # Send subscription request to message broker
        address = ("127.0.0.1", self.__subscriber_port)
        response = self.__subscription_socket.send_message(f"SUBSCRIBE_{subscriber_type};{address}",
                                                           ("127.0.0.1", 6000))

        # If subscription was successful, add topic to list of subscriptions and config
        if subscriber_type not in self.__subscriptions and response:
            self.__subscriptions.append(subscriber_type)
            self.save_subscriptions_to_config()

        return None

    def unsubscribe(self, subscriber_type: Literal["UV", "TEMP"]) -> None:
        """
        Send an unsubscription request to the message broker.

        :param subscriber_type: The topic the subscriber wants to unsubscribe from ('UV' or 'TEMP')

        :return: None
        """
        # Check if subscriber is subscribed to given topic at all
        if subscriber_type not in self.__subscriptions:
            logger.critical(f"[ERROR] | {self.__subscriber_id} | Subscription for {subscriber_type} not found")
            return None

        # Send unsubscription request to message broker
        address = ("127.0.0.1", self.__subscriber_port)
        response = self.__subscription_socket.send_message(f"UNSUBSCRIBE_{subscriber_type};{address}",
                                                           ("127.0.0.1", 6000))

        # If unsubscription was successful, remove topic from list of subscriptions and config
        if subscriber_type in self.__subscriptions and response:
            self.__subscriptions.remove(subscriber_type)
            self.save_subscriptions_to_config()

        return None

    def run_logger(self) -> None:
        """
        Logs messages received by the subscriber
        :return: None
        """
        sensor_value = ""
        # Run until the logger thread is stopped
        while not self.__logger_thread.stopped() and self.log:
            # If there are no messages in the queue, continue
            if self._subscription_udp_socket.message_queue.empty():
                continue

            message = self._subscription_udp_socket.message_queue.get()
            try:
                # Convert message to JSON
                message = message.replace("'", '"')
                message = json.loads(message)
            except JSONDecodeError as e:
                logger.error(f"[{self.__subscriber_id}]\tCannot decode message")
                logger.debug(f"[{self.__subscriber_id}]\tCannot decode message: {e} (Message: {message})")
                continue

            # Format output message based on topic subscribed to
            if message["sensor_type"] == "U":
                sensor_value = f"{message['uv_index']} UV in {message['location']}"
            elif message["sensor_type"] == "S":
                sensor_value = f"{message['temperature']} °C in {message['location']}"

            # Persist received messages to log file (tab separated values)
            with open(f"logs/{self.__subscriber_id}_messages.tsv", "a") as log_file:
                values = [str(value) for _, value in message.items()]
                log_file.write("\t".join(values) + "\n")

            # Delete message from database after processing, so it is not processed again
            logger.info(f"[{self.__subscriber_id}]\tSuccessfully received message: {sensor_value}")
            self._subscription_udp_socket.delete_message_from_db(message)
            self._subscription_udp_socket.message_queue.task_done()

        return None

    def stop(self) -> None:
        """
        Stops all running tasks of the sensor gracefully to shut the message broker down
        :return: None
        """
        logger.info(f"Shutting down {self.__subscriber_id}...")

        counter = 0

        # Iterate over all components that need to be stopped and call their own stop methods
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


def handle_signal(sig, frame) -> None:
    """
    Handle signals to stop the subscriber gracefully

    :param sig: Signal number
    :param frame: Current stack frame

    :return: None
    """
    subscriber.stop()
    sys.exit(0)


if __name__ == "__main__":
    # Create a subscriber
    subscriber = Subscriber(6202, "B")

    # Set up handler for stopping the program gracefully on SIGINT (CTRL+C signal) and SIGTERM (Termination signal)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while True:
        time.sleep(0.1)
