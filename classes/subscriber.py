import json
import queue
import select
import socket
import threading
import time
from json import JSONDecodeError

from classes.udpsocket import UdpSocket
from configuration import RETRY_DURATION_IN_SECONDS


class Subscriber:
    __messageQueue = queue.Queue()

    def __init__(self, subscriber_port: int, subscriber_type: str):
        if subscriber_type not in ["U", "S", "B"]:
            raise ValueError("Sensor  type must be either 'U' or 'S' or 'B'")
        self.__subscriber_port = subscriber_port
        self.__subscriber_type = subscriber_type
        self.__subscriber_id = f"SUBSCRIBER_{subscriber_type}_{subscriber_port}"

        # Socket
        self.__subscription_udp_socket = UdpSocket(subscriber_port, self.__subscriber_id)

        # Subscribe to the message broker on the appropriate ports
        self.initiate_subscription()

        threading.Thread(target=self.run_subscriber).start()
        threading.Thread(target=self.run_logger).start()

    def initiate_subscription(self):
        # Send subscription request to the messagebroker based on the type
        # Listen for the response from the messagebroker with a port and store it in the subscriptions
        subscriptions = []
        if self.__subscriber_type == "U" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_UV")
        if self.__subscriber_type == "S" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_TEMP")

        for subscription in subscriptions:
            self.__subscription_udp_socket.three_way_send(subscription, ("127.0.0.1", 6000))

    def run_subscriber(self):
        # For all the ports in the subscriptions list, listen for the messages
        # If a message is received, put it in the messageQueue
        while True:
            result = self.__subscription_udp_socket.listen()
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
            except  JSONDecodeError as e:
                print(f"[ERROR] | {self.__subscriber_id} | {e} | {message}")
                continue
            if message["sensor_type"] == "U":
                sensor_value = f"☀️  {message['uv_index']} UV in {message['location']}"
            elif message["sensor_type"] == "S":
                sensor_value = f"🌡️  {message['temperature']}  °C in {message['location']}"

            print(f"[SUCCESS] | {self.__subscriber_id} | {sensor_value}")
            time.sleep(0.1)

    def handle_incoming_message(self, data, addr):
        self.__messageQueue.put(data)
