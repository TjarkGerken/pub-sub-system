import queue
import select
import socket
import threading
import time

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
        self.__subscription_udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__subscription_udp_socket.bind(("127.0.0.1", subscriber_port))

        # Subscripe to the messagebroker on the appropriate ports
        self.initiate_subscription()

        threading.Thread(target=self.run_subscriber).start()
        threading.Thread(target=self.run_logger).start()

    def initiate_subscription(self):
        # Send subscription request to the messagebroker based on the type
        # Listen for the response from the messagebroker with a port and store it in the subscriptions
        subscriptions = []
        if self.__subscriber_type == "U" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_UV".encode())
        if self.__subscriber_type == "S" or self.__subscriber_type == "B":
            subscriptions.append(f"SUBSCRIBE_TEMP".encode())

        for subscription in subscriptions:
            while True:
                self.__subscription_udp_socket.sendto(subscription, ("127.0.0.1", 6000))
                ready = select.select([self.__subscription_udp_socket], [], [], 5)
                start_time = time.time()
                if ready[0]:
                    data, addr = self.__subscription_udp_socket.recvfrom(1024)
                    print(f"[INFO] | {self.__subscriber_id} | {data.decode()}")
                    break
                elif time.time() - start_time > RETRY_DURATION_IN_SECONDS:
                    print(f"{self.__subscriber_id} | Response timeout")
                    break

    def run_subscriber(self):
        # For all the ports in the subscriptions list, listen for the messages
        # If a message is received, put it in the messageQueue
        while True:
            data, addr = self.__subscription_udp_socket.recvfrom(1024)
            handle_connection_thread = threading.Thread(target=self.handle_incoming_message, args=(data, addr))
            handle_connection_thread.start()

    def run_logger(self):
        while True:
            if self.__messageQueue.empty():
                time.sleep(1)
                continue
            message = self.__messageQueue.get()
            print(f"[SUCCESS] | {self.__subscriber_id} | {message}")
            time.sleep(0.5)

    def handle_incoming_message(self, data, addr):
        self.__subscription_udp_socket.sendto("Confirm".encode(), addr)
        self.__messageQueue.put(data.decode())
