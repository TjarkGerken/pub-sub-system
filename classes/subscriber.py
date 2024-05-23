import queue
import socket


class Subscriber:
    __subscriber_port: int
    __subscriber_type: str
    __upd_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    __messageQueue = queue.Queue()
    __subscriptions = []

    def __init__(self, subscriber_port: int, subscriber_type: str):
        if subscriber_type not in ["U", "S", "B"]:
            raise ValueError("Sensor type must be either 'U' or 'S' or 'B'")
        self.__subscriber_port = subscriber_port
        self.__subscriber_type = subscriber_type


    def subscribe(self, location: str, sensor_type: str):
        # Send subscription request to the messagebroker based on the type
        # Listen for the response from the messagebroker with a port and store it in the subscroptoons
        print("Subscribe")

    def run_subscriber(self):
        # For all the ports in the subscriptions list, listen for the messages
        # If a message is received, put it in the messageQueue
        print("Run subscriber")

    def run_logger(self):
        # If the messageQueue is not empty, log the message
        print("Run logger")




