import ast
import queue
import threading
import sqlite3

from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.logger import logger


class MessageBroker:
    """
    Message Broker class that acts as a middleman between the sensors and the subscribers. It handles the subscription
    and broadcasting of messages between the publishers (sensors) and the subscribers. To ensure persistence it stores
    the messages and subscribers in a SQLite database.
    """

    def __init__(self):
        """
        Initializes the Message Broker by initializing the database, creating the necessary sockets and starting the
        listener threads.
        """
        self.__subscribers_uv = []
        self.__subscribers_temp = []

        self.__database_file = "database/message_broker.db"
        self.database_init()

        # Setup Sockets
        self.__sensor_udp_socket = ReceivingCommunicationProtocolSocket("MB_SENSOR", 5004, self.__database_file)
        self.__subscription_socket = ReceivingCommunicationProtocolSocket("MB_SUBSCRIPTION", 6000)
        self.__broadcast_udp_socket = SendingCommunicationProtocolSocket("MB_BROADCAST", 6200)

        threading.Thread(target=self.run_sensor_listener).start()
        threading.Thread(target=self.run_subscription_listener).start()
        threading.Thread(target=self.run_subscription_handler).start()

        self.subscriber_queues = {}
        # TODO: Key: SensorId, Value: Queue
        # Wenn neue Nachricht beim MB ankommt, dann wird geschaut welches Topic, dann schauen welcher Subscriber
        # => über Subscriber ID auf Queue zugreifen wo Nachrichten gespeichert werden -> queue.put
        # in thread geben wir die queue
        threading.Thread(target=self.run_broadcast).start()
        logger.info("Message Broker Started")

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

        try:
            subscription, addr = data.split(";")
            ip_str, port_str = addr.strip("()").split(", ")
            ip = ip_str.strip("'")
            port = int(port_str)
            addr = (ip, port)
        except ValueError:
            logger.error(f"[MB_SUBSCRIPTION] | Invalid Subscription Message")
            subscription, addr = None, None

        if subscription and addr:
            if subscription == "SUBSCRIBE_UV":
                self.__subscribers_uv.append(addr)
                message = "UV-Index ☀️"
            elif subscription == "SUBSCRIBE_TEMP":
                self.__subscribers_temp.append(addr)
                message = "Temperature 🌡️️"
            confirmation_message = f"[MB_SUBSCRIPTION] | Successfully Subscribed {addr} to {message}"
            logger.info(confirmation_message)

    def run_broadcast(self):
        while True:
            if self.__sensor_udp_socket.message_queue.empty():
                continue
            message = self.__sensor_udp_socket.message_queue.get()
            message = ast.literal_eval(message)

            if message["sensor_type"] == "U":
                self.broadcast_message_to_list(self.__subscribers_uv, message)
            elif message["sensor_type"] == "S":
                self.broadcast_message_to_list(self.__subscribers_temp, message)

            self.__sensor_udp_socket.message_queue.task_done()

    def broadcast_message(self, message, subscriber):
        self.__broadcast_udp_socket.send_message(str(message), subscriber)

    def broadcast_message_to_list(self, broadcast_list, message):
        threads = []
        for subscriber in broadcast_list:
            thread = threading.Thread(target=self.broadcast_message, args=(message, subscriber))
            thread.start()
            threads.append(thread)

        while len(threads) != 0:
            for thread in threads:
                if not thread.isAlive():
                    # remove item from subscriber queue
                    # remove thread form list
                    continue


if __name__ == "__main__":
    MessageBroker()
