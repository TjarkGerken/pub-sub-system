import ast
import json
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

    def database_init(self):
        """
            TODO: Documentation
        """
        # Create the database if it does not exist and connect to it
        db_connection = sqlite3.connect(self.__database_file)
        db_cursor = db_connection.cursor()

        # Execute DDL script to create tables if not already exist
        with open("database/ddl_mb.sql", "r") as ddl_file:
            db_cursor.executescript(ddl_file.read())
            db_connection.commit()

        # Close connection
        db_cursor.close()
        db_connection.close()

    def prefill_queue(self, db_connection, db_cursor):
        # Get all subscriptions from the database
        db_cursor.execute("SELECT * FROM Subscriber")
        subscribers = db_cursor.fetchall()

        for subscriber in subscribers:
            address, port, topic = subscriber["Address"], int(subscriber["Port"]), subscriber["Topic"]
            addr = (address, port)
            if topic == "UV":
                self.__subscribers_uv.append(addr)
            elif topic == "TEMP":
                self.__subscribers_temp.append(addr)

            self.subscriber_queues[addr] = {
                "queue": queue.Queue(),
                "thread": threading.Thread(target=self.broadcast_message, args=(addr,))
            }

            self.subscriber_queues[addr]["thread"].start()
            logger.info(f"[MB_SUBSCRIPTION] | Successfully Resubscribed {addr} to {topic}")

        db_connection.close()
        db_cursor.close()

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
        # TODO: UNSUBSCRIBE Function
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
                message = "UV-Index"
            elif subscription == "SUBSCRIBE_TEMP":
                self.__subscribers_temp.append(addr)
                message = "Temperature"

            action, topic = subscription.split("_")

            if action == "SUBSCRIBE":
                self.insert_to_db(addr, topic)
                self.subscriber_queues[addr] = {
                    "queue": queue.Queue(),
                    "thread": threading.Thread(target=self.broadcast_message, args=(addr,))
                } # TODO: How to stop if unsubscribe

                self.subscriber_queues[addr]["thread"].start()
                logger.info(f"[MB_SUBSCRIPTION] | Successfully Subscribed {addr} to {message}")

    def insert_to_db(self, addr, topic):
        db_connection = sqlite3.connect(self.__database_file)
        db_cursor = db_connection.cursor()

        try:
            db_cursor.execute("INSERT INTO Subscriber (Address, Port, Topic) VALUES (?, ?, ?)",
                              (addr[0], int(addr[1]), topic))
            db_connection.commit()
        except sqlite3.IntegrityError:
            logger.debug(f"Subscriber {addr} is already subscribed to {topic}")

        db_cursor.close()
        db_connection.close()

    def run_broadcast(self):
        while True:
            if self.__sensor_udp_socket.message_queue.empty():
                continue

            message = self.__sensor_udp_socket.message_queue.get()
            message = ast.literal_eval(message)

            if message["sensor_type"] == "U":
                self.distribute_message_to_list(self.__subscribers_uv, message)
            elif message["sensor_type"] == "S":
                self.distribute_message_to_list(self.__subscribers_temp, message)

    def broadcast_message(self, subscriber):
        while True:
            subscriber_queue = self.subscriber_queues[subscriber]["queue"]
            if not subscriber_queue.empty():
                message = subscriber_queue.get()
                self.__broadcast_udp_socket.send_message(json.dumps(message), subscriber)
                subscriber_queue.task_done()

    def distribute_message_to_list(self, broadcast_list, message):
        for subscriber in broadcast_list:
            self.subscriber_queues[subscriber]["queue"].put(message)
        self.__sensor_udp_socket.delete_message_from_db(message)
        self.__sensor_udp_socket.message_queue.task_done()

if __name__ == "__main__":
    MessageBroker()
