import ast
import json
import queue
import threading
import sqlite3

from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.StoppableThread import StoppableThread
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
        self.subscriber_queues = {}
        self.__subscribers_uv = []
        self.__subscribers_temp = []

        self.subscribers_map = {
            "UV": self.__subscribers_uv,
            "TEMP": self.__subscribers_temp
        }  # When adding a topic this needs to be updated

        # Setup Database
        self.init_done = False
        self.__lock = threading.Lock()
        self.__database_file = "database/message_broker.db"
        self.database_init()

        # TODO: Key: SensorId, Value: Queue
        # Wenn neue Nachricht beim MB ankommt, dann wird geschaut welches Topic, dann schauen welcher Subscriber
        # => über Subscriber ID auf Queue zugreifen wo Nachrichten gespeichert werden -> queue.put
        # in thread geben wir die queue

        self.init_done = True

        logger.info("Message Broker Started")

    def database_init(self):
        """
        Initializes the SQLite database by creating the necessary tables if they do not already exist.
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

        # Prefill the subscriber queues from database
        self.prefill_queue()

        return None

    def prefill_queue(self, db_connection, db_cursor):
        # Get all subscriptions from the database
        db_cursor.execute("SELECT * FROM Subscriber")
        subscribers = db_cursor.fetchall()
        db_connection.close()
        db_cursor.close()

        for subscriber in subscribers:
            address, port, topic = subscriber[1], subscriber[2], subscriber[3]
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

        return None


    def run_sensor_listener(self):
        self.__sensor_udp_socket.listener()

    def run_subscription_listener(self):
        self.__subscription_socket.listener()

    def run_subscription_handler(self):
        while True:
            if self.__subscription_socket.message_queue.empty() or not self.init_done:
                continue
            result = self.__subscription_socket.message_queue.get()
            threading.Thread(target=self.handle_subscription_message, args=(result,)).start()

    def handle_subscription_message(self, data):
        try:
            subscription, addr = data.split(";")
            ip_str, port_str = addr.strip("()").split(", ")
            ip = ip_str.strip("'")
            port = int(port_str)
            addr = (ip, port)
        except ValueError:
            logger.error(f"[MB_SUBSCRIPTION] | Invalid Subscription Message ({data})")
            return None

        message = None
        action, topic = subscription.split("_")

        if topic not in self.subscribers_map:
            logger.error(f"[MB_SUBSCRIPTION] | Invalid Topic ({topic})")
            return None

        if action == "SUBSCRIBE":
            self.insert_to_db_subscriber(addr, topic)
            self.subscribers_map[topic].append(addr)  # TODO: IF ERROR MAYBE IT OCCURS HERE :)
            message = f"{topic}"

            if addr not in self.subscriber_queues:
                self.subscriber_queues[addr] = {
                    "queue": queue.Queue(),
                    "thread": StoppableThread(target=self.broadcast_message, args=(addr,))
                }
                self.subscriber_queues[addr]["thread"].start()

            logger.info(f"[MB_SUBSCRIPTION] | Successfully Subscribed {addr} to Topic {message}")

        elif action == "UNSUBSCRIBE":
            self.remove_from_db_subscriber(addr, topic)
            self.subscribers_map[topic].remove(addr)

            is_present = False
            for s_topic, s_subscribers in self.subscribers_map.items():
                if addr in s_subscribers:
                    is_present = True
                    break

            if not is_present:
                self.subscriber_queues[addr]["thread"].stop()
                self.subscriber_queues[addr]["thread"].join()
                del self.subscriber_queues[addr]

            logger.info(f"[MB_SUBSCRIPTION] | Successfully Unsubscribed {addr} from Topic {topic}")

        return None

    def remove_from_db_subscriber(self, addr, topic):
        with self.__lock:
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            subscriber_id = db_cursor.execute(
                "DELETE FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (addr[0], addr[1], topic))
            db_connection.commit()

            db_cursor.close()
            db_connection.close()

    def insert_to_db_subscriber(self, addr, topic):
        with self.__lock:
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            try:
                db_cursor.execute("INSERT INTO Subscriber (Address, Port, Topic) VALUES (?, ?, ?)",
                                  (addr[0], int(addr[1]), topic))
                db_connection.commit()
            except sqlite3.IntegrityError:
                logger.debug(f"Subscriber {addr} is already subscribed to {topic}")
            finally:
                db_cursor.close()
                db_connection.close()

        return None

    def run_broadcast(self):
        while True:
            if self.__sensor_udp_socket.message_queue.empty() or not self.init_done:
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
