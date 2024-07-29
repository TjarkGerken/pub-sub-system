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
        self.__lock = threading.RLock()
        self.__database_file = "database/message_broker.db"
        self.database_init()

        # Setup Sockets
        self.__sensor_udp_socket = ReceivingCommunicationProtocolSocket("MB_SENSOR", 5004, self.__database_file)
        self.__subscription_socket = ReceivingCommunicationProtocolSocket("MB_SUBSCRIPTION", 6000)
        self.__broadcast_udp_socket = SendingCommunicationProtocolSocket("MB_BROADCAST", 6200)

        threading.Thread(target=self.run_sensor_listener).start()
        threading.Thread(target=self.run_subscription_listener).start()
        threading.Thread(target=self.run_subscription_handler).start()
        threading.Thread(target=self.run_broadcast).start()

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

    def prefill_subscriber_queues(self, subscriber_id, addr):
        with self.__lock:
            db_connection = sqlite3.connect(self.__database_file, )
            db_cursor = db_connection.cursor()

            subscriber_messages = db_cursor.execute(
                "SELECT Data FROM main.MessagesToSend WHERE SubscriberID = ? ORDER BY MessageID",
                (subscriber_id,)).fetchall()

            db_cursor.close()
            db_connection.close()

        for message in subscriber_messages:
            self.subscriber_queues[addr]["queue"].put(message[0])

        return None

    def prefill_queue(self):
        # Get all subscriptions from the database
        with self.__lock:
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            db_cursor.execute("SELECT * FROM Subscriber")
            subscribers = db_cursor.fetchall()

            db_cursor.close()
            db_connection.close()

        for subscriber in subscribers:
            address, port, topic = subscriber[1], subscriber[2], subscriber[3]
            addr = (address, port)
            if topic == "UV":
                self.__subscribers_uv.append(addr)
            elif topic == "TEMP":
                self.__subscribers_temp.append(addr)

            self.subscriber_queues[addr] = {
                "queue": queue.Queue(),
                "thread": StoppableThread(target=self.broadcast_message, args=(addr,))
            }
            self.prefill_subscriber_queues(subscriber[0], addr)
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
            self.subscribers_map[topic].remove(addr)

            is_present = False
            for s_topic, s_subscribers in self.subscribers_map.items():
                if addr in s_subscribers:
                    is_present = True
                    with (self.__lock):
                        old_queue = self.subscriber_queues[addr]["queue"]
                        self.subscriber_queues[addr]["queue"] = queue.Queue()
                        while not old_queue.empty():
                            message = old_queue.get()
                            if isinstance(message, str):
                                try:
                                    message = json.loads(message)
                                except (TypeError, json.JSONDecodeError):
                                    message = ast.literal_eval(message)
                            if message["sensor_type"] == "S" and topic == "UV" and is_present:
                                self.subscriber_queues[addr]["queue"].put(message)
                            elif message["sensor_type"] == "U" and topic == "TEMP" and is_present:
                                self.subscriber_queues[addr]["queue"].put(json.dumps(message))
                            old_queue.task_done()
                self.delete_all_from_db_messages_to_send(addr, topic)
                self.remove_from_db_subscriber(addr, topic)
                break

            if not is_present:
                try:
                    self.subscriber_queues[addr]["thread"].stop()
                    self.subscriber_queues[addr]["thread"].join()
                except:
                    logger.critical("Thread already not existing")
                try:
                    del self.subscriber_queues[addr]
                except KeyError as e:
                    logger.critical(f"Subscriber {addr} not found in subscriber_queues")

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
                self.distribute_message_to_list(self.__subscribers_uv, message, "UV")
            elif message["sensor_type"] == "S":
                self.distribute_message_to_list(self.__subscribers_temp, message, "TEMP")

    def broadcast_message(self, subscriber):
        thread = self.subscriber_queues[subscriber]["thread"]
        while not thread.stopped():
            if not self.init_done:
                continue

            subscriber_queue = self.subscriber_queues[subscriber]["queue"]
            if not subscriber_queue.empty():
                message = subscriber_queue.get()
                # TODO: If message == Type U => TOPIC => UV ELSE TOPIC = TEMP
                topic = ""
                if isinstance(message, str):
                    try:
                        message = json.loads(message)
                    except TypeError:
                        message = ast.literal_eval(message)
                if message["sensor_type"] == "S":
                    topic = "TEMP"
                elif message["sensor_type"] == "U":
                    topic = "UV"
                self.__broadcast_udp_socket.send_message(json.dumps(message), subscriber)
                self.delete_from_db_messages_to_send(subscriber, topic, json.dumps(message))
                subscriber_queue.task_done()

    def distribute_message_to_list(self, broadcast_list, message, topic):
        for subscriber in broadcast_list:
            try:
                self.subscriber_queues[subscriber]["queue"].put(json.dumps(message))
                self.insert_to_db_messages_to_send(message, subscriber, topic)
            except:
                logger.debug("Could not find Queue => Subscriber not subscribed")
        self.__sensor_udp_socket.delete_message_from_db(message)
        self.__sensor_udp_socket.message_queue.task_done()

    def delete_from_db_messages_to_send(self, subscriber, topic, data):
        with self.__lock:
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            subscriber_id = db_cursor.execute(
                "SELECT SubscriberID FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (subscriber[0], subscriber[1], topic)).fetchone()

            if subscriber_id:
                subscriber_id = subscriber_id[0]
                db_cursor.execute("DELETE FROM MessagesToSend WHERE SubscriberID = ? AND Data = ?",
                                  (subscriber_id, data))
                db_connection.commit()
            db_cursor.close()
            db_connection.close()

    def delete_all_from_db_messages_to_send(self, subscriber, topic):
        with self.__lock:
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()
            logger.critical(f"{str(subscriber[0])}, {str(subscriber[1])}, {str(topic)}")

            subscriber_id = db_cursor.execute(
                "SELECT SubscriberID FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (subscriber[0], subscriber[1], topic)).fetchone()
            logger.critical("DELETE ALL FAST JETZE")
            logger.critical(str(subscriber_id))

            if subscriber_id:
                subscriber_id = subscriber_id[0]
                logger.critical("DELETE ALL JETZE")
                db_cursor.execute("DELETE FROM MessagesToSend WHERE SubscriberID = ?",
                                  (subscriber_id,))
                db_connection.commit()
            db_cursor.close()
            db_connection.close()

    def insert_to_db_messages_to_send(self, data: str, subscriber: str, topic: str):
        with self.__lock:
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()
            subscriber_id = db_cursor.execute(
                "SELECT SubscriberID FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (subscriber[0], subscriber[1], topic)).fetchone()
            if subscriber_id:
                subscriber_id = subscriber_id[0]
                try:
                    db_cursor.execute("INSERT INTO MessagesToSend (SubscriberId, Data) VALUES (?, ?)",
                                      (subscriber_id, json.dumps(data)))
                    db_connection.commit()
                except sqlite3.IntegrityError as e:
                    logger.debug(f"Message {data} is already in the database")

            db_cursor.close()
            db_connection.close()


if __name__ == "__main__":
    MessageBroker()
