import ast
import json
import queue
import signal
import sqlite3
import sys
import threading
import time

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.StoppableThread import StoppableThread
from utils.logger import logger


class MessageBroker:
    """
    Message Broker class that acts as a middleman between the publishers (sensors) and the subscribers. It handles the
    subscription and broadcasting of messages between the publishers (sensors) and the subscribers. To ensure
    persistence it stores the messages and subscribers in a database.
    """

    def __init__(self) -> None:
        """
        Initializes the Message Broker by initializing the database, creating the necessary sockets and starting the
        listener threads.

        :return: None
        """
        self.subscriber_queues = {}
        self.__subscribers_uv = []
        self.__subscribers_temp = []
        self.subscribers_map = {"UV": self.__subscribers_uv,
            "TEMP": self.__subscribers_temp}  # When adding a topic this needs to be updated
        self.sequence_number = 0

        # Setup Database
        self.init_done = False
        self.__lock = threading.Lock()
        self.__database_file = "database/message_broker.db"
        self.database_init()

        # Setup Actions for signal based stopping (gracefully)
        self.__actions = []

        # Setup Sockets
        self._sensor_udp_socket = ReceivingCommunicationProtocolSocket("MB_SENSOR", 5004, self.__database_file)
        self.__subscription_socket = ReceivingCommunicationProtocolSocket("MB_SUBSCRIPTION", 6000)
        self.__broadcast_udp_socket = SendingCommunicationProtocolSocket("MB_BROADCAST", 6200)

        self.__actions.append(self._sensor_udp_socket)
        self.__actions.append(self.__subscription_socket)
        self.__actions.append(self.__broadcast_udp_socket)

        # Setup Threads
        self.__sensor_listener_thread = StoppableThread(target=self.run_sensor_listener)
        self.__subscription_listener_thread = StoppableThread(target=self.run_subscription_listener)
        self.__subscription_handler_thread = StoppableThread(target=self.run_subscription_handler)
        self.__broadcast_thread = StoppableThread(target=self.run_broadcast)

        self.__sensor_listener_thread.start()
        self.__subscription_listener_thread.start()
        self.__subscription_handler_thread.start()
        self.__broadcast_thread.start()

        self.__actions.append(self.__broadcast_thread)
        self.__actions.append(self.__sensor_listener_thread)
        self.__actions.append(self.__subscription_listener_thread)
        self.__actions.append(self.__subscription_handler_thread)

        self.init_done = True
        logger.info("Message Broker Initialized")

    def database_init(self) -> None:
        """
        Initializes the SQLite database by creating the necessary tables if they don't already exist.
        :return: None
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

    def prefill_subscriber_queues(self, subscriber_id: str, addr: tuple[str, int]) -> None:
        """
        Prefills the subscriber queues with messages from the database that were not sent to the message broker yet

        :param subscriber_id: Identifier of the subscriber
        :param addr: Tuple containing the address as string and port as integer of the subscriber

        :return: None
        """
        # Lock the database to prevent conflicts with other threads
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.__database_file, )
            db_cursor = db_connection.cursor()

            # Get all messages that were not sent yet
            subscriber_messages = db_cursor.execute(
                "SELECT Data FROM main.MessagesToSend WHERE SubscriberID = ? ORDER BY MessageID",
                (subscriber_id,)).fetchall()

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        # Put the messages retrieved from database into the subscriber's queue
        for message in subscriber_messages:
            self.subscriber_queues[addr]["queue"].put(message[0])

        return None

    def prefill_queue(self) -> None:
        """
        Prefills the subscriber queues with messages from the database
        :return: None
        """
        # Lock the database to prevent conflicts with other threads
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            # Get all subscribers from the database
            db_cursor.execute("SELECT * FROM Subscriber")
            subscribers = db_cursor.fetchall()

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        # Iterate over all subscribers and prefill their queues
        for subscriber in subscribers:
            address, port, topic = subscriber[1], subscriber[2], subscriber[3]
            addr = (address, port)

            # Append the subscriber to list of subscribers that are subscribed to the topic
            if topic == "UV":
                self.__subscribers_uv.append(addr)
            elif topic == "TEMP":
                self.__subscribers_temp.append(addr)

            # Create a new queue for each subscriber so that they can receive messages dependently from each other and
            # don't hinder other subscribers from receiving messages while paying attention to the order of the messages
            self.subscriber_queues[addr] = {"queue": queue.Queue(),
                "thread": StoppableThread(target=self.broadcast_message, args=(addr,))}

            # Prefill the subscriber's queue with messages from the database and start the thread to send the messages
            # when available
            self.prefill_subscriber_queues(subscriber[0], addr)
            self.subscriber_queues[addr]["thread"].start()

            logger.info(f"[MB_SUBSCRIPTION] | Successfully Resubscribed {addr} to {topic}")  # TODO: Remove re-?

        return None

    def run_sensor_listener(self) -> None:
        """
        Runs the listener for the sensor socket
        :return: None
        """
        self._sensor_udp_socket.listener()
        return None

    def run_subscription_listener(self) -> None:
        """
        Runs the listener for the subscription socket
        :return: None
        """
        self.__subscription_socket.listener()
        return None

    def run_subscription_handler(self) -> None:
        """
        Runs the handler for the subscription messages
        :return: None
        """
        # Send messages to subscribers as soon as they are available as long as the thread is not stopped
        while not self.__subscription_handler_thread.stopped():
            # If no messages are available or the initialization is not done yet, don't perform any actions
            if self.__subscription_socket.message_queue.empty() or not self.init_done:
                continue

            # Retrieve the message from the queue and start a new thread to handle the subscription message
            result = self.__subscription_socket.message_queue.get()
            StoppableThread(target=self.handle_subscription_message, args=(result,)).start()

        return None

    def handle_subscription_message(self, data: str) -> None:
        """
        Handles the subscription message by subscribing or unsubscribing the subscriber to a topic.

        :param data: Subscription message containing the action (SUBSCRIBE/UNSUBSCRIBE) and the topic (UV/TEMP) and
        address of the subscriber

        :return: None
        """
        try:
            # Parse the data to retrieve the subscription message and the address of the subscriber
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

        # If the requested topic is not available, log an error and don't perform any actions
        if topic not in self.subscribers_map:
            logger.error(f"[MB_SUBSCRIPTION] | Invalid Topic ({topic})")
            return None

        # If topic is valid try to subscribe the subscriber to the topic
        if action == "SUBSCRIBE":
            # Insert subscriber to the database
            self.insert_to_db_subscriber(addr, topic)
            # Append the subscriber to the list of subscribers that are subscribed to the topic
            self.subscribers_map[topic].append(addr)  # TODO: IF ERROR MAYBE IT OCCURS HERE :)

            message = f"{topic}"

            # If the subscriber does not already have a queue, create a one alongside a thread for the subscriber
            if addr not in self.subscriber_queues:
                self.subscriber_queues[addr] = {"queue": queue.Queue(),
                    "thread": StoppableThread(target=self.broadcast_message, args=(addr,))}
                self.subscriber_queues[addr]["thread"].start()

            logger.info(f"[MB_SUBSCRIPTION] | Successfully Subscribed {addr} to Topic {message}")

        elif action == "UNSUBSCRIBE":
            try:
                # Remove subscriber address from the list of subscribers that are subscribed to the topic so it won't
                # get any messages anymore from the topic
                self.subscribers_map[topic].remove(addr)
            except Exception as e:
                logger.error(f"[MB_SUBSCRIPTION] | {addr} not subscribed to {topic}")
                return None

            is_present = False
            # Check if the subscriber is present in any other topics and if so, transfer messages from the old queue
            # to a new one to remove the subscriber and its messages from the database
            for s_topic, s_subscribers in self.subscribers_map.items():
                #
                if addr in s_subscribers:
                    is_present = True
                    with self.__lock:
                        # Get the old queue and create a new one for the subscriber
                        old_queue = self.subscriber_queues[addr]["queue"]
                        self.subscriber_queues[addr]["queue"] = queue.Queue()

                        # Transfer messages from the old queue to the new one
                        while not old_queue.empty():
                            message = old_queue.get()
                            if isinstance(message, str):
                                # Parse the message
                                try:
                                    message = json.loads(message)
                                except (TypeError, json.JSONDecodeError):
                                    message = ast.literal_eval(message)

                            # TODO: Why is sensor_type and topic not the same?
                            # Check if the message corresponds to the correct sensor type and topic before re-adding it
                            # to the queue
                            if message["sensor_type"] == "S" and topic == "UV" and is_present:
                                self.subscriber_queues[addr]["queue"].put(message)
                            elif message["sensor_type"] == "U" and topic == "TEMP" and is_present:
                                self.subscriber_queues[addr]["queue"].put(json.dumps(message))

                            old_queue.task_done()

                # Remove the subscriber from the database and all the messages related to the subscriber that were not
                # sent yet
                self.delete_all_from_db_messages_to_send(addr, topic)
                self.remove_from_db_subscriber(addr, topic)
                break

            # If the subscriber is not subscribed to any other topic, stop the thread for sending messages and remove
            # the subscriber from the list of subscribers
            if not is_present:
                # Stop the thread for sending messages
                try:
                    self.subscriber_queues[addr]["thread"].stop()
                    self.subscriber_queues[addr]["thread"].join()
                except Exception as e:
                    logger.critical("Thread already not existing")
                    logger.debug(f"Thread already not existing: {e}")

                # Remove the subscriber from the list of subscribers
                try:
                    del self.subscriber_queues[addr]
                except KeyError as e:
                    logger.critical(f"Subscriber {addr} not found in subscriber_queues")

            logger.info(f"[MB_SUBSCRIPTION] | Successfully Unsubscribed {addr} from Topic {topic}")

        return None

    def remove_from_db_subscriber(self, addr: tuple[str, int], topic: str) -> None:
        """
        Removes the subscriber from the database so it won't receive any messages anymore

        :param addr: Address of the subscriber as tuple containing the IP address as string and the port as integer of
        the subscriber
        :param topic: The topic the subscriber is subscribed to and wants to unsubscribe from

        :return: None
        """
        # Lock the database to prevent conflicts with other threads
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            # Remove the subscriber from the database
            subscriber_id = db_cursor.execute("DELETE FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (addr[0], addr[1], topic))
            db_connection.commit()

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        return None

    def insert_to_db_subscriber(self, addr: tuple[str, int], topic: str) -> None:
        """
        Inserts a subscriber into the database.

        :param addr: The address of the subscriber as tuple containing the IP address as string and the port as integer
        :param topic: The topic the subscriber wants to subscribe to

        :return: None
        """
        # Lock the database to prevent conflicts with other threads
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            try:
                # Insert the subscriber into the database
                db_cursor.execute("INSERT INTO Subscriber (Address, Port, Topic) VALUES (?, ?, ?)",
                                  (addr[0], int(addr[1]), topic))
                db_connection.commit()
            except sqlite3.IntegrityError:
                logger.debug(f"Subscriber {addr} is already subscribed to {topic}")
            finally:
                # Close the database connection
                db_cursor.close()
                db_connection.close()

        return None

    def run_broadcast(self) -> None:
        """
        Runs the broadcast thread to send messages to the subscribers
        :return: None
        """
        # Send messages to subscribers as soon as they are available as long as the thread is not stopped
        while not self.__broadcast_thread.stopped():
            # If no messages are available or the initialization is not done yet, don't perform any actions
            if self._sensor_udp_socket.message_queue.empty() or not self.init_done:
                continue

            message = self._sensor_udp_socket.message_queue.get()
            message = ast.literal_eval(message)

            # Check to which topic the message belongs to and send it to the subscribers that are subscribed to it
            if message["sensor_type"] == "U":
                self.distribute_message_to_list(self.__subscribers_uv, message, "UV")
            elif message["sensor_type"] == "S":
                self.distribute_message_to_list(self.__subscribers_temp, message, "TEMP")

        return None

    def broadcast_message(self, subscriber: tuple[str, int]) -> None:
        """
        Broadcasts the messages to the subscriber from their queue

        :param subscriber: The subscriber for which to check if there are messages in the queue to send as tuple with
        the IP address as string and the port as integer

        :return: None
        """
        # Get the thread of the subscriber to check if it is still running
        while not self.subscriber_queues[subscriber]["thread"].stopped():
            # If the initialization is not done yet, don't perform any actions
            if not self.init_done:
                continue

            # If the queue of the subscriber is not empty, get the message and send it to the subscriber
            subscriber_queue = self.subscriber_queues[subscriber]["queue"]
            if not subscriber_queue.empty():
                message = subscriber_queue.get()

                # Check if the message is a string or a dictionary and parse it accordingly
                if isinstance(message, str):
                    try:
                        message = json.loads(message)
                    except TypeError:
                        message = ast.literal_eval(message)

                # Check to which topic the message belongs to and send it to the subscriber
                if message["sensor_type"] == "S":
                    topic = "TEMP"
                elif message["sensor_type"] == "U":
                    topic = "UV"
                else:
                    topic = ""

                # Send the message to the subscriber
                self.__broadcast_udp_socket.send_message(json.dumps(message), subscriber)
                self.delete_from_db_messages_to_send(subscriber, topic, json.dumps(message))

                # Mark the task as done
                subscriber_queue.task_done()

        return None

    def distribute_message_to_list(self, broadcast_list: list[tuple[str, int]], message: str, topic: str) -> None:
        """
        Distributes the message to the subscribers that are subscribed to the topic

        :param broadcast_list: List of subscribers that are subscribed to the topic
        :param message: The message to send to the subscribers
        :param topic: The topic the message belongs to

        :return: None
        """
        # Iterate over all subscribers that are subscribed to the topic and send the message to them
        for subscriber in broadcast_list:
            try:
                self.subscriber_queues[subscriber]["queue"].put(json.dumps(message))
                # Write message into database for persistence if subscriber is not available
                self.insert_to_db_messages_to_send(message, subscriber, topic)
            except:
                logger.debug(f"Could not find Queue: Subscriber ({subscriber}) not subscribed to topic {topic}")

        # Delete the message from the database after it was sent to all subscribers
        self._sensor_udp_socket.delete_message_from_db(message)
        self.sequence_number += 1
        self._sensor_udp_socket.message_queue.task_done()

    def delete_from_db_messages_to_send(self, subscriber: tuple[str, int], topic: str, data: str) -> None:
        """
        Deletes the message from the database that was sent to the subscriber to ensure persistence and that the message
        is not sent again

        :param subscriber: The subscriber to which the message was sent as tuple containing the IP address as string
        :param topic: The topic the subscriber is subscribed to
        :param data: The message that was sent to the subscriber

        :return: None
        """
        # Lock the database to prevent conflicts with other threads
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            # Get the subscriber ID of the specified subscriber from the database
            subscriber_id = db_cursor.execute(
                "SELECT SubscriberID FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (subscriber[0], subscriber[1], topic)).fetchone()

            # If the subscriber is found in the database, delete the message from the database
            if subscriber_id:
                # Just select SubscriberID from the query result
                subscriber_id = subscriber_id[0]

                # Delete the message task from the database to ensure persistence and that the message is not sent again
                db_cursor.execute("DELETE FROM MessagesToSend WHERE SubscriberID = ? AND Data = ?",
                                  (subscriber_id, data))
                db_connection.commit()

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        return None

    def delete_all_from_db_messages_to_send(self, subscriber: tuple[str, int], topic: str) -> None:
        """
        Delete all messages for a subscriber from the database that were not sent to the subscriber yet.
        This function is used to delete all messages for a subscriber when it unsubscribes from a topic.

        :param subscriber: The subscriber for which to delete all messages from the database as tuple containing the IP
        address as string and the port as integer
        :param topic: The topic the subscriber is subscribed to

        :return: None
        """
        # Lock the database to prevent conflicts with other threads
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            # Get the subscriber ID of the specified subscriber from the database
            subscriber_id = db_cursor.execute(
                "SELECT SubscriberID FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (subscriber[0], subscriber[1], topic)).fetchone()

            # If the subscriber is found in the database, delete all messages belonging to the subscriber
            if subscriber_id:
                # Just select SubscriberID from the query result
                subscriber_id = subscriber_id[0]

                # Delete all messages from the database that were not sent to the subscriber yet
                db_cursor.execute("DELETE FROM MessagesToSend WHERE SubscriberID = ?", (subscriber_id,))
                db_connection.commit()

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        return None

    def insert_to_db_messages_to_send(self, subscriber: tuple[str, int], topic: str, data: str) -> None:
        """
        Inserts the message to send to the subscriber into the database for persistence and to ensure that the message is
        sent to the subscriber even if the subscriber is not available at the moment.

        :param subscriber: The subscriber to which the message should be sent as tuple containing the IP address as
        string and the port as integer
        :param topic: The topic the subscriber is subscribed to
        :param data: The message to send to the subscriber

        :return: None
        """
        # Lock the database to prevent conflicts with other threads
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.__database_file)
            db_cursor = db_connection.cursor()

            # Get the subscriber ID of the specified subscriber from the database
            subscriber_id = db_cursor.execute(
                "SELECT SubscriberID FROM Subscriber WHERE Address = ? AND Port = ? AND Topic = ?",
                (subscriber[0], subscriber[1], topic)).fetchone()

            # If the subscriber is found in the database, insert the message to send to the subscriber into the database
            if subscriber_id:
                # Just select SubscriberID from the query result
                subscriber_id = subscriber_id[0]

                try:
                    # Insert the message to send to the subscriber into the database
                    db_cursor.execute("INSERT INTO MessagesToSend (SubscriberId, Data) VALUES (?, ?)",
                                      (subscriber_id, json.dumps(data)))
                    db_connection.commit()
                except sqlite3.IntegrityError as e:
                    # Ensure that no message occurs twice in the database. This is solved by uniqueness constraints in
                    # the database therefore an IntegrityError is raised if the message is already in the database
                    logger.debug(f"Message {data} is already in the database")

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        return None

    def stop(self) -> None:
        """
        Stops all running tasks of the sensor gracefully to shut the message broker down
        :return: None
        """
        logger.info(f"Shutting down Message Broker")

        # Collect all threads because each subscriber has its own thread
        for subscriber in self.subscriber_queues:
            self.__actions.append(self.subscriber_queues[subscriber]["thread"])

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


def handle_signal(sig, frame) -> None:
    """
    Handle signals to stop the subscriber gracefully

    :param sig: Signal number
    :param frame: Current stack frame

    :return: None
    """
    mb.stop()
    sys.exit(0)


if __name__ == "__main__":
    # Start the message broker
    mb = MessageBroker()

    # Set up handler for stopping the program gracefully on SIGINT (CTRL+C signal) and SIGTERM (Termination signal)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while True:
        time.sleep(0.1)
