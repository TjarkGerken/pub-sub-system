import os
import queue
import sqlite3
import threading
import time
import unittest

import configuration
from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.logger import logger
from utils.StoppableThread import StoppableThread


def database_init():
    """
    Initializes the SQLite database by creating the necessary tables if they do not already exist.
    """
    # Create the database if it does not exist and connect to it
    db_path = "database/message_broker.db"
    db_dir = os.path.dirname(db_path)

    # Ensure the directory exists
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    db_connection = sqlite3.connect(db_path)
    db_cursor = db_connection.cursor()

    # Execute DDL script to create tables if not already exist
    with open("database/ddl_mb.sql", "r") as ddl_file:
        db_cursor.executescript(ddl_file.read())
        db_connection.commit()

    db_cursor.close()
    db_connection.close()
    return None


class TestCommunicationIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        database_init()
        cls.__lock = threading.Lock()

    def test_send_data(self):
        """
        Tests the sending of data from the client to the server in a regular scenario.

        This test sets up a listener thread for the server, sends a test message
        from the client to the server, and verifies that the message is received
        correctly.    Test the sending of data from the client to the server.
        :return: None
        """
        client = SendingCommunicationProtocolSocket("SENDING_SOCKET", 5002)
        server = ReceivingCommunicationProtocolSocket("RECEIVING_SOCKET", 6002, "database/message_broker.db")
        with self.__lock:
            db_connection = sqlite3.connect(server.database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("DELETE FROM MessageSocketQueue")
            db_connection.commit()
            db_cursor.close()
            db_connection.close()

        server.message_queue = queue.Queue()

        def run_listener():
            server.listener()

        listener_thread = StoppableThread(target=run_listener)
        listener_thread.start()
        client.send_message("Test message", ("127.0.0.1", 6002))
        client.send_message("Test message", ("127.0.0.1", 6002))
        time.sleep(2)
        with self.__lock:
            db_connection = sqlite3.connect(server.database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM MessageSocketQueue")
            messages = db_cursor.fetchall()
            db_cursor.close()
            db_connection.close()
        smq_size = server.message_queue.qsize()
        checksums = server.stored_checksums
        sq = client.sequence_number
        cmq_size = client.message_queue.qsize()
        listener_thread.stop()
        client.stop()
        server.stop()
        self.assertEqual(smq_size, 2, "Expected 2 messages in server message queue")
        self.assertEqual(len(checksums), 0, "Expected 0 stored checksums in server")
        self.assertEqual(sq, 2, "Expected client sequence number to be 2")
        self.assertEqual(cmq_size, 0, "Expected 0 messages in client message queue")
        self.assertEqual(len(messages), 2, "Expected 2 messages in the database")

    def test_retry_behaviour(self):
        """
            Test the retry behavior of the communication protocol when the Server is not reachable and no ACK is received.
            This test configures the retry duration and interval, sets up listener and sender threads,
            and verifies that the message is received correctly after retries.
            :return: None
            """
        client = SendingCommunicationProtocolSocket("SENDING_SOCKET", 5003)
        server = ReceivingCommunicationProtocolSocket("RECEIVING_SOCKET", 6003, "database/message_broker.db")
        with self.__lock:
            db_connection = sqlite3.connect(server.database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("DELETE FROM MessageSocketQueue")
            db_cursor.execute("DELETE FROM MessagesToSend")
            db_connection.commit()
            db_cursor.close()
            db_connection.close()

        server.message_queue = queue.Queue()
        message = "Test message"

        def run_listener():
            server.listener()

        def send_message():
            client.send_message(message, ("127.0.0.1", 6003))

        listener_thread = StoppableThread(target=run_listener)
        send_thread = StoppableThread(target=send_message).start()
        time.sleep(10)
        listener_thread.start()
        time.sleep(5)
        received_message = server.message_queue.get()

        listener_thread.stop()
        client.stop()
        server.stop()
        self.assertEqual(received_message, message)

    def test_duplicate_messages(self):
        """
            #TODO: Behaviour diskutieren
            :return:
            """
        client = SendingCommunicationProtocolSocket("SENDING_SOCKET", 5000)
        server = ReceivingCommunicationProtocolSocket("RECEIVING_SOCKET", 6000, "database/message_broker.db")
        with self.__lock:
            db_connection = sqlite3.connect(server.database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("DELETE FROM MessageSocketQueue")
            db_cursor.execute("DELETE FROM MessagesToSend")
            db_connection.commit()
            db_cursor.close()
            db_connection.close()

        server.message_queue = queue.Queue()
        message = "Test message"
        database_init()

        def run_listener():
            server.listener()

        def send_message():
            client.send(("127.0.0.1", 6000), "DATA", 0, 0, message)

        listener_thread = StoppableThread(target=run_listener)
        listener_thread.start()
        send_thread = StoppableThread(target=send_message)
        send_thread.start()
        send_thread1 = StoppableThread(target=send_message)
        time.sleep(5)
        send_thread1.start()
        send_thread1.join()
        send_thread.join()
        size = server.message_queue.qsize()
        server.stop()
        client.stop()
        listener_thread.stop()
        self.assertEqual(size, 1)
