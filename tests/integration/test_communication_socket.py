import os
import queue
import sqlite3
import threading
import time
import unittest

from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.StoppableThread import StoppableThread


def database_init():
    """
    Initializes the SQLite database by replacing them with new ones to ensure no data is left from previous runs.
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
    """
    Integration tests for the communication protocol between client and server.
    """
    @classmethod
    def setUpClass(cls):
        """
        Initializes the database and introduces a lock for the test.
        :return:
        """
        database_init()
        cls.__lock = threading.Lock()

    def test_send_data(self):
        """
        Tests the sending of data from the client to the server in a regular scenario.

        This test initializes the client and server, sends two messages from the client to the server,
        and verifies that the messages are received correctly by the server. The test also verifies that the
        client sequence number is incremented correctly and that the messages are stored in the database.
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
            Tests if the client retries sending a message to the server when it is not reachable.

            This test initializes the client and server, sends a message from the client to the server,
            and verifies that the message is received correctly by the server even though the server gets started 10
            seconds after the client.


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
        sq_number = client.sequence_number
        listener_thread.stop()
        client.stop()
        server.stop()
        self.assertEqual(received_message, message, "Expected message to be received by the server")
        self.assertEqual(sq_number, 1, "Expected client sequence number to be 1")


    def test_duplicate_messages(self):
        """
            Tests if the server ignores duplicate messages from the client if no 2nd ACK is returned.
            The communication protocol expects when the 2nd ACK is sent from the client to the server, that the client
            has removed the message from its queue and the message is not send again. Therefore only messages that have
            not yet received a 2nd ACK have to be checked for duplicates.

            This test initializes the client and server, sends the same message twice from the client to the server.
            The server only receives the data and returns a 1st ACK. The client does not respond with a 2nd ACK, but
            sends the same data again. The server should ignore the duplicate message and not store it in the database.

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
        stored_checksums = server.stored_checksums
        server.stop()
        client.stop()
        listener_thread.stop()
        self.assertEqual(size, 1, "Expected 1 message in the server message queue")
        self.assertEqual(len(stored_checksums), 1, "Expected 1 stored checksum in the server")
