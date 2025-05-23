import json
import os
import queue
import sqlite3
import threading
import time
import unittest

from classes.CommunicationProtocol.receiving_communication_protocol_socket import ReceivingCommunicationProtocolSocket
from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from utils.StoppableThread import StoppableThread
from utils.delete_files_and_folders import delete_files_and_folders


class TestCommunicationIntegration(unittest.TestCase):
    """
    Integration tests for the communication protocol between client and server.
    """

    def database_init(self):
        """
        Initializes the database by replacing them with new ones to ensure no data is left from previous runs
        :return: None
        """
        # Create the database if it does not exist and connect to it
        with self.__lock:
            db_path = "database/message_broker.db"
            db_dir = os.path.dirname(db_path)

            # Ensure the directory exists
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)

            # Connect to database
            db_connection = sqlite3.connect(db_path)
            db_cursor = db_connection.cursor()

            # Execute DDL statements to create tables if not already exist
            with open("database/ddl_mb.sql", "r") as ddl_file:
                db_cursor.executescript(ddl_file.read())
                db_connection.commit()

            # Close database connection
            db_cursor.close()
            db_connection.close()

            return None

    @classmethod
    def setUpClass(cls):
        """
        Initializes the database and introduces a lock for the test
        :return: None
        """
        cls.__lock = threading.RLock()
        return None

    def tearDown(self):
        """
        Perform clean up after test
        :return: None
        """
        delete_files_and_folders()
        return None

    def test_send_data(self):
        """
        Tests the sending of data from a client to the server in a regular scenario.

        This test initializes the client and server, sends two messages from the client to the server,
        and verifies that the messages are received correctly by the server. The test also verifies that the
        client sequence number is incremented correctly and that the messages are stored in the database.

        :return: None
        """
        # Clean up old files and folders and initialize setup
        delete_files_and_folders()
        self.database_init()
        client = SendingCommunicationProtocolSocket("SENDING_SOCKET", 5002)
        server = ReceivingCommunicationProtocolSocket("RECEIVING_SOCKET", 6002, "database/message_broker.db")

        # Ensure tables are empty
        with self.__lock:
            db_connection = sqlite3.connect(server.database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("DELETE FROM MessageSocketQueue")
            db_connection.commit()
            db_cursor.close()
            db_connection.close()

        server.message_queue = queue.Queue()

        def run_listener():
            # Start the server listener
            server.listener()

        # Start the listener thread
        listener_thread = StoppableThread(target=run_listener)
        listener_thread.start()

        # Send two messages from the client to the server (here: artificial sensor data)
        client.send_message(
            json.dumps({
                "uv_index": 18,
                "sensor_id": "SENSOR_BRM_U_50001",
                "datetime": "2024-07-31T19:52:41.937366",
                "sensor_type": "U",
                "location": "BRM"
            }),
            ("127.0.0.1", 6002)
        )
        client.send_message(
            json.dumps({
                "uv_index": 18,
                "sensor_id": "SENSOR_BRM_U_50001",
                "dateti,me": "2024-07-31T19:52:41.937366",
                "sensor_type": "U",
                "location": "BRM"
            }),
            ("127.0.0.1", 6002)
        )

        # Allow some time for the messages to be processed
        time.sleep(2)

        # Verify that the messages are received and stored correctly
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(server.database_file)
            db_cursor = db_connection.cursor()

            # Retrieve messages from the database that have to be processed later on
            db_cursor.execute("SELECT * FROM MessageSocketQueue")
            messages = db_cursor.fetchall()

            # Close database connection
            db_cursor.close()
            db_connection.close()

        # Retrieve server and client stats to compare with expected values
        smq_size = server.message_queue.qsize()
        checksums = server.stored_checksums
        sq = client.sequence_number
        cmq_size = client.message_queue.qsize()

        # Stop the server and client to free up resources
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
        # Clean up old files and folders and initialize setup
        delete_files_and_folders()
        self.database_init()
        client = SendingCommunicationProtocolSocket("SENDING_SOCKET", 50044)
        server = ReceivingCommunicationProtocolSocket("RECEIVING_SOCKET", 60044, "database/message_broker.db")

        server.message_queue = queue.Queue()
        message = "Test message"

        def run_listener():
            server.listener()

        def send_message():
            client.send_message(message, ("127.0.0.1", 60044))

        # Start the listener to be able to receive messages
        listener_thread = StoppableThread(target=run_listener)
        send_thread = StoppableThread(target=send_message)
        listener_thread.start()

        # Wait 10 seconds to let the listener socket be ready to receive messages and then send the message
        time.sleep(10)
        send_thread.start()

        # Wait 5 seconds to let the message be processed by the server
        time.sleep(5)
        received_message = None
        # To prevent the test from hanging, check if the message queue is empty
        if not server.message_queue.empty():
            received_message = server.message_queue.get()
        else:
            pass

        # Retrieve sequence number (counter for packages sent) from the client to compare with expected value
        sq_number = client.sequence_number

        # Stop the server and client to free up resources
        time.sleep(5)
        client.stop()
        server.stop()
        listener_thread.stop()
        send_thread.stop()

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

        :return: None
        """
        # Clean up old files and folders and initialize setup
        delete_files_and_folders()
        self.database_init()

        client = SendingCommunicationProtocolSocket("SENDING_SOCKET", 50400)
        server = ReceivingCommunicationProtocolSocket("RECEIVING_SOCKET", 60400, "database/message_broker.db")

        server.message_queue = queue.Queue()
        message = "Test message"

        def run_listener():
            server.listener()

        def send_message():
            client.send(("127.0.0.1", 60400), "DATA", 0, 0, message)

        # Start the listener to be able to receive messages
        listener_thread = StoppableThread(target=run_listener)
        listener_thread.start()
        # Send the same message twice from the client to the server but with a delay in between
        send_thread = StoppableThread(target=send_message)
        send_thread.start()
        send_thread1 = StoppableThread(target=send_message)
        time.sleep(5)
        send_thread1.start()

        # Wait until both transmissions have been completed
        send_thread1.join()
        send_thread.join()

        # Retrieve stats of server to compare with expected values
        size = server.message_queue.qsize()
        stored_checksums = server.stored_checksums

        # Stop the server and client to free up resources
        server.stop()
        client.stop()
        listener_thread.stop()

        self.assertEqual(size, 1, "Expected 1 message in the server message queue")
        self.assertEqual(len(stored_checksums), 1, "Expected 1 stored checksum in the server")
