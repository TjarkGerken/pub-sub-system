"""
This module contains the ReceivingCommunicationProtocolSocket class, which is responsible for receiving messages from
the client over a communication protocol socket
"""

import json
import queue
import sqlite3
import threading

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from utils.StoppableThread import StoppableThread
from utils.logger import logger
from utils.utils import calculate_checksum, remove_if_exists


class ReceivingCommunicationProtocolSocket(CommunicationProtocolSocketBase):
    """
    A class to represent a receiving communication protocol socket

    Attributes:
    ----------
    uid : str
        Unique identifier for the instance using the socket (Sensor, MB, Subscriber)
    port : int
        Port number to bind the socket
    database_file : str
        Path to the database file to store messages
    stored_checksums : dict
        Dictionary to store checksums of received packets
    message_queue : queue.Queue
        A queue to store messages to be processed
    __lock : threading.Lock
        Thread lock to ensure thread safety
    """

    def __init__(self, uid: str, port: int, database_file: str = None) -> None:
        """
        Constructor of the CommunicationProtocolSocket class.
        Initializes all the necessary attributes for the CommunicationProtocolSocket object.

        :param uid: Unique identifier for the instance using the socket (Sensor, MB, Subscriber).
        :param port: Port number to bind the socket.
        :param database_file: Path to the database file to store messages.

        :return: None
        """
        # Initialize the base class
        super().__init__(uid, port)

        # Initialize extra attributes
        self.stored_checksums = {}
        self.message_queue = queue.Queue()
        self.database_file = database_file
        self.__lock = threading.Lock()

        # Set a timeout for the socket to gracefully handle the stop event
        self.set_timeout(1)

        if self.database_file:
            self.init_db()

    def init_db(self) -> None:
        """
        Initialize the database by creating the necessary tables if not already present. Finally the queue is prefilled
        with unsent messages.

        :return: None
        """
        # Create the database if it does not exist and connect to it
        db_connection = sqlite3.connect(self.database_file)
        db_cursor = db_connection.cursor()

        # Read DDL statements from predefined file and execute them
        with open("database/ddl_socket.sql", "r") as ddl_file:
            with self.__lock:
                db_cursor.executescript(ddl_file.read())
                db_connection.commit()

        logger.debug(f"Initialized database connection (UID: {self.uid})")

        # Prefill queue with messages that weren't sent yet
        self.prefill_queue(db_connection, db_cursor)

        db_cursor.close()
        db_connection.close()

        return None

    def prefill_queue(self, db_connection: sqlite3.Connection, db_cursor: sqlite3.Cursor) -> None:
        """
        Prefills the message queue with messages that weren't sent yet.

        :param db_connection: The database connection to use for the query
        :param db_cursor: The database cursor to use for the query

        :return: None
        """
        # Lock the database connection and cursor to ensure thread safety
        with self.__lock:
            # Get all messages from the database that haven't been sent yet
            messages_to_send = db_cursor.execute("SELECT * FROM MessageSocketQueue ORDER BY MessageID ASC").fetchall()

        # Add the messages to the queue
        for message in messages_to_send:
            self.message_queue.put(message[1])

        return None

    def listener(self) -> None:
        """
        Listens for incoming messages and starts a new thread to handle each message.
        :return: None
        """
        logger.info(f"Listening for incoming messages... (UID: {self.uid})")

        # While the stop event is not set, listen for incoming messages
        while not self._stop:
            try:
                # Receive a message from the client
                message, addr = self.cp_socket.recvfrom(1024)
                logger.debug(f"Message Received from {addr} (UID: {self.uid})")

                if message:
                    # Start a new thread to handle the message and stop it immediately
                    # No need for infinite threads, as the message is processed only once
                    t = StoppableThread(target=self.handle_message, args=(message,))
                    t.start()
                    t.stop()

            except TimeoutError as e:
                # Timeout is reached while waiting for a message
                # Timeout is important to check if the thread should stop → start listening again
                continue

        logger.info(f"Stopped listening for incoming messages... (UID: {self.uid})")

        return None

    def insert_message_into_db(self, data: str) -> None:
        """
        Inserts a message into the database

        :param data: The message data to be inserted

        :return: None
        """
        # Skip this method if no database is provided
        if self.database_file is None:
            return None

        try:
            # Lock the resources to ensure thread safety
            with self.__lock:
                # Connect to the database
                db_connection = sqlite3.connect(self.database_file)
                db_cursor = db_connection.cursor()

                # Insert the message into the database for persistence
                db_cursor.execute("INSERT INTO MessageSocketQueue (Data) VALUES (?)", (data,))
                db_connection.commit()

                # Close the resources
                db_cursor.close()
                db_connection.close()
        except sqlite3.OperationalError as e:
            logger.error(f"Error while inserting message into database: {e}")

        return None

    def delete_message_from_db(self, data: str) -> None:
        """
        Deletes a message from the database

        :param data: The message to be deleted

        :return: None
        """
        # Set the threadsafety level to 2 to prevent threads hindering each other
        sqlite3.threadsafety = 2
        logger.debug(f"Deleting message from database (UID: {self.uid} | Data: {data})")

        counter = 0
        serialized_data = json.dumps(data)

        # Lock the resources to ensure thread safety
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.database_file, check_same_thread=False)
            db_cursor = db_connection.cursor()

            # Delete the message from the database
            db_cursor.execute("DELETE FROM MessageSocketQueue WHERE Data = ?", (serialized_data,))
            db_connection.commit()

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        return None

    def handle_message(self, data: str) -> None:
        """
        This method handles the received message. If it is a data packet (ACK No. 0), it stores the checksum and sends
        an ACK for the received message. If the packet is an Acknowledgement for the Acknowledgement (ACK No. 2), it
        removes the stored checksum and logs the completion of the communication to avoid duplicate messages.

        :param data: The received message to be handled

        :return: None
        """
        # Deserialize the data and extract the necessary fields
        sdr_addr, sdr_port, rec_addr, rec_port, sq_no, ack_no, checksum, sdr_uid, data = data.decode().split(" | ")

        # Calculate the checksum of the received data
        calculated_checksum = calculate_checksum(data)

        # Compare the calculated checksum with the one received in the packet and don't send an ACK if they don't match
        # This is to ensure that the data is not corrupted → Because no ACK is sent the sender will resend the data
        if checksum != calculated_checksum:
            logger.error(
                f"Checksums of packets do not match. Dropping packet (UID: {sdr_uid} | SQ No. {sq_no} | ACK No. "
                f"{ack_no})")

            logger.debug(
                f"Checksums do not match: {checksum} != {calculated_checksum} | Data: {data} (UID: {sdr_uid} | SQ No. "
                f"{sq_no} | ACK No. {ack_no})")

            return None  # TODO: Return Error Code

        sdr_port = int(sdr_port)
        rec_port = int(rec_port)
        ack_no = int(ack_no)
        sq_no = int(sq_no)

        # Data packet received
        if ack_no == 0 and data != "ACK":
            logger.debug(
                f"{str('Received Message').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: {data})")

            # Store the checksum of the received packet
            self.stored_checksums[f"{sdr_uid}_{sq_no}"] = checksum

            # Insert the message into the queue and database
            self.message_queue.put(data)
            self.insert_message_into_db(data)

            # Send an ACK for the received message
            logger.debug(f"{str('Send Ack').ljust(50)}(UID: {self.uid} | SQ No. {sq_no} | ACK No. 1 | Data: {data})")
            self.send((sdr_addr, sdr_port), "ACK", sq_no, 1, "ACK")

        # ACK for ACK received
        elif ack_no == 2 and data == "ACK" and calculated_checksum in self.stored_checksums:
            logger.debug(f"{str('Received ACK 2.1').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | "
                         f"Data: {data})")
            # Remove the stored checksum because data packet is received successfully and the communication is complete
            self.stored_checksums = remove_if_exists(self.stored_checksums, f"{sdr_uid}_{sq_no}")
            logger.debug(f"{self.uid} | RM CHECKSUM Communication Complete")

        # ACK for ACK received but the stored checksum is not found # TODO: When is this case possible?
        elif ack_no == 2:
            logger.debug(f"{str('Received ACK 2.2').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | "
                         f"Data: {data})")
            logger.debug(f"{self.uid} | Communication Complete")

        else:
            logger.debug(
                f"{str('Duplicate ACK received').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: "
                f"{data})")
            logger.debug(f"{self.uid} | ACK already sent, skipping...")

        return None  # TODO: Return Error Code
