import json
import queue
import socket
import sqlite3
import threading

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from utils.logger import logger
from utils.utils import calculate_checksum, remove_if_exists


class ReceivingCommunicationProtocolSocket(CommunicationProtocolSocketBase):
    def __init__(self, uid: str, port: int, database_file: str = None) -> None:
        """
        Constructor of the CommunicationProtocolSocket class.
        Constructs and initializes all the necessary attributes for the CommunicationProtocolSocket object.

        :param uid: Unique identifier for the instance using the socket (Sensor, MB, Subscriber).
        :param port: Port number to bind the socket.
        :param database_file: Path to the SQLite database file to store messages.
        :return: None
        """
        super().__init__(uid, port)
        self.stored_checksums = {}
        self.message_queue = queue.Queue()
        self.database_file = database_file
        self.__lock = threading.Lock()

        if self.database_file:
            self.init_db()

    def init_db(self):
        # Create the database if it does not exist and connect to it
        db_connection = sqlite3.connect(self.database_file)
        db_cursor = db_connection.cursor()

        # Execute DDL script to create tables if not already exist
        with open("database/ddl_socket.sql", "r") as ddl_file:
            db_cursor.executescript(ddl_file.read())
            db_connection.commit()
        logger.debug(f"Initialized database connection (UID: {self.uid})")

        # Prefill queue with messages that weren't sent yet
        self.prefill_queue(db_connection, db_cursor)

        db_cursor.close()
        db_connection.close()

    def prefill_queue(self, db_connection, db_cursor):
        messages_to_send = db_cursor.execute("SELECT * FROM MessageSocketQueue ORDER BY MessageID ASC").fetchall()
        for message in messages_to_send:
            self.message_queue.put(message[1])

    def listener(self):
        """
        Listens for incoming messages and starts a new thread to handle each message.

        :return: None
        """
        logger.info(f"Listening for incoming messages... (UID: {self.uid})")
        while True:
            message, addr = self.cp_socket.recvfrom(1024)
            logger.debug(f"Message Received from {addr} (UID: {self.uid})")
            if message:
                threading.Thread(target=self.handle_message, args=(message,)).start()

    def insert_message_into_db(self, data):
        if self.database_file is None:
            return None

        db_connection = sqlite3.connect(self.database_file)
        db_cursor = db_connection.cursor()

        with open("logs/write-logs.csv", "a") as f:
            f.write(f"{data}\n")

        try:
            db_cursor.execute("INSERT INTO MessageSocketQueue (Data) VALUES (?)", (data,))
            db_connection.commit()
        except sqlite3.OperationalError as e:
            logger.error(f"Error while inserting message into database: {e}")

        return None

    def delete_message_from_db(self, data):
        db_connection = sqlite3.connect(self.database_file)
        db_cursor = db_connection.cursor()

        try:
            db_cursor.execute("DELETE FROM MessageSocketQueue WHERE Data = ?", (json.dumps(data),))
            db_connection.commit()
        except sqlite3.OperationalError as e:
            logger.error(f"Error while deleting message from database: {e}")



    def handle_message(self, data):
        sdr_addr, sdr_port, rec_addr, rec_port, sq_no, ack_no, checksum, sdr_uid, data = data.decode().split(" | ")
        calculated_checksum = calculate_checksum(data)

        if checksum != calculated_checksum:
            logger.error(
                f"Checksums of packets do not match. Dropping packet (UID: {sdr_uid} | SQ No.: {sq_no} | ACK No.: {ack_no})")
            logger.debug(
                f"Checksums do not match: {checksum} != {calculated_checksum} | Data: {data} (UID: {sdr_uid} | SQ No.: {sq_no} | ACK No.: {ack_no})")
            return None  # TODO: Return Error Code

        sdr_port = int(sdr_port)
        rec_port = int(rec_port)
        ack_no = int(ack_no)
        sq_no = int(sq_no)

        if ack_no == 0 and data != "ACK":
            with self.__lock:
                self.stored_checksums[f"{sdr_uid}_{sq_no}"] = checksum
                self.message_queue.put(data)
                self.insert_message_into_db(data)
                self.send((sdr_addr, sdr_port), "ACK", sq_no, 1, "ACK")
                logger.debug(f"Data Received ACK Send (UID: {self.uid} | SQ No.: {sq_no} | ACK No.: 1)")
        elif ack_no == 2 and data == "ACK" and calculated_checksum in self.stored_checksums:
            self.stored_checksums = remove_if_exists(self.stored_checksums, f"{sdr_uid}_{sq_no}")
            logger.debug(f"{self.uid} | RM CHECKSUM Communication Complete")
        elif ack_no == 2:
            logger.debug(f"{self.uid} | Communication Complete")
        else:
            logger.debug(f"{self.uid} | ACK already sent, skipping...")

        return None  # TODO: Return Error Code
