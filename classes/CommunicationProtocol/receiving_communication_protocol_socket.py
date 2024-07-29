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
            with self.__lock:
                db_cursor.executescript(ddl_file.read())
                db_connection.commit()
        logger.debug(f"Initialized database connection (UID: {self.uid})")

        # Prefill queue with messages that weren't sent yet
        self.prefill_queue(db_connection, db_cursor)

        db_cursor.close()
        db_connection.close()

    def prefill_queue(self, db_connection, db_cursor):
        with self.__lock:
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
            # logger.debug(f"{str('Message Recieved from ' + str(addr)).ljust(50)}(UID: {self.uid} | Message: {message})")  # TODO: Redundant with log message in while loop down below
            if message:
                threading.Thread(target=self.handle_message, args=(message,)).start()

    def insert_message_into_db(self, data):
        if self.database_file is None:
            return None

        db_connection = sqlite3.connect(self.database_file)
        db_cursor = db_connection.cursor()

        try:
            with self.__lock:
                db_cursor.execute("INSERT INTO MessageSocketQueue (Data) VALUES (?)", (data,))
                db_connection.commit()
        except sqlite3.OperationalError as e:
            logger.error(f"Error while inserting message into database: {e}")

        db_cursor.close()
        db_connection.close()

        return None

    def delete_message_from_db(self, data):
        sqlite3.threadsafety = 2
        db_connection = sqlite3.connect(self.database_file, check_same_thread=False)
        db_cursor = db_connection.cursor()

        logger.debug(f"Deleting message from database (UID: {self.uid} | Data: {data})")
        serialized_data = json.dumps(data)
        counter = 0
        while True:
            with self.__lock:
                items = db_cursor.execute("SELECT * FROM MessageSocketQueue WHERE Data = ?",
                                          (serialized_data,)).fetchall()

            if len(items) < 1:
                break

            if counter > 0:
                logger.critical(
                    f"Message occurred more than once in the database, deleting all affected instances | Counter: {counter}")

            with self.__lock:
                db_cursor.execute("DELETE FROM MessageSocketQueue WHERE Data = ?", (serialized_data,))
                db_connection.commit()

        db_cursor.close()
        db_connection.close()

    def handle_message(self, data):
        sdr_addr, sdr_port, rec_addr, rec_port, sq_no, ack_no, checksum, sdr_uid, data = data.decode().split(" | ")
        calculated_checksum = calculate_checksum(data)

        if checksum != calculated_checksum:
            logger.error(
                f"Checksums of packets do not match. Dropping packet (UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no})")
            logger.debug(
                f"Checksums do not match: {checksum} != {calculated_checksum} | Data: {data} (UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no})")
            return None  # TODO: Return Error Code

        sdr_port = int(sdr_port)
        rec_port = int(rec_port)
        ack_no = int(ack_no)
        sq_no = int(sq_no)

        if ack_no == 0 and data != "ACK":

            logger.debug(
                f"{str('Received Message').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: {data})")

            self.stored_checksums[f"{sdr_uid}_{sq_no}"] = checksum

            self.message_queue.put(data)
            self.insert_message_into_db(data)

            logger.debug(f"{str('Send Ack').ljust(50)}(UID: {self.uid} | SQ No. {sq_no} | ACK No. 1 | Data: {data})")
            self.send((sdr_addr, sdr_port), "ACK", sq_no, 1, "ACK")

        elif ack_no == 2 and data == "ACK" and calculated_checksum in self.stored_checksums:
            logger.debug(
                f"{str('Received ACK 2.1').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: {data})")
            self.stored_checksums = remove_if_exists(self.stored_checksums, f"{sdr_uid}_{sq_no}")
            # logger.debug(f"{self.uid} | RM CHECKSUM Communication Complete")

        elif ack_no == 2:
            logger.debug(
                f"{str('Received ACK 2.2').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: {data})")
            # logger.debug(f"{self.uid} | Communication Complete")

        else:
            logger.debug(
                f"{str('Duplicate ACK received').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: {data})")
            # logger.debug(f"{self.uid} | ACK already sent, skipping...")

        return None  # TODO: Return Error Code
