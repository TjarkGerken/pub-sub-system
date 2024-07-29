import json
import queue
import socket
import sqlite3
import threading
import time

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from classes.Database import Database, DatabaseTask
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
            self.db = Database(self.database_file)
            self.init_db()

    def init_db(self):
        with open("database/ddl_socket.sql", "r") as ddl_file:
            task = DatabaseTask(op="script",
                                sql=ddl_file.read())
            self.db.add_task(task)

        while not task.is_ready:
            time.sleep(0.1)
        logger.debug(f"Initialized database connection (UID: {self.uid})")

        logger.debug(f"Prefill queue from database (UID: {self.uid})")
        self.prefill_queue()

        return None

    def prefill_queue(self):
        task = DatabaseTask(op="single",
                            sql="SELECT * FROM MessageSocketQueue ORDER BY MessageID",
                            args=None,
                            return_result=True)
        self.db.add_task(task)

        while not task.is_ready:
            time.sleep(0.1)

        messages_to_send = task.result
        for message in messages_to_send:
            self.message_queue.put(message[1])

        return None

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
        """
        Inserts a message into the SQLite database.

        :param data: The message to insert into the database.
        :return: None
        """
        if self.database_file is None:
            return None

        task = DatabaseTask(op="single", sql="INSERT INTO MessageSocketQueue (Data) VALUES (?)", args=(data,))
        self.db.add_task(task)

        while not task.is_ready:
            time.sleep(0.1)

        return None


    def delete_message_from_db(self, data):
        """
        Deletes a message from the SQLite database.

        :param data: The message to delete from the database.
        :return: None
        """
        if self.database_file is None:
            return None

        serialized_data = json.dumps(data)

        task = DatabaseTask(op="single", sql="DELETE FROM MessageSocketQueue WHERE Data = ?", args=(serialized_data,))
        self.db.add_task(task)

        while not task.is_ready:
            time.sleep(0.1)

        return None


    def handle_message(self, data):
        """
        Handles the incoming message by checking the checksum, storing the message in the queue and database, and sending an ACK.

        :param data: The incoming message.
        :return: None
        """
        sdr_addr, sdr_port, rec_addr, rec_port, sq_no, ack_no, checksum, sdr_uid, data = data.decode().split(" | ")
        calculated_checksum = calculate_checksum(data)

        if checksum != calculated_checksum:
            logger.error(
                f"Checksums of packets do not match. Dropping packet (UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no})")
            logger.debug(
                f"Checksums do not match: {checksum} != {calculated_checksum} | Data: {data} (UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no})")
            return None

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

        elif ack_no == 2:
            logger.debug(
                f"{str('Received ACK 2.2').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: {data})")
            # logger.debug(f"{self.uid} | Communication Complete")

        else:
            logger.debug(
                f"{str('Duplicate ACK received').ljust(50)}(UID: {sdr_uid} | SQ No. {sq_no} | ACK No. {ack_no} | Data: {data})")
            # logger.debug(f"{self.uid} | ACK already sent, skipping...")

        return None  # TODO: Return Error Code
