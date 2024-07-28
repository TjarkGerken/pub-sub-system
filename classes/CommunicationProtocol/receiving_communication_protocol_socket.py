import queue
import socket
import threading

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from utils.logger import logger
from utils.utils import calculate_checksum, remove_if_exists


class ReceivingCommunicationProtocolSocket(CommunicationProtocolSocketBase):
    def __init__(self, uid: str, port: int) -> None:
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
        self.__lock = threading.Lock()

        if self.database_file:
            self.init_db()

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

    def handle_message(self, data):
        sdr_addr, sdr_port, rec_addr, rec_port, sq_no, ack_no, checksum, sdr_uid, data = data.decode().split(" | ")
        calculated_checksum = calculate_checksum(data)

        if checksum != calculated_checksum:
            logger.error(f"Checksums of packets do not match. Dropping packet (UID: {sdr_uid} | SQ No.: {sq_no} | ACK No.: {ack_no})")
            logger.debug(f"Checksums do not match: {checksum} != {calculated_checksum} | Data: {data} (UID: {sdr_uid} | SQ No.: {sq_no} | ACK No.: {ack_no})")
            return None  # TODO: Return Error Code

        sdr_port = int(sdr_port)
        rec_port = int(rec_port)
        ack_no = int(ack_no)
        sq_no = int(sq_no)

        if ack_no == 0 and data != "ACK":
            with self.__lock:
                self.stored_checksums[f"{sdr_uid}_{sq_no}"] = checksum
                self.message_queue.put(data)
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

