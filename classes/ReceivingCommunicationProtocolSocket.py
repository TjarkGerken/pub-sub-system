import queue
import socket
import threading

from classes.CommunicationProtocolSocketBase import CommunicationProtocolSocketBase
from classes.utils import calculate_checksum
from utils.logger import logger


class ReceivingCommunicationProtocolSocket(CommunicationProtocolSocketBase):
    def __init__(self, uid: str, port: int) -> None:
        """
        Constructor of the CommunicationProtocolSocket class.
        Constructs and initializes all the necessary attributes for the CommunicationProtocolSocket object.

        :param uid: Unique identifier for the instance using the socket (Sensor, MB, Subscriber).
        :param port: Port number to bind the socket.
        :return: None
        """
        super().__init__(uid, port)
        self.stored_checksums = {}
        self.message_queue = queue.Queue()
        self.__lock = threading.Lock()

    def listener(self):
        """
        Listens for incoming messages and starts a new thread to handle each message.

        :return: None
        """
        while True:
            message, addr = self.cp_socket.recvfrom(1024)
            if message:
                threading.Thread(target=self.handle_message, args=(message,)).start()

    def handle_message(self, data):
        sdr_addr, sdr_port, rec_addr, rec_port, sq_no, ack_no, checksum, sdr_uid, data = data.decode().split(" | ")
        calculated_checksum = calculate_checksum(data)

        if checksum != calculated_checksum:
            logger.error(f"{self.uid} | Communication Complete")
            return

        sdr_port = int(sdr_port)
        rec_port = int(rec_port)
        ack_no = int(ack_no)
        sq_no = int(sq_no)

        if ack_no == 0 and data != "ACK":
            with self.__lock:
                self.stored_checksums[f"{sdr_uid}_{sq_no}"] = checksum
                self.message_queue.put(data)
                self.send((sdr_addr, sdr_port), "ACK", sq_no, 1, "ACK")
                logger.debug(f"{self.uid} | Data Received ACK Send")

        elif ack_no == 2 and data == "ACK" and calculated_checksum in self.stored_checksums:
            # self.stored_checksums.remove(calculated_checksum)
            logger.debug(f"{self.uid} | RM CHECKSUM Communication Complete")
        elif ack_no == 2:
            logger.debug(f"{self.uid} | Communication Complete")
        else:
            logger.debug(f"{self.uid} | ACK already sent, skipping...")

