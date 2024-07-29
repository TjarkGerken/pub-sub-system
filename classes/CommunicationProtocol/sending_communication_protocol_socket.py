import queue
import select
import socket
import threading
import time

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from utils.logger import logger
from configuration import RETRY_DURATION_IN_SECONDS, SECONDS_BETWEEN_RETRIES

"""
connection_string = "127.0.0.1 | 5001 | 127.0.0.1 | 5005 | SQ_NO | ACK_NO | CHECKSUM | SENDER_UID | DATA AS JSON STRING"

Packet Structure: Sender Address | Sender Port | Recipient Address | Recipient Port | Sequence Number | Acknowledgement Number | Checksum | Sender UID | Data

sender_addr, sender_prot, recipient_addr, recipient_port, sq_no, ack_no, checksum, sender_uid, msg = connection_string.split(" | ")
"""


class SendingCommunicationProtocolSocket(CommunicationProtocolSocketBase):
    """
    A class to represent a communication protocol socket using UDP.

    Attributes:
        uid : str
            Unique identifier for the instance using the socket (Sensor, MB, Subscriber)
        port : int
            Port number to bind the socket
        cp_socket : socket.socket
            The socket object
        sequence_number : int
            The sequence number for the packets.
    """

    def __init__(self, uid: str, port: int) -> None:
        """
        Constructor of the CommunicationProtocolSocket class.
        Constructs and initializes all the necessary attributes for the CommunicationProtocolSocket object.

        :param uid: Unique identifier for the instance using the socket (Sensor, MB, Subscriber).
        :param port: Port number to bind the socket.
        :return: None
        """

        super().__init__(uid, port)
        self.sequence_number = 0
        self.message_queue = queue.Queue()
        self.__lock = threading.Lock()

    """
    ack_event = threading.Event()

    def listen_for_ack():
        while not ack_event.is_set():
            try:
                ack = self.cp_socket.recvfrom(1024)
                if ack[0]:
                    ack_event.set()
            except socket.timeout:
                continue"""

    def send_message(self, data, address):
        """
        Sends a message to the specified address and waits for an acknowledgement.

        :param data: str
            The data to send in the message.
        :param address: tuple
            The address to send the message to, in the form (IP, port).
        :return: None
        """
        start_time = time.time()
        ack_received = False

        while time.time() - start_time < RETRY_DURATION_IN_SECONDS and not ack_received:
            logger.debug(f"{str('Send Message to ' + str(address)).ljust(50)}(UID: {self.uid} | Seq No. {self.sequence_number} | ACK No. 0 | Data ka: {data})")
            self.send(address, "DATA", self.sequence_number, 0, data)

            ready = select.select([self.cp_socket], [], [], SECONDS_BETWEEN_RETRIES)
            if ready[0]:
                try:
                    ack = self.cp_socket.recvfrom(1024)
                    if ack[0]:
                        logger.debug(f"{str('ACK received for message').ljust(50)}(UID: {self.uid} | SQ No. {self.sequence_number} | ACK No. 1 | Data ka: {data})")
                        logger.debug(f"{str('Sending ACK for ACK').ljust(50)}(UID: {self.uid} | SQ No. {self.sequence_number} | ACK No. 2 | Data ka: {data})")
                        self.send(address, "ACK", self.sequence_number, 2, "ACK")
                        ack_received = True
                except ConnectionResetError as e:
                    logger.critical("Connection reset error")
                except Exception as e:  # TODO: Specify exception
                    logger.critical("Error sending ACK_NO:2")

        if not ack_received:
            logger.error(f"{str('Retries exhausted, message').ljust(50)}(UID: {self.uid} | Seq No. {self.sequence_number}) not sent")

        self.sequence_number += 1
        return None
