import time
import socket
import threading
from typing import Literal

from utils.logger import logger
from utils.logger2 import logger2
from utils.utils import calculate_checksum


class CommunicationProtocolSocketBase:
    def __init__(self, uid: str, port: int) -> None:
        """
        Constructor of the CommunicationProtocolSocket class.
        Constructs and initializes all the necessary attributes for the CommunicationProtocolSocket object.

        :param uid: Unique identifier for the instance using the socket (Sensor, MB, Subscriber).
        :param port: Port number to bind the socket.
        :return: None
        """
        self.uid = uid
        self.port = port
        self.cp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.cp_socket.bind(("127.0.0.1", self.port))

    def send(self, address: tuple, flag: Literal["DATA", "ACK"], sq_no: int, ack_no: int = 0, data: str = "") -> None:
        """
        Sends a packet to the specified address.

        :param address: tuple
            The address to send the packet to, in the form (IP, port).
        :param flag: Literal["DATA", "ACK"]
            The type of packet to send, either "DATA" or "ACK".
        :param sq_no: int, optional
            The sequence number for the packet, default is 0.
        :param ack_no: int, optional
            The acknowledgement number for the packet, default is 0.
        :param data: str, optional
            The data to send in the packet, default is an empty string.
        :return: None
        """
        if flag == "ACK":
            data = "ACK"

        checksum = calculate_checksum(data)
        data = f"127.0.0.1 | {self.port} | {address[0]} | {address[1]} | {sq_no} | {ack_no} | {checksum} | {self.uid} | {data}".encode()
        logger2.debug(f"{time.time()},{self.uid},{address[0]}:{address[1]},{sq_no},{ack_no}")

        try:
            self.cp_socket.sendto(data, address)
            logger.debug(f"{self.uid} | Data Sent to {address}")
        except Exception as e:
            logger.debug(f"Error sending data: {e}")
