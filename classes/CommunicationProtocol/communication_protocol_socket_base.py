"""
This module defines the CommunicationProtocolSocketBase class, which is a base class for the
SendingCommunicationProtocolSocket and ReceivingCommunicationProtocolSocket classes. It provides the basic functionality
for sending and receiving messages over a communication protocol socket.
"""

import time
import socket
import threading
from typing import Literal

from utils.logger import logger
from utils.utils import calculate_checksum


class CommunicationProtocolSocketBase:
    """
    A base class to represent a communication protocol socket.

    Attributes:
    ----------
    uid : str
        Unique identifier for the instance using the socket (Sensor, MB, Subscriber).
    port : int
        Port number to bind the socket.
    cp_socket : socket.socket
        The socket object.
    _stop : bool
        Flag to stop the socket.
    """
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
        self._stop = False

    def set_timeout(self, timeout: int) -> None:
        """
        Sets the timeout for the socket to be able to gracefully handle thread terminations.

        :param timeout: The timeout in seconds.

        :return: None
        """
        self.cp_socket.settimeout(timeout)
        return None

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
        # Prepare the data to send → When sending an ACK there is no real data to be sent
        if flag == "ACK":
            data = "ACK"

        # Calculate the checksum
        checksum = calculate_checksum(data)
        data = (f"127.0.0.1 | {self.port} | {address[0]} | {address[1]} | {sq_no} | {ack_no} | {checksum} | {self.uid} "
                f"| {data}").encode()

        try:
            # Send the data to the specified endpoint
            self.cp_socket.sendto(data, address)
        except Exception as e:  # TODO: Genauere Exception abfangen
            logger.critical(f"Error sending data (UID: {self.uid}) | SQ No.:{sq_no} | ACK No.:{ack_no} ) | Error : {e} | {address}") # TODO: Remove Error E
            logger.debug(f"Error sending data (UID: {self.uid}) | SQ No.:{sq_no} | ACK No.:{ack_no}) | Error: {e})")

        # TODO: Return status (OK or Error)?
        return None

    def stop(self) -> None:
        """
        Sets the stop flag to True to stop the socket.
        :return: None
        """
        self._stop = True
        return None
