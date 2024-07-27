import hashlib
import threading

import select
import socket
import time
from typing import Literal

from configuration import RETRY_DURATION_IN_SECONDS, SECONDS_BETWEEN_RETRIES


class CommunicationProtocolSocket:
    """
    A class to represent a communication protocol socket using UDP.

    Attributes:
        uid : str
            Unique identifier for the instance using the socket (Sensor, MB, Subscriber).
        port : int
            Port number to bind the socket.
        cp_socket : socket.socket
            The socket object.
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
        self.uid = uid
        self.port = port
        self.cp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.cp_socket.bind(("127.0.0.1", self.port))
        self.sequence_number = 0

    def calculate_checksum(self, data: str) -> str:
        """
        Calculates the SHA-256 checksum of the given data.
        ---
        :param data str: The data to calculate the checksum for.
        :return: The checksum of the data in hex representation.
        """
        return hashlib.sha256(data.encode()).hexdigest()

    def send(self, address: tuple, flag: Literal["DATA", "ACK"], ack_number: int = 0, data: str = ""):
        """
        Sends a packet to the specified address.

        :param ack_number:
        :param data:
        :param address:
        :param flag: DATA OR ACK
        :return:
        """
        """
        connection_string = "127.0.0.1 | 5001 | 127.0.0.1 | 5005 | SQ_NO | ACK_NO | CHECKSUM | SENDER_UID | DATA AS JSON STRING"
        
        Packet Structure: Sender Address | Sender Port | Recipient Address | Recipient Port | Sequence Number | Acknowledgement Number | Checksum | Sender UID | Data
        
        sender_addr, sender_prot, recipient_addr, recipient_port, sq_no, ack_no, checksum, sender_uid, msg = connection_string.split(" | ")
        """
        if flag == "ACK":
            data = "ACK"

        checksum = self.calculate_checksum(data)
        data = f"127.0.0.1 | {self.port} | {address[0]} | {address[1]} | {self.sequence_number} | {ack_number} | {checksum} | {self.uid} | {data}".encode()

        try:
            self.cp_socket.sendto(data, address)
        except Exception as e:
            print(f"Error sending data: {e}")

    def send_message(self, data, address):
        # Send data
        start_time = time.time()
        while time.time() - start_time < RETRY_DURATION_IN_SECONDS:
            self.send(address, "DATA", 0, data)
            ack = select.select([self.cp_socket], [], [], SECONDS_BETWEEN_RETRIES)
            if ack[0] and ack[0].decode() == "ACK":
                break
        self.send(address, "ACK", 2)
        self.sequence_number += 1

    def listener(self):
        while True:
            message = self.cp_socket.recvfrom(1024)
            if message:
                threading.Thread(target=self.handle_message, args=(data)).start()

    def handle_massage(self, data):
        pass

