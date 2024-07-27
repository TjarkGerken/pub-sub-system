import hashlib
import queue
import threading

import select
import socket
import time
from typing import Literal

from configuration import RETRY_DURATION_IN_SECONDS, SECONDS_BETWEEN_RETRIES

"""
connection_string = "127.0.0.1 | 5001 | 127.0.0.1 | 5005 | SQ_NO | ACK_NO | CHECKSUM | SENDER_UID | DATA AS JSON STRING"

Packet Structure: Sender Address | Sender Port | Recipient Address | Recipient Port | Sequence Number | Acknowledgement Number | Checksum | Sender UID | Data

sender_addr, sender_prot, recipient_addr, recipient_port, sq_no, ack_no, checksum, sender_uid, msg = connection_string.split(" | ")
"""


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
        self.stored_checksums = {}
        self.message_queue = queue.Queue()

    def calculate_checksum(self, data: bytes or str) -> str:
        """
        Calculates the SHA-256 checksum of the given data.

        :return:
        :param data: The data to calculate the checksum for.
        :type data: str
        :return: The checksum of the data in hex representation.
        """
        if isinstance(data, str):
            data = data.encode()
        return hashlib.sha256(data).hexdigest()

    def send(self, address: tuple, flag: Literal["DATA", "ACK"], sq_no: int,  ack_number: int = 0, data: str = "") -> None:
        """
        Sends a packet to the specified address.

        :param address: tuple
            The address to send the packet to, in the form (IP, port).
        :param flag: Literal["DATA", "ACK"]
            The type of packet to send, either "DATA" or "ACK".
        :param sq_no: int, optional
            The sequence number for the packet, default is 0.
        :param ack_number: int, optional
            The acknowledgement number for the packet, default is 0.
        :param data: str, optional
            The data to send in the packet, default is an empty string.
        :return: None
        """
        if flag == "ACK":
            data = "ACK"

        if flag == "DATA":
            sq_no = self.sequence_number

        checksum = self.calculate_checksum(data)
        data = f"127.0.0.1 | {self.port} | {address[0]} | {address[1]} | {sq_no} | {ack_number} | {checksum} | {self.uid} | {data}".encode()
        print(data)
        try:
            self.cp_socket.sendto(data, address)
        except Exception as e:
            print(f"Error sending data: {e}")

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
        while time.time() - start_time < RETRY_DURATION_IN_SECONDS:
            self.send(address, "DATA", self.sequence_number, 0, data)
        """            ack = select.select([self.cp_socket], [], [], SECONDS_BETWEEN_RETRIES)
            if ack[0]:
                break
        self.send(address, "ACK", 2)"""

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
        # data = f"127.0.0.1 | {self.port} | {address[0]} | {address[1]} | {self.sequence_number} | {ack_number} | {checksum} | {self.uid} | {data}".encode()

        sdr_addr, sdr_port, rec_addr, rec_port, sq_no, ack_no, checksum, sdr_uid, data = data.decode().split(" | ")
        calculated_checksum = self.calculate_checksum(data)

        if checksum != calculated_checksum:
            raise Exception("Checksums do not match")
            pass

        sdr_port = int(sdr_port)
        rec_port = int(rec_port)
        ack_no = int(ack_no)
        sq_no = int(sq_no)
        print(data, calculated_checksum, self.stored_checksums, ack_no, sq_no)

        if ack_no == 0 and data != "ACK" and calculated_checksum not in self.stored_checksums:
            self.stored_checksums[f"{sdr_uid}_{sq_no}"] = checksum
            self.message_queue.put(data)
            self.send((sdr_addr, sdr_port), "ACK", sq_no, ack_no + 1, "ACK")
            print(f"[INFO] | {self.uid} | Data Stored ACK Send")
        elif ack_no == 1:
            self.sequence_number += 1
            self.send((sdr_addr, sdr_port), "ACK", sq_no, ack_no + 1)
            print(f"[INFO] | {self.uid} | Data Stored ACK  2 Send")
        elif ack_no == 2 and data == "ACK" and calculated_checksum in self.stored_checksums:
            # self.stored_checksums.remove(calculated_checksum)
            print(f"[INFO] | {self.uid} | RM CHECKSUM Communication Complete")
        elif ack_no == 2:
            print(f"[INFO] | {self.uid} | Communication Complete")
        else:
            print(f"[INFO] | {self.uid} | ACK already sent, skipping...")
