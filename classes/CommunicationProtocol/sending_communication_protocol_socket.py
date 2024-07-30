import queue
import select
import socket
import threading
import time

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from utils.logger import logger
from configuration import RETRY_DURATION_IN_SECONDS, SECONDS_BETWEEN_RETRIES


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
        self.set_timeout(1)

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
            logger.debug(
                f"{str('Send Message to ' + str(address)).ljust(50)}(UID: {self.uid} | Seq No. {self.sequence_number} | ACK No. 0 | Data ka: {data})")
            self.send(address, "DATA", self.sequence_number, 0, data)

            try:
                ack = self.cp_socket.recvfrom(1024)
                if ack[0]:
                    logger.debug(
                        f"{str('ACK received for message').ljust(50)}(UID: {self.uid} | SQ No. {self.sequence_number} | ACK No. 1 | Data ka: {data})")
                    logger.debug(
                        f"{str('Sending ACK for ACK').ljust(50)}(UID: {self.uid} | SQ No. {self.sequence_number} | ACK No. 2 | Data ka: {data})")
                    self.send(address, "ACK", self.sequence_number, 2, "ACK")
                    ack_received = True
                    break
            except ConnectionResetError as e:
                logger.warning(f"Client not reachable, retrying in {SECONDS_BETWEEN_RETRIES} second(s)...")
                logger.debug(f"Connection reset error | {e}")
            except TimeoutError as e:
                logger.debug(f"Timeout error | {e}")
                pass
            except Exception as e:  # TODO: Specify exception
                logger.critical("Error sending ACK No.: 2")

            # Check if the thread should stop
            # To speed things up, check every 0.1 seconds if signal to stop has been received
            sleep_start_time = time.time()
            while time.time() - sleep_start_time < SECONDS_BETWEEN_RETRIES and not self._stop:
                time.sleep(0.1)

            # If the thread should stop, break the loop
            if self._stop:
                break

        # Display error message if no acknowledement was recieved and was not caused by stop signal
        if not ack_received and not self._stop:
            logger.error(
                f"{str('Retries exhausted, message will be dropped').ljust(50)}(UID: {self.uid} | Seq No. {self.sequence_number}) not sent")

        self.sequence_number += 1
        return ack_received
