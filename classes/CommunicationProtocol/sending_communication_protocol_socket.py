import queue
import threading
import time

from classes.CommunicationProtocol.communication_protocol_socket_base import CommunicationProtocolSocketBase
from configuration import RETRY_DURATION_IN_SECONDS, SECONDS_BETWEEN_RETRIES
from utils.logger import logger


class SendingCommunicationProtocolSocket(CommunicationProtocolSocketBase):
    """
    This class is responsible for sending messages to the client over a communication protocol socket with retry logic.

    Attributes:
    ----------
    uid : str
        Unique identifier of the client
    port : int
        Port number of the client
    sequence_number : int
        Sequence number of the packet sent
    message_queue : queue.Queue
        A Queue to store messages to be sent
    __lock : threading.Lock
        Thread lock to ensure thread safety
    """

    def __init__(self, uid: str, port: int) -> None:
        """
        Constructor for the SendingCommunicationProtocolSocket class. It sets up the sending socket with a unique
        identifier and port, initializes the sequence number to 0 (assuming that the first packet sent has sequence
        number 0), and creates an empty message queue.

        :param uid: The unique identifier for the socket
        :param port: The port number for the socket to bind to and send messages to

        :return: None
        """
        # Initialize the base class
        super().__init__(uid, port)

        # Initialize extra attributes
        self.completed_results = []
        self.sequence_number = 0
        self.message_queue = queue.Queue()
        self.__lock = threading.Lock()
        self.set_timeout(1)

    def send_message(self, data, address) -> bool:
        """
        This method sends a message to the client over the communication protocol socket. It sends the message and waits
        for an ACK for RETRY_DURATION_IN_SECONDS seconds. If no ACK is received, it retries sending the message every
        SECONDS_BETWEEN_RETRIES seconds. If the message is sent successfully, it increments the sequence number for the
        next message to differentiate it from the current message.

        :param data: The data to be sent to the client
        :param address: The address of the client to send the message to
        :return: True if the message was sent successfully and an ACK was received, otherwise False
        """
        ack_received = False
        start_time = time.time()

        # Try to send the message and wait for an ACK for RETRY_DURATION_IN_SECONDS seconds. If no ACK is received,
        # retry sending the message every SECONDS_BETWEEN_RETRIES seconds.
        while time.time() - start_time < RETRY_DURATION_IN_SECONDS and not ack_received:
            logger.debug(
                f"{str('Send Message to ' + str(address)).ljust(50)}(UID: {self.uid} | SQ No. {self.sequence_number} |"
                f" ACK No. 0 | Data ka: {data})")

            # Send the message to the client
            self.send(address, "DATA", self.sequence_number, 0, data)

            try:
                # Wait for an ACK from the client
                ack = self.cp_socket.recvfrom(1024)
                if ack[0]:
                    logger.debug(
                        f"{str('ACK received for message').ljust(50)}(UID: {self.uid} | SQ No. {self.sequence_number} |"
                        f" ACK No. 1 | Data ka: {data})")
                    logger.debug(
                        f"{str('Sending ACK for ACK').ljust(50)}(UID: {self.uid} | SQ No. {self.sequence_number} | "
                        f"ACK No. 2 | Data ka: {data})")

                    # Send an ACK for the ACK received
                    self.completed_results.append(f"{self.uid} {self.sequence_number}")
                    self.send(address, "ACK", self.sequence_number, 2, "ACK")
                    ack_received = True
                    break
            except ConnectionResetError as e:
                logger.warning(f"Client not reachable, retrying in {SECONDS_BETWEEN_RETRIES} second(s)...")
                logger.debug(f"Connection reset error | {e}")
            except TimeoutError as e:
                # TODO: Comment
                logger.debug(f"Timeout error | {e}")
                pass
            except Exception as e:  # TODO: Specify exception
                logger.critical("Error sending ACK No. 2")
                logger.debug(f"Error sending ACK No. 2: {e}")

            # Check if the thread should stop
            # To speed things up, check every 0.1 seconds if signal to stop has been received
            sleep_start_time = time.time()
            while time.time() - sleep_start_time < SECONDS_BETWEEN_RETRIES and not self._stop:
                time.sleep(0.1)

            # If the thread should stop, break the loop
            if self._stop:
                break

        # Display error message if no acknowledgement was received and it was not caused by stop signal
        if not ack_received and not self._stop:
            logger.error(
                f"{str('Retries exhausted, message will be dropped').ljust(50)}(UID: {self.uid} | Seq No. {self.sequence_number}) not sent")

        # Increment the sequence number for the next message to differentiate it from the current message
        self.sequence_number += 1

        return ack_received
