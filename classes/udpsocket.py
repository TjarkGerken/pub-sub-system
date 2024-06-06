import select
import socket
import threading
import time

from configuration import RETRY_DURATION_IN_SECONDS, SECONDS_BETWEEN_RETRIES
from utils.logger import logger

class UdpSocket:
    def __init__(self, port, uid):
        self.__upd_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__upd_socket.bind(("127.0.0.1", port))
        self.uid = uid

    def three_way_send(self, message, addr):
        start_time = time.time()
        while True:
            logger.debug(f"{self.uid} | Send message to {addr}")
            self.__upd_socket.sendto(message.encode(), addr)
            ready = select.select([self.__upd_socket], [], [], SECONDS_BETWEEN_RETRIES)
            if ready[0]:
                data, addr = self.__upd_socket.recvfrom(1024)
                if data.decode() == "ACK":
                    logger.debug(f"{self.uid} | Received ACK from {addr}")
                    logger.debug(f"{self.uid} | Send ACK to {addr}")
                    self.__upd_socket.sendto("ACK".encode(), addr)
                    return data.decode(), addr
            elif time.time() - start_time > RETRY_DURATION_IN_SECONDS:
                logger.debug(f"{self.uid} | Response timeout")
                logger.error(f"{self.uid} | Failed to receive ACK from {addr}") # TODO: Error message
                break

    def listen(self):
        data, addr = self.__upd_socket.recvfrom(1024)
        threading.Thread(target=self.handle_message, args=(data, addr)).start()

    def handle_message(self, data, addr):
        logger.debug(f"{self.uid} | Received message from {addr}")
        if data and not data.decode() == "ACK":
            self.__upd_socket.sendto("ACK".encode(), addr)
            logger.debug(f"{self.uid} | Send ACK to {addr}") 

            ready = select.select([self.__upd_socket], [], [], SECONDS_BETWEEN_RETRIES)
            if ready[0]:
                ack, addr = self.__upd_socket.recvfrom(1024)
                if ack.decode() == "ACK":
                    logger.debug(f"{self.uid} | Received ACK from {addr} | Communication completed")
                    logger.debug((data.decode(), addr)) # TODO: Add identifier (self.uid) to message?
                    if data and addr:
                        return data.decode(), addr
            else:
                logger.error(f"{self.uid} | Failed to receive ACK from {addr}")
                return None