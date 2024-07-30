import queue
import sqlite3
import threading
import time
import unittest

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from utils.logger import logger


class TestCommunicationIntegration(unittest.TestCase):
    """
    Integration tests for the communication protocol between client and server.
    """

    @classmethod
    def setUpClass(cls):
        """
        Introduces a lock for the test.
        :return: None
        """
        #cls.sensor = Sensor(5000, "B", "BRM")
        cls.__lock = threading.Lock()

    def test_reboot_consistent_data(self):
        """

        :return: None
        """
        sensor = Sensor(5000, "U", "BRM")

        time.sleep(5)

        with self.__lock:
            sensor.generate = False
            db_connection = sqlite3.connect(sensor.database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM MessagesToSend")
            messages = db_cursor.fetchall()
            sensor_message_queue = sensor._sensor_results
            db_cursor.close()
            db_connection.close()
            sensor.stop()

        time.sleep(5)
        sensor = Sensor(5000, "U", "BRM", False)
        time.sleep(1)

        with self.__lock:
            db_connection = sqlite3.connect(sensor.database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM MessagesToSend")
            messages_after_reboot = db_cursor.fetchall()
            sensor_message_queue_after_reboot = sensor._sensor_results
            db_cursor.close()
            db_connection.close()
            sensor.stop()

        self.assertEqual(messages, messages_after_reboot, "The messages in the database are not the same after reboot.")
        # + 1 because one element has been taken out of the queue for sending and is added back to the queue after the reboot
        self.assertEqual(sensor_message_queue.qsize() + 1, sensor_message_queue_after_reboot.qsize(), "The messages in the queue are not the same after reboot.")

    def send_message_to_mb(self):
        """
        Tests the sending of a message to the message broker.
        :return: None
        """
        mb = MessageBroker()
        sensor = Sensor(5000, "U", "BRM",generate=False)
        time.sleep(1)
