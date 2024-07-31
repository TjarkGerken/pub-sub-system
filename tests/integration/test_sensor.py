import queue
import sqlite3
import threading
import time
import unittest

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from utils.delete_files_and_folders import delete_files_and_folders
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
        cls.__lock = threading.Lock()


    def tearDown(self):
        """
        Deletes the database after the tests are done.
        :return:
        """
        delete_files_and_folders()

    def test_reboot_consistent_data(self):
        """
        Tests if the data stays consistent after the sensor shutdowns and reboots.

        The test creates a sensor, waits for 5 seconds, stops the sensor, reads the messages from the database and the
        message queue, then creates a new sensor, waits for 5 seconds, reads the messages from the database and the messages
        from the message queue. The messages from the database and the message queue should be the same before and after
        the reboot.

        :return: None
        """
        delete_files_and_folders()

        sensor = Sensor(50001, "U", "BRM")

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
        sensor = Sensor(50001, "U", "BRM", False)
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

        time.sleep(5)

        self.assertEqual(messages, messages_after_reboot, "The messages in the database are not the same after reboot.")
        self.assertEqual(sensor_message_queue.qsize(), sensor_message_queue_after_reboot.qsize(),
                         "The messages in the queue are not the same after reboot.")

    def clear_tables(self, db_file):
        with self.__lock:
            db_connection = sqlite3.connect(db_file)
            db_cursor = db_connection.cursor()
            try:
                db_cursor.execute("DELETE FROM MessagesToSend")
                db_cursor.execute("DELETE FROM MessageSocketQueue")
                db_cursor.execute("DELETE FROM Checksums")
            except sqlite3.OperationalError as e:
                logger.critical(f"Error in clearing tables: {e}")
            db_connection.commit()
            db_cursor.close()
            db_connection.close()

    def test_send_message_to_mb(self):
        """
        Tests the sending of a message to the message broker.
        :return: None
        """
        delete_files_and_folders()

        self.clear_tables("database/message_broker.db")
        mb = MessageBroker()
        offset = mb.sequence_number
        messages = []
        sensor = Sensor(60101, "U", "BRM", generate=False)
        sensor2 = Sensor(60102, "S", "FFM", generate=False)

        sensor.generate_sensor_result()  # 1
        sensor2.generate_sensor_result()  # 2

        #time.sleep(1)

        mb.stop()
        del mb

        sensor.generate_sensor_result()  # 3

        time.sleep(2)

        mb = MessageBroker()
        sensor.generate_sensor_result()  # 4

        time.sleep(5)
        #sensor2.generate_sensor_result()  # 5

        time.sleep(10)
        sq_no = mb.sequence_number

        logger.warning(mb._sensor_udp_socket.stored_results)
        logger.warning(sensor._cp_socket.completed_results)
        logger.warning(sensor2._cp_socket.completed_results)

        mb.stop()
        sensor.stop()
        sensor2.stop()

        time.sleep(5)

        self.assertEqual(4 + offset, sq_no)

    # def test_s(self):
