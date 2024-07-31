import os
import sqlite3
import unittest
import threading
import time
import json

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from classes.subscriber import Subscriber
from utils.delete_files_and_folders import delete_files_and_folders
from utils.logger import logger


class TestMessageBrokerIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Introduces a lock for the test
        :return: None
        """
        cls.__lock = threading.Lock()

    def tearDown(self):
        """
        Perform clean up after test
        :return: None
        """
        delete_files_and_folders()

    def test_mb_reboot_sensor_data_consistency(self):
        """
        Tests if the data stays consistent after the message broker shutdowns and reboots.

        The test creates a message broker and a sensor that sends messages to the message broker, waits for 5 seconds,
        stops the message broker, waits for 5 seconds, starts the message broker, waits for 5 seconds and checks if all
        messages sent by the sensor are in the database

        :return: None
        """
        # Clean up old files and folders and initialize setup
        delete_files_and_folders()

        mb = MessageBroker()
        # Get the current number of messages received by sensor that are already processed
        # This ensures that when we check the final sequence number, we are only considering the messages sent during
        # this test
        offset = mb.sequence_number

        # Create a sensor and generate some artificial data that will be sent to the message broker.
        sensor = Sensor(50001, "U", "BRM", generate=False)
        sensor.generate_sensor_result()
        sensor.generate_sensor_result()

        # Wait for 5 seconds to give the message broker time to process the messages
        time.sleep(5)

        # Stop the message broker and clean up the object
        mb.stop()
        del mb

        # Wait for 5 seconds to give the message broker time to shut down
        time.sleep(10)

        # Generate some more artificial data and try to send it to the message broker while it is down
        sensor.generate_sensor_result()
        sensor.generate_sensor_result()

        # Wait some time to check what how clients handle messages that cannot be sent
        time.sleep(5)

        # Start the message broker again
        mb = MessageBroker()

        # Generate some more artificial data and try to send it to the message broker
        sensor.generate_sensor_result()
        sensor.generate_sensor_result()

        # Wait for 5 seconds to give the message broker time to process all the messages
        time.sleep(10)

        # Stop the sensor and the message broker
        sensor.stop()
        mb.stop()

        # Check if the message broker received all the messages including those that were sent while it was down
        self.assertEqual(6 + offset, mb.sequence_number, "The message broker didn't receive all messages.")

    def test_mb_reboot_subscriber_data_consistency(self):
        """
        Tests if the data stays consistent after the message broker shutdowns and reboots.

        The test creates a message broker, a sensor and a subscriber that sends messages to the message broker, waits for
        5 seconds, stops the message broker, waits for 5 seconds, starts the message broker, waits for 5 seconds and checks
        if all messages sent by the sensor are in the database.

        :return: None
        """
        # Clean up old files and folders
        delete_files_and_folders()

        # Generate some artificial test data
        test_data = [
            {
                "sensor_id": "SENSOR_BRM_U_50001",
                "sensor_type": "U",
                "location": "BRM",
                "datetime": "2021-01-01T01:01:01.00001",
                "uv_index": 1
            },
            {
                "sensor_id": "SENSOR_BRM_U_50001",
                "sensor_type": "U",
                "location": "BRM",
                "datetime": "2022-02-02T02:02:02.00002",
                "uv_index": 2
            },
            {
                "sensor_id": "SENSOR_BRM_U_50001",
                "sensor_type": "U",
                "location": "BRM",
                "datetime": "2023-03-03T03:03:03.00003",
                "uv_index": 3
            },
            {
                "sensor_id": "SENSOR_BRM_U_50001",
                "sensor_type": "U",
                "location": "BRM",
                "datetime": "2024-04-04T04:04:04.00004",
                "uv_index": 4
            },
        ]

        # Create a message broker and a subscriber
        mb = MessageBroker()
        subscriber = Subscriber(5005, "B", log=False, ignore_startup=False)

        # Give the message broker and the subscriber some time to start
        time.sleep(5)

        # Send some test data to the message broker
        test = str(test_data[0])
        mb._sensor_udp_socket.message_queue.put(test)

        # Give the message broker some time to process the message and shut it down
        time.sleep(5)
        mb.stop()

        # Wait until message broker is shut down and insert some more test data to simulate the scenario that sensor
        # messages were received but not distributed to topic queues and therefore not sent to subscribers
        time.sleep(5)
        with self.__lock:
            # Connect to database
            db_connection = sqlite3.connect(mb._database_file)
            db_cursor = db_connection.cursor()

            # Insert sensor data
            db_cursor.execute("INSERT INTO MessageSocketQueue (data) VALUES(?)", (json.dumps(test_data[1]),))
            db_connection.commit()

            # Close database conncetion
            db_cursor.close()
            db_connection.close()

        # Wait a bit so the value is successfully inserted into the database
        time.sleep(10)

        # Start the message broker up again and put a test message in the queue
        mb = MessageBroker()
        mb._sensor_udp_socket.message_queue.put(test)

        # Wait a bit so the message broker can send the message to the subscriber
        time.sleep(5)

        # Stop the message broker and subscriber to free up resources
        subscriber.stop()
        mb.stop()
        time.sleep(5)

        items = []
        # Retrieve all items from the subscriber's message queue to check if all messages were received
        while not subscriber._subscription_udp_socket.message_queue.empty():
            items.append(subscriber._subscription_udp_socket.message_queue.get())

        self.assertEqual(2,
                         len(items),
                         "The subscriber didn't receive all messages.")
