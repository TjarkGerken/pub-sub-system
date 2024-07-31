import os
import sqlite3
import unittest
import threading
import time
import json

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from classes.subscriber import Subscriber
from utils.logger import logger


class TestMessageBrokerIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.__lock = threading.Lock()

    def test_mb_reboot_sensor_data_consistency(self):
        """
        Tests if the data stays consistent after the message broker shutdowns and reboots.

        The test creates a message broker and a sensor that sends messages to the message broker, waits for 5 seconds,
        stops the message broker, waits for 5 seconds, starts the message broker, waits for 5 seconds and checks if all
        messages sent by the sensor are in the database.

        :return: None
        """
        mb = MessageBroker()
        offset = mb.sequence_number

        sensor = Sensor(50001, "U", "BRM", generate=False)

        sensor.generate_sensor_result()
        sensor.generate_sensor_result()

        time.sleep(5)

        mb.stop()
        del mb

        time.sleep(10)

        sensor.generate_sensor_result()
        sensor.generate_sensor_result()

        time.sleep(5)

        mb = MessageBroker()

        sensor.generate_sensor_result()
        sensor.generate_sensor_result()

        time.sleep(10)

        sensor.stop()
        mb.stop()

        self.assertEqual(6 + offset, mb.sequence_number, "The message broker didn't receive all messages.")

    def test_mb_reboot_subscriber_data_consistency(self):
        """
        Tests if the data stays consistent after the message broker shutdowns and reboots.

        The test creates a message broker, a sensor and a subscriber that sends messages to the message broker, waits for
        5 seconds, stops the message broker, waits for 5 seconds, starts the message broker, waits for 5 seconds and checks
        if all messages sent by the sensor are in the database.

        :return: None
        """
        with self.__lock:
            filenames = [
                "database/message_broker.db",
                "database/message_broker_sub.db",
                "database/SUBSCRIBER_B_5005.db",
                "config/SUBSCRIBER_B_5005.json",
                "config/message_broker.json"
            ]

            # Iterate over the list of filenames and delete each file if it exists
            for filename in filenames:
                if os.path.exists(filename):
                    os.remove(filename)
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

        mb = MessageBroker()
        subscriber = Subscriber(5005, "B", log=False, ignore_startup=False)

        time.sleep(5)

        test = str(test_data[0])
        mb._sensor_udp_socket.message_queue.put(test)

        time.sleep(5)

        subscriber.stop()
        mb.stop()

        time.sleep(10)

        with self.__lock:
            db_connection = sqlite3.connect(mb._database_file)
            db_cursor = db_connection.cursor()
            logger.warning(f"Insert {json.dumps(test_data[1])}")
            db_cursor.execute("INSERT INTO MessageSocketQueue (data) VALUES(?)", (json.dumps(test_data[1]),))
            db_connection.commit()
            db_cursor.close()
            db_connection.close()

        time.sleep(5)

        mb = MessageBroker()

        time.sleep(5)

        subscriber.stop()
        mb.stop()

        time.sleep(5)

        items = []
        logger.warning(subscriber._subscription_udp_socket.message_queue.qsize())
        while not subscriber._subscription_udp_socket.message_queue.empty():
            items.append(subscriber._subscription_udp_socket.message_queue.get())

        logger.warning(items)

        self.assertEqual(2,
                         len(items),
                         "The subscriber didn't receive all messages.")



