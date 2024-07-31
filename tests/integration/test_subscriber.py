import os
import sqlite3
import threading
import time
import unittest

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from classes.subscriber import Subscriber


class TestSubscriberIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.__lock = threading.Lock()

    def test_data_consistency_on_reboot(self):
        """
        Tests if the data stays consistent after the subscriber shutdowns and reboots.

        The test creates a subscriber, waits for 5 seconds, stops the subscriber, reads the messages from the database and the
        message queue, then creates a new subscriber, waits for 5 seconds, reads the messages from the database and the messages
        from the message queue. The messages from the database and the message queue should be the same before and after
        the reboot.

        :return: None
        """
        mb = MessageBroker()
        subscriber = Subscriber(50005, "U")
        sensor = Sensor(50004, "U", "BRM")

        time.sleep(5)

        with self.__lock:
            db_connection = sqlite3.connect(subscriber._database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM MessageSocketQueue")
            messages = db_cursor.fetchall()
            subscriber_message_queue = subscriber._subscription_udp_socket.message_queue
            db_cursor.close()
            db_connection.close()
        subscriber.stop()
        sensor.generate = False

        time.sleep(5)
        subscriber = Subscriber(50001, "U")
        time.sleep(1)

        with self.__lock:
            db_connection = sqlite3.connect(subscriber._database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM MessageSocketQueue")
            messages_after_reboot = db_cursor.fetchall()
            subscriber_message_queue_after_reboot = subscriber._subscription_udp_socket.message_queue
            db_cursor.close()
            db_connection.close()
        sensor.stop()
        mb.stop()
        subscriber.stop()
        self.assertEqual(messages, messages_after_reboot, "The messages in the database are not the same after reboot.")
        self.assertEqual(subscriber_message_queue.qsize(), subscriber_message_queue_after_reboot.qsize(),
                         "The messages in the queue are not the same after reboot.")

    def test_subscribe_unsubscribe(self):
        """

        :return:
        """

        mb = MessageBroker()
        subscriber = Subscriber(50005, "B")
        time.sleep(10)
        with self.__lock:
            db_connection = sqlite3.connect(mb._database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM Subscriber")
            subscriptions = db_cursor.fetchall()
            db_cursor.close()
            db_connection.close()

        subscriber.unsubscribe("UV")
        time.sleep(5)

        with self.__lock:
            db_connection = sqlite3.connect(mb._database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM Subscriber")
            after_un_subscriptions = db_cursor.fetchall()
            db_cursor.close()
            db_connection.close()

        subscriber.subscribe("UV")
        time.sleep(5)

        with self.__lock:
            db_connection = sqlite3.connect(mb._database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM Subscriber")
            after_re_subscriptions = db_cursor.fetchall()
            db_cursor.close()
            db_connection.close()

        mb.stop()
        subscriber.stop()
        #sensor.stop()

        self.assertEqual(2, len(subscriptions), "Initial Subscriptions count should be 2")
        self.assertEqual(1, len(after_un_subscriptions), "After unsubscribing the Subscriptions count should be 1")
        self.assertEqual(2, len(after_re_subscriptions), "After resubscribing the Subscriptions count should be 2")


    def test_data_flow_subscription(self):
        """

        :return:
        """
        with self.__lock:
            filenames = [
                "database/message_broker.db",
                "database/message_broker_sub.db",
                "database/SENSOR_BRM_S_50032.db",
                "database/SENSOR_BRM_U_50004.db",
                "database/SUBSCRIBER_U_50005.db",
                "config/SUBSCRIBER_U_50005.json",
                "config/message_broker.json"
            ]

            # Iterate over the list of filenames and delete each file if it exists
            for filename in filenames:
                if os.path.exists(filename):
                    os.remove(filename)

        mb = MessageBroker()

        subscriber = Subscriber(50005, "U", log=False, ignore_startup=True)
        time.sleep(5)

        sensor_u = Sensor(50004, "U", "BRM", generate=False)
        sensor_s = Sensor(50032, "S", "BRM", generate=False)
        time.sleep(2)
        sensor_s.generate_sensor_result()  # 0
        time.sleep(2)
        sensor_u.generate_sensor_result()  # 1
        time.sleep(10)
        subscriber.subscribe("TEMP")
        time.sleep(10)
        sensor_u.generate_sensor_result()  # 2
        time.sleep(2)
        sensor_s.generate_sensor_result()  # 3
        time.sleep(10)
        subscriber.unsubscribe("UV")
        time.sleep(10)
        sensor_u.generate_sensor_result()  # 3
        time.sleep(2)
        sensor_s.generate_sensor_result()  # 4
        time.sleep(10)

        with self.__lock:
            db_connection = sqlite3.connect(subscriber._database_file)
            db_cursor = db_connection.cursor()
            db_cursor.execute("SELECT * FROM MessageSocketQueue")
            messages = db_cursor.fetchall()
            db_cursor.close()
            db_connection.close()
        time.sleep(2)
        mb.stop()
        sensor_u.stop()
        sensor_s.stop()
        subscriber.stop()
        self.assertEqual(4, len(messages))

