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
        subscriber = Subscriber(50005, "U")
        sensor = Sensor(50004, "U", "BRM")

        time.sleep(5)
        subscriber.unsubscribe("UV")

        with self.__lock:
            db_connection = sqlite3.connect(mb._database_file)



        mb.stop()
        subscriber.stop()
        sensor.stop()


