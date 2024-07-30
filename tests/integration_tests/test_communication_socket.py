import unittest
import threading
import time
import sqlite3
from classes.sensor import Sensor
from classes.message_broker import MessageBroker
from classes.subscriber import Subscriber

class TestMessageBrokerIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize the MessageBroker
        cls.message_broker = MessageBroker()

        # Initialize a Sensor
        cls.sensor = Sensor(sensor_port=5100, sensor_type="U", location="BRM")

        # Initialize a Subscriber
        cls.subscriber = Subscriber(subscriber_port=6205, subscriber_type="B")

        # Allow some time for setup
        time.sleep(2)

    @classmethod
    def tearDownClass(cls):
        # Stop the MessageBroker
        cls.message_broker.stop()

        # Stop the Sensor
        cls.sensor.stop()

        # Stop the Subscriber
        cls.subscriber.stop()

    def test_message_flow(self):
        # Simulate sending a message from the sensor
        self.sensor.send_message("Test message")

        # Allow some time for the message to be processed
        time.sleep(2)

        # Check if the message was received by the subscriber
        received_message = self.subscriber.receive_message()
        self.assertEqual(received_message, "Test message")

    def test_subscription(self):
        # Subscribe to a topic
        self.subscriber.subscribe("UV")

        # Allow some time for the subscription to be processed
        time.sleep(2)

        # Check if the subscription was successful
        with sqlite3.connect(self.message_broker.__database_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM Subscriber WHERE Topic = 'UV'")
            result = cursor.fetchone()
            self.assertIsNotNone(result)

if __name__ == '__main__':
    unittest.main()