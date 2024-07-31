import time
from unittest import TestCase

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from classes.subscriber import Subscriber
from utils.delete_files_and_folders import delete_files_and_folders


class TestOverallScenario(TestCase):
    """
    Integration test over all components.
    """

    def manual_tear_down(self):
        """
        Deletes the database after the tests are done.
        :return:
        """
        delete_files_and_folders()

    def test_init(self) -> None:
        """
        This Test case tests the initialization of the message broker, sensors, and subscribers.

        It creates a message broker, sensors, and subscribers. It then checks if the number of sensors and subscribers
        created are as expected. It then stops the sensors and subscribers and the message broker.

        :return: None
        """
        def init(sen_no, sub_no) -> tuple:
            """
            Returns a full set of working components with a given count for the sensor and the subscribers.

            :param sen_no: Number of sensors to be created
            :param sub_no: Number of subscribers to be created
            :return: Tuple of Message Broker, List of Sensors and List of Sensors
            """
            delete_files_and_folders()

            # Create message broker
            temp_mb = MessageBroker()

            # Create lists to return
            temp_sensors = []
            temp_subscribers = []

            # Create Sensors
            for i in range(1, sen_no):
                temp_sensor = Sensor(sensor_port=52000 + i, sensor_type="U" if i % 2 == 0 else "S",
                                     location="BRM" if i % 2 == 0 else "MHN")
                temp_sensors.append(temp_sensor)

            # Create subscribers
            for i in range(1, sub_no * 2, 2):
                temp_subscriber = Subscriber(subscriber_port=63000 + i, subscriber_type="B")
                temp_subscribers.append(temp_subscriber)

            return temp_mb, temp_sensors, temp_subscribers

        sen_no = 12
        sub_no = 6
        mb, sensors, subscribers = init(sen_no, sub_no)
        # Check if the correct amount has been created
        self.assertEqual(len(sensors), sen_no - 1, "Expected number of sensors not created")
        self.assertEqual(len(subscribers), sub_no, "Expected number of subscribers not created")

        # Shutdown all processes
        for sensor in sensors:
            sensor.stop()
        for subscriber in subscribers:
            subscriber.stop()
        mb.stop()
        time.sleep(10)
        self.manual_tear_down()
