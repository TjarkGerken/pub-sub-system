from unittest import TestCase

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from classes.subscriber import Subscriber


class TestOverallScenario(TestCase):
    def init(self, SEN_NO, SUB_NO):
        # Create message broker
        mb = MessageBroker()
        sensors = []
        subscribers = []
        for i in range(1, SEN_NO):
            sensor = Sensor(sensor_port=52000 + i, sensor_type="U" if i % 2 == 0 else "S",
                            location="BRM" if i % 2 == 0 else "MHN")
            sensors.append(sensor)
        # Create subscribers
        for i in range(1, SUB_NO * 2, 2):
            subscriber = Subscriber(subscriber_port=63000 + i, subscriber_type="B")
            subscribers.append(subscriber)

        return mb, sensors, subscribers

    def test_init(self):
        sen_no = 12
        sub_no = 6
        mb, sensors, subscribers = self.init(sen_no, sub_no)
        self.assertEqual(len(sensors), sen_no - 1, "Expected number of sensors not created")
        self.assertEqual(len(subscribers), sub_no, "Expected number of subscribers not created")
        for sensor in sensors:
            sensor.stop()
            sensors.remove(sensor)
        for subscriber in subscribers:
            subscriber.stop()
            subscribers.remove(subscriber)
        mb.stop()
