import time

from classes.message_broker import MessageBroker

from classes.sensor import Sensor
from classes.subscriber import Subscriber


def main():
    mb = MessageBroker()

    for i in range(1, 2):
        sensor = Sensor(sensor_port=5100 + i, sensor_type="U" if i % 2 == 0 else "S",
                        location="BRM" if i % 2 == 0 else "MHN")

        subscriber = Subscriber(subscriber_port=6200 + i + 1,
                                subscriber_type="B")  # if i % 3 == 0 else "U" if i % 3 == 1 else "S")


def test_subscriber():
    mb = MessageBroker()
    sensor = Sensor(sensor_port=5100, sensor_type="U", location="BRM")
    sensor = Sensor(sensor_port=5102, sensor_type="S", location="BRM")
    subscriber = Subscriber(subscriber_port=6205, subscriber_type="B")
    time.sleep(10)
    subscriber.unsubscribe("UV")
    time.sleep(10)
    subscriber.unsubscribe("TEMP")
    #time.sleep(30)
    #subscriber.subscribe("UV")


#def test_case_1():
#   mb = MessageBroker()
#  s1 = Sensor(sensor_port=5100, sensor_type="U", location="BRM")
# # s1 = Sensor(sensor_port=5102, sensor_type="U", location="BRM")
# s1 = Sensor(sensor_port=5105, sensor_type="U", location="BRM")
#sb = Subscriber(subscriber_port=6201, subscriber_type="B")


if __name__ == '__main__':
    test_subscriber()
    # main()


#test_case_1()

# TODO: https://docs.python.org/3/howto/logging.html
# TODO: Three Way Handshake
