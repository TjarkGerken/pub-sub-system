from classes.messageBroker import MessageBroker

from classes.sensor import Sensor
from classes.subscriber import Subscriber

def main():
    mb = MessageBroker()

    for i in range(1, 2):
        sensor = Sensor(sensor_port=5100 + i, sensor_type="U" if i % 2 == 0 else "S",
                        location="BRM" if i % 2 == 0 else "MHN")

        subscriber = Subscriber(subscriber_port=6200 + i,
                                subscriber_type="B" if i % 3 == 0 else "U" if i % 3 == 1 else "S")


def test_case_1():
    mb = MessageBroker()
    s1 = Sensor(sensor_port=5100, sensor_type="U", location="BRM")
    sb = Subscriber(subscriber_port=6201, subscriber_type="B")


if __name__ == '__main__':
    main()

   #test_case_1()

# TODO: https://docs.python.org/3/howto/logging.html
# TODO: Three Way Handshake
