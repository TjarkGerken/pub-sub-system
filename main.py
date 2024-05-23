from classes.messageBroker import MessageBroker
import socket

from classes.sensor import Sensor
from classes.subscriber import Subscriber


def main():
    mb = MessageBroker()
    s1 = Sensor(sensor_port=5100, sensor_type="U", location="BRM")
    s2 = Sensor(sensor_port=5101, sensor_type="S", location="BRM")
    # s3 = Sensor(sensor_port=5102, sensor_type="S", location="MHN")
    # s4 = Sensor(sensor_port=5103, sensor_type="U", location="MHN")
    # s5 = Sensor(sensor_port=5104, sensor_type="S", location="KIE")


if __name__ == '__main__':
    main()
