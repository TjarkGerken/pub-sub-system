from classes.messageBroker import MessageBroker

from classes.sensor import Sensor
from classes.subscriber import Subscriber


def main():
    mb = MessageBroker()
    
    s1 = Sensor(sensor_port=5100, sensor_type="U", location="BRM")
    s2 = Sensor(sensor_port=5101, sensor_type="S", location="BRM")
    s3 = Sensor(sensor_port=5102, sensor_type="S", location="MHN")
    s4 = Sensor(sensor_port=5103, sensor_type="U", location="MHN")
    s5 = Sensor(sensor_port=5104, sensor_type="S", location="KIE")

    sb1 = Subscriber(subscriber_port=50012, subscriber_type="B")
    sb2 = Subscriber(subscriber_port=6007, subscriber_type="U")
    sb3 = Subscriber(subscriber_port=6006, subscriber_type="S")
    sb4 = Subscriber(subscriber_port=6008, subscriber_type="S")
    sb5 = Subscriber(subscriber_port=6202, subscriber_type="S")


if __name__ == '__main__':
    main()
