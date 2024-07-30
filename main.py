import signal
import threading
import time
import sys

from classes.message_broker import MessageBroker

from classes.sensor import Sensor
from classes.subscriber import Subscriber
from utils.logger import logger

components = []


def signal_handler(sig, frame):
    logger.info("Stopping all threads...")

    threads = []
    for component in components:
        # Stop the components
        t = threading.Thread(target=component.stop)
        threads.append(t)
        t.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    # Stop the main thread
    sys.exit(0)


def main():
    # Create publishers (sensors)
    for i in range(1, 2):
        sensor = Sensor(sensor_port=5100 + i, sensor_type="U" if i % 2 == 0 else "S",
                        location="BRM" if i % 2 == 0 else "MHN")
        components.append(sensor)

    # Create message broker
    mb = MessageBroker()
    components.append(mb)
    #
    # # Create subscribers
    #for i in range(1, 2):
    #     subscriber = Subscriber(subscriber_port=6200 + i + 1,
    #                             subscriber_type="B")  # if i % 3 == 0 else "U" if i % 3 == 1 else "S")
    #     components.append(subscriber)

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        time.sleep(0.1)


#def test_case_1():
#   mb = MessageBroker()
#  s1 = Sensor(sensor_port=5100, sensor_type="U", location="BRM")
# # s1 = Sensor(sensor_port=5102, sensor_type="U", location="BRM")
# s1 = Sensor(sensor_port=5105, sensor_type="U", location="BRM")
#sb = Subscriber(subscriber_port=6201, subscriber_type="B")


if __name__ == '__main__':
    main()

#test_case_1()

# TODO: https://docs.python.org/3/howto/logging.html
# TODO: Three Way Handshake
