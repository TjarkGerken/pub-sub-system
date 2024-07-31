import signal
import sys
import threading
import time

from classes.message_broker import MessageBroker
from classes.sensor import Sensor
from classes.subscriber import Subscriber
from utils.logger import logger

components = []
stop = False


def signal_handler(sig, frame, already_stopping) -> None:
    """
    Signal handler for stopping all threads and exiting the program.

    :param sig: Signal number
    :param frame: Current stack frame
    :param already_stopping: Flag to check if the threads are already stopping

    :return: None
    """
    if already_stopping:
        logger.info("Already stopping all threads, please wait...")
        return None

    already_stopping = True

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


def test_subscriber() -> None:
    """
    Test function for creating a message broker, sensors and a subscriber.
    Unsubscribes the subscriber from certain topics after a delay.

    :return: None
    """
    # Create message broker
    mb = MessageBroker()

    # Create sensors
    sensor = Sensor(sensor_port=5100, sensor_type="U", location="BRM")
    sensor = Sensor(sensor_port=5102, sensor_type="S", location="BRM")

    # Create subscriber
    subscriber = Subscriber(subscriber_port=6205, subscriber_type="B")

    # Unsubscribe from topics
    time.sleep(10)
    subscriber.unsubscribe("UV")
    time.sleep(10)
    subscriber.unsubscribe("TEMP")  # time.sleep(30)  # subscriber.subscribe("UV")

    return None


def main() -> None:
    """
    Main function for creating sensors, a message broker and setting up the signal handler.
    Runs an infinite loop to keep the program running.
    This method is only executed when the program is run as the main program.

    :return: None
    """
    # Create publishers (sensors)
    for i in range(1, 6):
        sensor = Sensor(sensor_port=50100 + i, sensor_type="U" if i % 2 == 0 else "S",
                        location="BRM" if i % 2 == 0 else "MHN")
        components.append(sensor)

    # Create message broker
    mb = MessageBroker()
    components.append(mb)

    # Create subscribers
    for i in range(1, 5, 2):
        subscriber = Subscriber(subscriber_port=60200 + i, subscriber_type="B")
        components.append(subscriber)

    # Set up handler for stopping the program gracefully on SIGINT (CTRL+C signal) and SIGTERM (Termination signal)
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, stop))
    signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame, stop))

    while True:
        time.sleep(0.1)


if __name__ == '__main__':
    # test_subscriber()
    main()
