"""
This module defines the Sensor class, which represents an UV or temperature sensor.
The Sensor class will generate artificial sensor data, store it in a database and send it to the message broker.
"""

import ast
import datetime
import json
import queue
import random
import signal
import sqlite3
import sys
import threading
import time

from classes.CommunicationProtocol.sending_communication_protocol_socket import SendingCommunicationProtocolSocket
from configuration import MAX_SENSOR_INTERVAL_IN_SECONDS
from utils.StoppableThread import StoppableThread
from utils.logger import logger


class Sensor:
    """
    A class to represent a UV or temperature sensor that sends its data to a message broker

    Attributes
    ----------
    sensor_id : str
        The unique identifier of the sensor
    sensor_port : int
        The port number of the sensor
    sensor_type : str
        The type of the sensor ('U' for UV, 'S' for temperature)
    location : str
        The location of the sensor
    database_file : str
        The path to the database file for the sensor
    _sensor_results : queue.Queue
        A queue to store the sensor results before they are sent to the message broker
    __actions : list
        A list of all actions that are performed by the sensor (threads and sockets) that should be gracefully stopped
    _cp_socket : SendingCommunicationProtocolSocket
        A socket to send messages to the message broker
    __thread_sensor : StoppableThread
        A thread that generates sensor data
    __thread_messenger : StoppableThread
        A thread that sends messages when sensor data is available
    __lock : threading.Lock
        A lock to avoid conflicts when accessing the same resource from multiple threads
    """

    def __init__(self, sensor_port: int, sensor_type: str, location: str, generate: bool = True):
        """
        Initializes the sensor with the given port, type and location and starts the threads to generate sensor data and
        communicate with the message broker.

        :param sensor_port: The port number of the sensor
        :param sensor_type: The type of the sensor ('U' for UV, 'S' for temperature)
        :param location: The location of the sensor
        :param generate: Whether the sensor should generate data or not

        :return: Sensor object

        :raise ValueError: If the sensor type is neither 'U' nor 'S'
        """

        if sensor_type not in ["U", "S"]:
            raise ValueError("Sensor type must be either 'U' or 'S'")

        # Sensor info
        self.sensor_port = sensor_port
        self.sensor_id = f"SENSOR_{location.upper()}_{sensor_type.upper()}_{sensor_port}"
        self.sensor_type = sensor_type
        self.location = location
        self._sensor_results = queue.Queue()
        self.generate = generate
        self.__actions = []
        self.__lock = threading.Lock()

        # Initialize the database for the sensor
        self.database_file = f"database/{self.sensor_id}.db"
        self.init_db()

        # Initialize socket to send messages to the Message broker
        self._cp_socket = SendingCommunicationProtocolSocket(self.sensor_id, self.sensor_port)
        self.__actions.append(self._cp_socket)
        logger.info(
            f"Sensor initialized (UID: {self.sensor_id} | Type: {self.sensor_type} | Location: {self.location})")

        # Start threads to generate sensor data and send messages when data is available
        self.__thread_sensor = StoppableThread(target=self.run_sensor)
        self.__thread_messenger = StoppableThread(target=self.run_messenger)

        # Add threads to the list of actions
        self.__actions.append(self.__thread_messenger)
        self.__actions.append(self.__thread_sensor)

        # Start threads
        self.__thread_sensor.start()
        self.__thread_messenger.start()

    def init_db(self) -> None:
        """
        Initializes the database for the sensor. If the tables within the database don't exist, they will be created by
        the DDL-statements. Finally, it will prefill the queue with messages that weren't sent to the message broker yet

        :return: None
        """
        # Create the database if it does not exist and connect to it
        with self.__lock:
            db_connection = sqlite3.connect(self.database_file)
            db_cursor = db_connection.cursor()

            # Execute DDL script to create tables if not already exist
            with open("database/ddl_sensor.sql", "r") as ddl_file:
                db_cursor.executescript(ddl_file.read())
                db_connection.commit()

            db_cursor.close()
            db_connection.close()

        # Prefill queue with messages that weren't sent yet
        self.prefill_queue()

        logger.debug(f"Initialized database connection (UID: {self.sensor_id})")

    def prefill_queue(self) -> None:
        """
        Prefill the sensor results queue with messages that weren't sent to the message broker yet. This is done by
        fetching all messages from the `MessagesToSend` table in the database. The messages are then converted from a
        to a dictionary and put into the queue.

        :return: None
        """
        # Lock the resource to avoid conflicts when accessing the database
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.database_file)
            db_cursor = db_connection.cursor()

            # Fetch all messages from the database ordered by oldest first
            messages_to_send = db_cursor.execute("SELECT * FROM MessagesToSend ORDER BY MessageID").fetchall()

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        # Convert the string representation of the message to a dictionary and put it into the queue
        for message in messages_to_send:
            data = ast.literal_eval(message[1])
            self._sensor_results.put(data)

        return None

    def generate_sensor_info(self):
        """
        Generate basic sensor information like a sensor id, the current datetime, sensor type and location
        :return: A dictionary containing the sensor information
        """
        return {"sensor_id": self.sensor_id, "datetime": str(datetime.datetime.now().isoformat()),
                "sensor_type": self.sensor_type, "location": self.location
                }

    def generate_sensor_result(self) -> None:
        """
        Generates artificial sensor results based on the sensor type (UV index or temperature), stores it in the
        database and puts it into the sensor results queue.

        :return: None
        """
        data = {}
        sensor_info = self.generate_sensor_info()

        # Generate artificial sensor data
        if self.sensor_type == "U":
            uv_index = random.randint(0, 26)
            data = {"uv_index": uv_index}
        elif self.sensor_type == "S":
            temperature = random.randint(-50, 50)
            data = {"temperature": temperature}
        data.update(sensor_info)

        # Lock the resource to avoid conflicts when accessing the database
        with self.__lock:
            # Connect to the database
            db_connection = sqlite3.connect(self.database_file)
            db_cursor = db_connection.cursor()

            try:
                # Insert the sensor data into the database
                db_cursor.execute("INSERT INTO MessagesToSend (Data) VALUES(?)", (json.dumps(data),))
                db_connection.commit()
            except sqlite3.OperationalError as e:
                logger.error(f"Error while inserting into the database: {e}")

            # Close the database connection
            db_cursor.close()
            db_connection.close()

        self._sensor_results.put(data)

        return None

    def run_sensor(self) -> None:
        """
        Continuously generate sensor data and put it into the sensor results queue until the thread is stopped
        :return: None
        """
        # Generate sensor data until the thread is stopped
        while not self.__thread_sensor.stopped() and self.generate:
            self.generate_sensor_result()

            # Sleep for a random time between 1 and MAX_SENSOR_INTERVAL_IN_SECONDS defined in configuration file
            sleep_time = random.randint(1, MAX_SENSOR_INTERVAL_IN_SECONDS)
            start_time = time.time()
            # Wait for the next sensor reading (faster than time.sleep(x) for high x)
            while time.time() - start_time < sleep_time and not self.__thread_sensor.stopped():
                time.sleep(0.1)

        return None

    def run_messenger(self) -> None:
        # Send sensor data to the message broker when available until the thread is stopped
        while not self.__thread_messenger.stopped():
            # Check if there is any sensor data available to send
            if self._sensor_results.empty():
                continue

            # Get the sensor data from the queue and send it to the message broker
            sensor_result = self._sensor_results.get()
            message = json.dumps(sensor_result)

            # logger.info(f"[{self.sensor_id}] Send Message: {message}")
            ack_received = self._cp_socket.send_message(message, ("127.0.0.1", 5004))

            # Delete the message from the database when it was sent successfully (message broker received it) or could
            # not be sent (within specified time interval), it should be deleted anyway. If send_message is over, either
            # the threshold was exceeded or the message was sent successfully and the message can be deleted from the
            # database, so it won't be sent again
            with self.__lock:
                if not ack_received:
                    return
                # Connect to the database
                db_connection = sqlite3.connect(self.database_file)
                db_cursor = db_connection.cursor()

                # Delete the message from the database, so it won't be sent again
                db_connection.execute("DELETE FROM MessagesToSend WHERE Data = ?", (message,))
                db_connection.commit()

                # Close the database connection
                db_cursor.close()
                db_connection.close()

        return None

    def stop(self) -> None:
        """
        Stops all running tasks of the sensor gracefully to shut the sensor down
        :return: None
        """
        logger.info(f"Stopping threads for sensor {self.sensor_id}...")

        counter = 0
        # Iterate over all components that need to be stopped and call their own stop methods
        for action in self.__actions:
            counter += 1
            if isinstance(action, StoppableThread):
                logger.info(f"Stopping thread ({counter}/{len(self.__actions)}) (Thread Name: {action.name})")
                action.stop()
                action.join()
            elif isinstance(action, SendingCommunicationProtocolSocket):
                logger.info(f"Stopping thread ({counter}/{len(self.__actions)}) (Socket Name: {action.uid})")
                action.stop()

        return None


def handle_signal(sig, frame):
    """
    Handle signals to stop the subscriber gracefully

    :param sig: Signal number
    :param frame: Current stack frame

    :return: None
    """
    sensor.stop()
    sys.exit(0)


if __name__ == "__main__":
    # Create a sensor
    sensor = Sensor(5001, "U", "BERLIN")

    # Set up handler for stopping the program gracefully on SIGINT (CTRL+C signal) and SIGTERM (Termination signal)
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while True:
        time.sleep(0.1)
