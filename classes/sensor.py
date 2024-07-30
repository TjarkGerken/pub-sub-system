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
    A class to represent a UV or temperature sensor

    Attributes
    ----------
    sensor_id : str
        The unique identifier of the sensor
    sensor_port : int
        The port number the sensor is listening on
    sensor_type : str
        The type of the sensor, either 'U' for UV or 'S' for temperature
    location : str
        The location of the sensor
    database_file : str
        The path to the SQLite database file for the sensor
    __sensor_results : queue.Queue
        A queue to store the sensor results before they are sent to the Message broker
    __cp_socket : SendingCommunicationProtocolSocket
        The socket to send messages to the Message broker
    __lock : threading.Lock
        A lock to avoid conflicts when accessing the sensor results queue
    """

    def __init__(self, sensor_port: int, sensor_type: str, location: str):
        """
        Initializes the sensor with the given port, type and location

        :param sensor_port: The port number of the sensor
        :param sensor_type: The type of the sensor ('U' for UV, 'S' for temperature)
        :param location: The location of the sensor

        :raise ValueError: If the sensor type is neither 'U' nor 'S'

        :return: Sensor object
        """
        if sensor_type not in ["U", "S"]:
            raise ValueError("Sensor type must be either 'U' or 'S'")

        # Sensor info
        self.sensor_port = sensor_port
        self.sensor_id = f"SENSOR_{location.upper()}_{sensor_type.upper()}_{sensor_port}"
        self.sensor_type = sensor_type
        self.location = location
        self.__sensor_results = queue.Queue()
        self.__actions = []
        self.__lock = threading.Lock()

        # Initialize the database for the sensor
        self.database_file = f"database/{self.sensor_id}.db"
        self.init_db()

        # Initialize socket to send messages to the Message broker
        self.__cp_socket = SendingCommunicationProtocolSocket(self.sensor_id, self.sensor_port)
        self.__actions.append(self.__cp_socket)
        logger.info(
            f"Sensor initialized (UID: {self.sensor_id} | Type: {self.sensor_type} | Location: {self.location})"
        )

        # Start threads to generate sensor data and send messages when data is available
        self.__thread_sensor = StoppableThread(target=self.run_sensor)
        self.__thread_messenger = StoppableThread(target=self.run_messenger)

        # Add threads to the list of actions
        self.__actions.append(self.__thread_messenger)
        self.__actions.append(self.__thread_sensor)

        # Start threads
        self.__thread_sensor.start()
        self.__thread_messenger.start()

    def init_db(self):
        """
        Initializes the database for the sensor. If the database does not exist, it will be created and executes the
        DDL-statements. Finally, it will pre-fill the queue with messages that weren't sent yet

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

        # Close the database connection and cleanup
        logger.debug(f"Initialized database connection (UID: {self.sensor_id})")

    def prefill_queue(self) -> None:
        """
        TODO: UPDATE DOCSTRING
        Prefill the sensor results queue with messages that weren't sent yet. This is done by fetching all messages from
        the `MessagesToSend` table in the sensor's database and converts them from their string representation to a
        dictionary. The dictionary is then put into the queue object.

        :param db_connection: The (already opened) SQLite database connection.
        :param db_cursor: The (already initialized) SQLite database cursor
        :return: None
        """
        # Fetch all messages from the database ordered by oldest first

        with self.__lock:
            db_connection = sqlite3.connect(self.database_file)
            db_cursor = db_connection.cursor()

            messages_to_send = db_cursor.execute("SELECT * FROM MessagesToSend ORDER BY MessageID").fetchall()

            db_cursor.close()
            db_connection.close()

        # Convert the string representation of the message to a dictionary and put it into the queue
        for message in messages_to_send:
            data = ast.literal_eval(message[1])
            self.__sensor_results.put(data)

        return None

    def generate_sensor_info(self):
        """
        Generate basic sensor information like a sensor id, the current datetime, sensor type and location

        :return: A dictionary containing the sensor information
        """
        return {
            "sensor_id": self.sensor_id,
            "datetime": str(datetime.datetime.now().isoformat()),
            "sensor_type": self.sensor_type,
            "location": self.location
        }

    def generate_sensor_result(self):
        """
        TODO: UPDATE DOCSTRING
        Generates artificial sensor results based on the sensor type (UV index or temperature), stores it in the
        database and puts it into the sensor results queue.

        :param db_connection: The (already opened) SQLite database connection
        :param db_cursor: The (already initialized) SQLite database cursor

        :return: None
        """
        data = {}
        sensor_info = self.generate_sensor_info()

        if self.sensor_type == "U":
            uv_index = random.randint(0, 26)
            data = {
                "uv_index": uv_index
            }
        elif self.sensor_type == "S":
            temperature = random.randint(-50, 50)
            data = {
                "temperature": temperature
            }

        data.update(sensor_info)

        with self.__lock:
            db_connection = sqlite3.connect(self.database_file)
            db_cursor = db_connection.cursor()

            try:
                db_cursor.execute("INSERT INTO MessagesToSend (Data) VALUES(?)", (json.dumps(data),))
                db_connection.commit()
            except sqlite3.OperationalError as e:
                logger.error(f"Error while inserting into the database: {e}")

            db_cursor.close()
            db_connection.close()

        self.__sensor_results.put(data)

        return None

    def run_sensor(self):
        while not self.__thread_sensor.stopped():
            self.generate_sensor_result()
            sleep_time = random.randint(1, MAX_SENSOR_INTERVAL_IN_SECONDS)
            start_time = time.time()
            # Wait for the next sensor reading (faster than time.sleep(x) for high x)
            while time.time() - start_time < sleep_time and not self.__thread_sensor.stopped():
                time.sleep(0.1)

        return None

    def run_messenger(self):
        while not self.__thread_messenger.stopped():
            if self.__sensor_results.empty():
                continue

            sensor_result = self.__sensor_results.get()
            message = json.dumps(sensor_result)

            logger.info(f"[{self.sensor_id}] Send Message: {message}")
            self.__cp_socket.send_message(message, ("127.0.0.1", 5004))

            # TODO: Documentation - When message could not be sent, it should be deleted anyway \
            #  --> If the function is over, either the threshold was exceeded or the message was sent successfully
            with self.__lock:
                db_connection = sqlite3.connect(self.database_file)
                db_cursor = db_connection.cursor()

                db_connection.execute("DELETE FROM MessagesToSend WHERE Data = ?", (message,))
                db_connection.commit()

                db_cursor.close()
                db_connection.close()

    def stop(self):
        """
        Stops all running tasks of the sensor gracefully to shut the sensor down

        :return: None
        """
        logger.info(f"Stopping threads for sensor {self.sensor_id}...")

        counter = 0
        for action in self.__actions:
            counter += 1
            if isinstance(action, StoppableThread):
                logger.debug(f"Stopping thread ({counter}/{len(self.__actions)}) (Thread Name: {action.name})")
                action.stop()
                action.join()
            elif isinstance(action, SendingCommunicationProtocolSocket):
                logger.debug(f"Stopping thread ({counter}/{len(self.__actions)}) (Socket Name: {action.uid})")
                action.stop()

        return None


def handle_signal(sig, frame):
    sensor.stop()
    sys.exit(0)


if __name__ == "__main__":
    sensor = Sensor(5001, "U", "BERLIN")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while True:
        time.sleep(0.1)
