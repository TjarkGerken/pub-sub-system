import datetime
import queue
import random
import select
import threading
import time

from classes.udpsocket import UdpSocket
from configuration import MAX_SENSOR_INTERVAL_IN_SECONDS, RETRY_DURATION_IN_SECONDS
from utils.logger import logger


class Sensor:
    __sensor_results = queue.Queue()

    def __init__(self, sensor_port: int, sensor_type: str, location: str):
        if sensor_type not in ["U", "S"]:
            raise ValueError("Sensor type must be either 'U' or 'S'")

        # Sensor info
        self.sensor_port = sensor_port
        self.sensor_id = f"SENSOR_{location.upper()}_{sensor_type.upper()}"
        self.sensor_type = sensor_type
        self.location = location

        # Socket
        self.__upd_socket = UdpSocket(self.sensor_port, self.sensor_id)

        logger.info(f"{self.sensor_id} | Sensor initialized")

        # Start threads
        threading.Thread(target=self.run_sensor).start()
        threading.Thread(target=self.run_messenger).start()

    def generate_sensor_info(self):
        return {
            "sensor_id": self.sensor_id,
            "datetime": str(datetime.datetime.now().isoformat()),
            "sensor_type": self.sensor_type,
            "location": self.location
        }

    def generate_sensor_result(self):
        sensor_info = self.generate_sensor_info()
        data = {}
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
        self.__sensor_results.put(data)

    def run_sensor(self):
        while True:
            self.generate_sensor_result()
            sleep_time = random.randint(1, MAX_SENSOR_INTERVAL_IN_SECONDS)
            time.sleep(sleep_time)

    def run_messenger(self):
        while True:
            if self.__sensor_results.empty():
                continue

            sensor_result = self.__sensor_results.get()
            message = str(sensor_result)
            self.__upd_socket.three_way_send(message, ("127.0.0.1", 5004))
