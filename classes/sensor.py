import datetime
import queue
import random
import select
import socket
import threading
import time

from configuration import MAX_SENSOR_INTERVAL_IN_SECONDS, RETRY_DURATION_IN_SECONDS


class Sensor:
    __sensor_results = queue.Queue()

    def __init__(self, sensor_port: int, sensor_type: str, location: str):
        if sensor_type not in ["U", "S"]:
            raise ValueError("Sensor type must be either 'U' or 'S'")

        # Sensor info
        self.sensor_port = sensor_port
        self.sensor_id = f"{location.upper()}_{sensor_type.upper()}"
        self.sensor_type = sensor_type
        self.location = location

        # Socket
        self.__upd_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.__upd_socket.bind(("127.0.0.1", self.sensor_port))

        print(f"[INFO] | {self.sensor_id} | INITIALIZED")

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
                time.sleep(1)
                continue

            sensor_result = self.__sensor_results.get()
            message = str(sensor_result).encode()

            start_time = time.time()
            while True:
                self.__upd_socket.sendto(message, ("127.0.0.1", 5004))
                ready = select.select([self.__upd_socket], [], [], 5)
                if ready[0]:
                    data, addr = self.__upd_socket.recvfrom(1024)
                    # print(f"{self.sensor_id} | Response Successful")
                    break
                elif time.time() - start_time > RETRY_DURATION_IN_SECONDS:
                    print(f"{self.sensor_id} | Response timeout")
                    break
