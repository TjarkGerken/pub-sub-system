import sqlite3
import threading
from queue import Queue
from typing import Literal


class Database:
    def __init__(self, db_filename):
        self.tasks = Queue()
        self.connection = sqlite3.connect(db_filename, check_same_thread=False)
        self.cursor = self.connection.cursor()
        self._stop_event = threading.Event()
        self.worker_thread = threading.Thread(target=self.execute_tasks)
        self.worker_thread.start()

    def end(self):
        self._stop_event.set()
        self.worker_thread.join()
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

        return None

    def add_task(self, task):
        self.tasks.put(task)
        return None

    def execute_tasks(self):
        while not self._stop_event.is_set() and not self.tasks.empty():
            if self.tasks.empty():
                continue

            task = self.tasks.get()
            try:
                if task.op == "single":
                    self.cursor.execute(task.sql, task.args)
                elif task.op == "script":
                    self.cursor.executescript(task.sql)

                if task.return_result:
                    task.result = self.cursor.fetchall()

                self.connection.commit()
            except sqlite3.Error as e:
                print(f"Database error: {e}")
            finally:
                self.tasks.task_done()
                task.is_ready = True

        return None


class DatabaseTask:
    def __init__(self, op: str = Literal["single", "script"], sql: str = "", args: tuple = None, return_result: bool = False):
        self.op = op
        self.sql = sql
        self.args = args
        self.return_result = return_result
        self.result = None
        self.is_ready = False
