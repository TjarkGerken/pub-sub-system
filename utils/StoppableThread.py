"""
This module contains the StoppableThread class, which is a subclass of the threading.Thread class and is extended
by functionality to stop the thread. The thread itself has to check regularly for the stopped() condition.
"""

import threading


class StoppableThread(threading.Thread):
    """
    Thread class with a stop() method. The thread itself has to check regularly for the stopped() condition.
    Source: https://stackoverflow.com/questions/323972/is-there-any-way-to-kill-a-thread
    """
    def __init__(self,  *args, **kwargs) -> None:
        """
        Constructor for the StoppableThread class.

        :param args: Positional arguments passed to the threading.Thread constructor.
        :param kwargs: Keyword arguments passed to the threading.Thread constructor.
        """
        super(StoppableThread, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()

    def stop(self) -> None:
        """
        Triggers the stop event. The thread itself has to check regularly for the stopped() condition.
        :return: None
        """
        self._stop_event.set()
        return None

    def stopped(self) -> bool:
        """
        Checks if the stop event is set.
        :return: True if the stop event is set, False otherwise.
        """
        return self._stop_event.is_set()
