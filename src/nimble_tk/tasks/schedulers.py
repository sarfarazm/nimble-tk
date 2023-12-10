import datetime
import threading
from .. import common

class FixedDelayTaskScheduler(object):
    """
    A class for scheduling a task to be executed at regular delays. 
    This will start the delay countdown only after the current task completes.

    Sample code
    >>> 
    >>> def code_to_run_repeatedly():
    >>>     ntk.log_info('Running a repeated task')
    >>> 
    >>> scheduler = ntk.FixedDelayTaskScheduler(interval=5, function=code_to_run_repeatedly, log_prefix='[Some][Log][Prefix]')
    >>> 
    >>> import time
    >>> time.sleep(20)
    >>> scheduler.shutdown()
    """

    def __init__(self, interval, function, log_prefix='', log=True, init_interval=None, max_interval=None, *args, **kwargs):
        """
        Initializes the scheduler with specified parameters.

        :param interval: The initial interval in seconds between task executions.
        :param function: The function to be executed.
        :param log_prefix: Optional prefix for log messages.
        :param log: Flag to enable or disable logging.
        :param init_interval: Optional initial delay before the first task execution.
        :param max_interval: Maximum time allowed for an execution; beyond this, a new execution starts.
        :param args: Additional arguments to pass to the function.
        :param kwargs: Additional keyword arguments to pass to the function.
        """
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs

        # internal state
        self.is_running = False
        self.running_since = datetime.datetime(1970, 1, 1)
        self.is_shutdown = False
        self.log_prefix = log_prefix
        self.log = log
        self.max_interval = max_interval
        self.__set_timer(interval=init_interval)

    def _run(self):
        """
        Internal method to execute the scheduled function.
        Manages the state and logs before and after the function execution.
        """
        common.set_thread_log_prefix(self.log_prefix)

        if self.should_start_next_run():
            self.__set_timer()
            try:
                self.is_running = True
                self.running_since = datetime.datetime.now()
                stopwatch = common.StopWatch()
                if self.log:
                    common.log_info(f"Function Started")
                self.function(*self.args, **self.kwargs)
                if self.log:
                    common.log_info(
                        f"Function Done - took time {stopwatch.get_time()}")
            except:
                common.log_traceback(
                    f"Error in calling the Function - took time {stopwatch.get_time()}")
            finally:
                self.is_running = False

    def should_start_next_run(self):
        """
        Determines whether the next function execution should start.

        :return: True if the next execution should start, False otherwise.
        """
        if not self.is_running:
            return True
        elif self.max_interval:
            delta_seconds = (datetime.datetime.now() - self.running_since).total_seconds()
            if delta_seconds > self.max_interval:
                common.log_error(f"Previous invocation of the function is running for more than {self.max_interval} seconds - starting a new run")
                return True
            else:
                common.log_error(f"Previous invocation of the function is still running. delta_seconds: {delta_seconds} . Waiting for {self.min_interval} seconds.")
                self.set_timer(interval=self.min_interval)
                return False
        else:
            common.log_error(f"Previous invocation of the function is still running. Max interval not set. Waiting for {self.min_interval} seconds.")
            self.set_timer(interval=self.min_interval)
            return False

    def update_interval(self, new_interval):
        """
        Updates the interval between task executions.

        :param new_interval: The new interval in seconds.
        """
        common.log_info(f"Updating interval from {self.interval} to {new_interval}")
        self.interval = new_interval

    def __set_timer(self, interval=None):
        """
        Sets the timer for the next execution of the task.

        :param interval: The interval after which the task should be executed. Defaults to the current interval.
        """
        if not interval:
            interval = self.interval

        if not self.is_shutdown:
            self._timer = threading.Timer(interval, self._run)
            self._timer.daemon=True
            self._timer.start()

    def shutdown(self):
        """
        Shuts down the scheduler, cancelling any pending executions.
        """
        common.log_info('shutdown called')
        self.is_shutdown = True
        self._timer.cancel()
