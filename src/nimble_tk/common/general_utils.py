# import numpy as np
import random
import gc
import importlib
from importlib import reload
import pandas as pd
import sys
import traceback
import os
import datetime
from io import StringIO

from logger import *


class StopWatch:
    """
    A simple stopwatch class for tracking elapsed time.

    Attributes:
        start (datetime): The start time of the stopwatch.
    """

    def __init__(self):
        """
        Initializes a new instance of the StopWatch class, setting the start time to the current time.
        """
        self.start = datetime.datetime.now()

    def reset(self) -> None:
        """
        Resets the start time of the stopwatch to the current time.
        """
        self.start = datetime.datetime.now()

    def get_time_hhmmss(self):
        """
        Calculates the elapsed time since the start in hours, minutes, seconds, and milliseconds.

        Returns:
            str: The elapsed time formatted as 'HH:MM:SS.mmm'.
        """
        end = datetime.datetime.now()
        time_diff = (end - self.start)
        diff_seconds = time_diff.seconds
        m, s = divmod(diff_seconds, 60)
        h, m = divmod(m, 60)
        ms = (time_diff.microseconds / 1000) % 1000
        time_str = "%02d:%02d:%02d.%03d" % (h, m, s, ms)
        return time_str

    def get_time(self) -> str:
        """
        Convenience method to get the elapsed time in hours, minutes, seconds, and milliseconds format.

        Returns:
            str: The elapsed time formatted as 'HH:MM:SS.mmm'.
        """
        return self.get_time_hhmmss()

    def get_time_and_reset(self) -> str:
        """
        Returns the elapsed time and then resets the stopwatch.

        Returns:
            str: The elapsed time before reset, formatted as 'HH:MM:SS.mmm'.
        """
        time_str = self.get_time_hhmmss()
        self.reset()
        return time_str


class ApplicationError(RuntimeError):
    """
    Helper class to raise checked exceptions. It can be used to differentiate
    between errors in current applications vs third party. 
    Usage:
        raise ApplicationError("Required column missing from the features dataframe.")
    """
    pass


class IterWrapper(object):
    """
    A wrapper class to handle iterators, especially for decoding byte streams to UTF-8 strings.

    Attributes:
        iter1 (iterator): The iterator to be wrapped.
    """

    def __init__(self, iter1):
        """
        Initializes the IterWrapper with the provided iterator.

        Args:
            iter1 (iterator): An iterator to be wrapped by IterWrapper.
        """
        self.iter1 = iter1

    def __iter__(self):
        """
        Returns the IterWrapper instance itself as an iterator.

        Returns:
            IterWrapper: The instance itself as an iterator.
        """
        return self

    def __next__(self):
        """
        Fetches the next item from the wrapped iterator, decodes it to a UTF-8 string if it's not None.

        Returns:
            str or None: The next decoded string from the iterator, or None if no more items.
        """
        next1 = self.iter1.__next__()
        if next1:
            return next1.decode('utf-8')
        return None


class FixedDelayTaskScheduler(object):
    """
    A class for scheduling a task to be executed at regular delays. 
    This will start the delay countdown only after the current task completes.

    # sample code
    # define the function to be run at regular intervals
    def code_to_run_repeatedly():
        ...
        ...

    # schedule the task to run every 2 seconds
    scheduler = FixedDelayTaskScheduler(interval=2, code_to_run_repeatedly, log_prefix='[Some][Log][Prefix]')

    # stop the time if required
    scheduler.stop()
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
        self.set_timer(interval=init_interval)

    def _run(self):
        """
        Internal method to execute the scheduled function.
        Manages the state and logs before and after the function execution.
        """
        set_thread_log_prefix(self.log_prefix)

        if self.should_start_next_run():
            self.set_timer()
            try:
                self.is_running = True
                self.running_since = datetime.datetime.now()
                stopwatch = StopWatch()
                if self.log:
                    log_info(f"Function Started")
                self.function(*self.args, **self.kwargs)
                if self.log:
                    log_info(
                        f"Function Done - took time {stopwatch.get_time()}")
            except:
                log_traceback(
                    f"Error in calling the Function - took time {stopwatch.get_time()}")
            finally:
                self.is_running = False

    def should_start_next_run(self):
        """
        Determines whether the next function execution should start.

        :return: True if the next execution should start, False otherwise.
        """
        # Your existing logic here...

    def update_interval(self, new_interval):
        """
        Updates the interval between task executions.

        :param new_interval: The new interval in seconds.
        """
        log_info(f"Updating interval from {self.interval} to {new_interval}")
        self.interval = new_interval

    def set_timer(self, interval=None):
        """
        Sets the timer for the next execution of the task.

        :param interval: The interval after which the task should be executed. Defaults to the current interval.
        """
        # Your existing logic here...

    def shutdown(self):
        """
        Shuts down the scheduler, cancelling any pending executions.
        """
        self.is_shutdown = True
        self._timer.cancel()

# ------------------------------------------------


# write to excel
"""
usage:
dfs_map = {'Sheet1':df1, 'sheet2': df2}
file_name = '/tmp/data.xlsx'
write_dfs_to_excel(dfs_map, file_name, percent_cols=[])
"""


def write_dfs_to_excel(dfs_map, file_name, maximize_col_widths=True, percent_cols=[], text_cols=[], index=False):
    log_info(f"Write to excel - {file_name}")
    writer = pd.ExcelWriter(file_name, engine='xlsxwriter',
                            datetime_format='mmm d yyyy hh:mm:ss', date_format='mmm dd yyyy')
    workbook = writer.book
    num_format = workbook.add_format(
        {'num_format': '[>9999999]##\,##\,##\,##0; [>99999]##\,##\,##0; ##,##0'})
    percent_fmt = workbook.add_format({'num_format': '0.00%'})
    text_format = workbook.add_format({'num_format': '@'})
    for sheet, df in dfs_map.items():
        log_info(f"Write to excel - writing sheet - {sheet}")
        df.to_excel(writer, sheet, index=index)

    for sheet, df in dfs_map.items():

        df_sample = df.sample(
            n=min(df.shape[0], 100)).reset_index(drop=(not index))

        for column in df_sample.columns:
            col_format = None
            max_length_for_col = df_sample[column].astype(str).map(len).max()
            # if column name is bigger than column content
            max_length_for_col = max(max_length_for_col, len(column))
            # limit column size to 250 chars
            max_length_for_col = min(max_length_for_col, 250)
            column_length = max_length_for_col
            if column_length > 150:
                column_length = 150
            col_idx = df_sample.columns.get_loc(column)

            if column in text_cols:
                col_format = text_format
            elif 'int' in str(df_sample[column].dtype) or 'float' in str(df_sample[column].dtype):
                if column in percent_cols:
                    col_format = percent_fmt
                else:
                    col_format = num_format
            elif 'datetime64[ns' in str(df_sample[column].dtype):
                column_length = 19
            else:
                # writer.sheets[sheet].set_column(col_idx, col_idx, column_length + 2)
                pass

            writer.sheets[sheet].set_column(
                col_idx, col_idx, column_length + 2, col_format)

    try:
        writer.save()
    except:
        writer.close()

    log_info("Write to excel - done")

# ----------------------------------------------------------


def exception_to_trace_string(exception):
    if exception.__cause__:
        return '\n'.join(exception.__cause__.args)
    else:
        return '\n'.join(exception.args)

# ----------------------------------------------------------------------


def get_num_cpus():
    return os.cpu_count()


def get_system_ram():
    mem_bytes = os.sysconf('SC_PAGE_SIZE') * \
        os.sysconf('SC_PHYS_PAGES')  # e.g. 4015976448
    mem_gib = mem_bytes/(1024.**3)
    return mem_gib

# ------------------------------------------------


def run_gc():
    # Returns the number of
    # objects it has collected
    # and deallocated
    collected = gc.collect()

    # Prints Garbage collector
    # as 0 object
    log_info("Garbage collector: collected %d objects." % collected)

# ----------------------------------------------------------------------


# useful constants
ONE_LAKH = 100 * 1000
ONE_MILLION = ONE_LAKH * 10
TEN_LAKHS = ONE_MILLION
FIVE_MILLION = ONE_MILLION * 5
ONE_CRORE = ONE_MILLION * 10
TEN_CRORE = ONE_CRORE * 10
ONE_BILLION = TEN_CRORE * 10
HUNDRED_CRORE = ONE_BILLION
ONE_QUADRILLION = ONE_BILLION * ONE_MILLION

"""
This functions helps in hot reloading a class which has been recently modified.
"""


def reload_modules(module_list=[]):
    # module_list = ['common_funcs']
    for module in module_list:
        if module in sys.modules:
            reload(sys.modules[module])
            importlib.import_module(module)


def add_function_to_object(object1, function1, class1):
    object1.__dict__[function1.name] = function1.__get__(function1, class1)


def generate_pseudo_uuid(n=5, prefix=''):
    now = datetime.datetime.now()
    random_append_str = ''
    for r in range(n):
        random_append_str += str(random.randint(0, 10))
    return prefix + now.strftime("%y%m%d%H%M%S%f") + random_append_str

# todo: print traceback in proper format


def log_uncaught_exception(exctype, value, tb):
    if issubclass(exctype, KeyboardInterrupt):
        sys.__excepthook__(exctype, value, tb)
        return
    NEWLINE = "\n"
    log_error(
        f'log_uncaught_exception: exctype: {exctype} - value: {value} - traceback: {NEWLINE.join(traceback.format_tb(tb))}')
    sys.__excepthook__(exctype, value, tb)


sys.excepthook = log_uncaught_exception


def map_to_string(args):
    str_io = StringIO()
    for k, v in args.items():
        str_io.write(f'{k} = ')
        if isinstance(v, pd.DataFrame):
            str_io.write(f'\n')
            str_io_temp = StringIO()
            # just print first 15 columns, in case of huge no. of cols
            v.iloc[:, :15].head(3).to_csv(str_io_temp)
            # again limit text output to max 2000 chars, in case individual col data is large
            str_io.write((str_io_temp.getvalue())[:2000])
            str_io.write('\n')
        elif isinstance(v, str):
            str_io.write(f'{v[:200]}')
        else:
            str_io.write(f'{(str(v))[:500]}')
        str_io.write(f'\n')
    return str_io.getvalue()


try:
    display_funcs.html("")
except:
    log_info('importing display_funcs')
    try:
        import display_funcs
        from display_funcs import *
    except Exception as e:
        log_traceback()
        sys.stderr.write(
            f"Warning: Problem while importing display_funcs - {str(e)}\n")
