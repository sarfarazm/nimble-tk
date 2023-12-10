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

from .logger import *


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


# ------------------------------------------------

class TempClass(object):
    pass


# ------------------------------------------------

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

import pickle 
import base64 


def pickle_dumps_base64(obj: object) -> str:
    """Pickles the given object and converts the result to a base64 encoded 
    string

    Args:
        obj (object): The object to pickle

    Returns:
        str: base64 encoded pickled representation of the given object
    """
    return base64.b64encode(pickle.dumps(obj)).decode()


def pickle_loads_base64(base64_str: str) -> object:
    """Performs the reverse operation of the `pickle_dumps_base64` method.
    base64 decodes the given string and then unpickles the binary data back to
    a python object.

    Args:
        base64_str (str): base64 encoded pickled representation of an object

    Returns:
        object: The unpickled object
    """
    return pickle.loads(base64.b64decode(base64_str))


def reload_modules(module_list:list=[]):
    """This functions helps in hot reloading a module which has been recently modified.

    Args:
        module_list (list, optional): List of names of modules in string.
        e.g. module_list = ['general_utils']
    """
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


from hashlib import sha256
def hash(obj:object) -> str:
    """Create sha256 hash of the given object. 

    If the object is not a string or a number, it is first converted into a 
    base64 encoded pickle string.

    Args:
        obj (object): The object to hash

    Returns:
        str: The hash value 
    """
    value = obj
    if not isinstance(obj, int) and not isinstance(obj, int) and not isinstance(obj, float):
        value = pickle_dumps_base64(obj)
    return sha256(value.encode('utf-8')).hexdigest()


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


# try:
#     display_funcs.html("")
# except:
#     log_info('importing display_funcs')
#     try:
#         import display_funcs
#         from display_funcs import *
#     except Exception as e:
#         log_traceback()
#         sys.stderr.write(
#             f"Warning: Problem while importing display_funcs - {str(e)}\n")
