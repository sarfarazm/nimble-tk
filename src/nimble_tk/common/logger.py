import logging
import logging.handlers
import threading
import sys
import datetime
import traceback


class Logger:
    """
    A custom logger class for handling logging across an application.

    This class provides methods to log informational messages, errors, and tracebacks,
    either to console and/or file based on configuration. It supports dynamic log prefixes,
    formatting, and conditional console logging.

    Attributes:
        log_prefix (str): A prefix string added to each log message.
        logger (logging.Logger): Internal Python logger instance.
        console_log_on (bool): Flag to enable or disable console logging.
        replace_new_lines (str): String to replace newlines in log messages.
    """

    def __init__(self, log_file_path=None, console_log_on=False, max_bytes=50*1024*1024, backup_count=20,
                 logger=None, log_prefix='', replace_new_lines=None,
                 format_='%(asctime)s %(threadName)s %(filename)s:%(lineno)d: %(message)s'):
        """
        Initialize the Logger instance with specified configurations.

        Parameters:
            log_file_path (str): Path to the log file.
            console_log_on (bool): Flag to enable or disable logging to console.
            max_bytes (int): The maximum file size before rollover for file logging.
            backup_count (int): The number of backup files to keep.
            logger (logging.Logger): An existing logger instance, if any.
            log_prefix (str): Prefix string for each log message.
            replace_new_lines (str): String to replace newlines in log messages.
            format_ (str): Format string for log messages.
        """

        self.log_prefix = log_prefix
        self.console_log_on = console_log_on
        self.replace_new_lines = replace_new_lines
        if logger:
            self.logger = logger
            self.log_info(f"Log init with given logger")
        elif log_file_path:
            formatter = logging.Formatter(format_)
            handler = logging.handlers.RotatingFileHandler(
                log_file_path, maxBytes=max_bytes, backupCount=backup_count)
            handler.setFormatter(formatter)
            logger = logging.getLogger('info')
            logger.setLevel(logging.INFO)
            logger.addHandler(handler)
            self.logger = logger
            self.log_info(f"Log file init at {log_file_path}")

    def log_info(self, *msgs, **kwargs):
        """
        Log informational messages.

        This method logs messages at the INFO level. It supports logging both to the console and to a file.
        
        Parameters:
            msgs: Variable length argument list for error messages to be logged.
        """
        thread_log_prefix = get_thread_local_attribute('thread_log_prefix', '')

        log_to_file_only = False
        if 'file_only' in kwargs and kwargs['file_only']:
            log_to_file_only = True
        level_error = False
        if 'level_error' in kwargs and kwargs['level_error']:
            level_error = True

        local_console_logger, local_logger = sys.stdout, self.logger.info
        # if level is error, change console stream and logger level accordingly
        if level_error:
            local_console_logger = sys.stderr
            local_logger = self.logger.error

        log_msg = ' '.join([thread_log_prefix] + [str(msg) for msg in msgs])

        if self.replace_new_lines:
            log_msg = log_msg.replace("\n", self.replace_new_lines)

        if local_logger:
            local_logger(log_msg)

        if log_to_file_only:
            return

        if self.console_log_on:
            local_console_logger.write(
                f'{datetime.datetime.now()} :: {threading.current_thread().name} :: {self.log_prefix}')
            local_console_logger.write(log_msg)
            local_console_logger.write('\n')
            local_console_logger.flush()

    def log_info_file(self, *msgs):
        """
        Log informational messages to a file only and not to the console.
        Useful in case of Jupyter notebooks where we do not want to flood the browser with logs.

        This method logs messages at the INFO level to the configured log file without printing them to the console.

        Parameters:
            msgs: Variable length argument list for messages to be logged.
        """
        self.log_info(*msgs, file_only=True)

    def log_error(self, *msgs):
        """
        Log error messages.

        This method logs messages at the ERROR level. It supports logging both to the console and to a file.

        Parameters:
            msgs: Variable length argument list for error messages to be logged.
        """
        self.log_info(*msgs, level_error=True)

    def log_error_file(self, *msgs):
        """
        Log error messages to a file only and not to the console.
        Useful in case of Jupyter notebooks where we do not want to flood the browser with logs.

        This method logs messages at the ERROR level to the configured log file without printing them to the console.

        Parameters:
            msgs: Variable length argument list for error messages to be logged.
        """
        self.log_info(*msgs, level_error=True, file_only=True)

    def log_traceback(self, extra_info_str=''):
        """
        Log a traceback of the current exception with an optional additional message.

        This method is typically used in exception handling blocks to log the detailed traceback of an exception.
        It logs at the ERROR level and supports both console and file logging.

        Parameters:
            extra_info_str (str): Additional information to prepend to the traceback message.
        """
        error_msg = traceback.format_exc().strip().split('\n')
        self.log_error(f'{extra_info_str}', '::',
                       error_msg[-1], '::\n', '\n'.join(error_msg))

    def log_traceback_file(self, extra_info_str=''):
        """
        Log a traceback of the current exception to a file only and not to the console, 
        with an optional additional message.
        Useful in case of Jupyter notebooks where we do not want to flood the browser with logs.

        Similar to log_traceback, but this method logs the detailed traceback of an exception only to the configured log file.

        Parameters:
            extra_info_str (str): Additional information to prepend to the traceback message.
        """
        error_msg = traceback.format_exc().strip().split('\n')
        self.log_error_file(f'{extra_info_str}', '::',
                            error_msg[-1], '::\n', '\n'.join(error_msg))


# a thread-local object which can be used anywhere in the app
app_thread_local = threading.local()

def get_thread_local_attribute(attr_name, default_value=None):
    """
    Retrieve a thread-local attribute.

    This function fetches the value of a thread-local attribute. If the attribute is not set,
    it returns a default value.

    Parameters:
    attr_name (str): The name of the attribute to retrieve.
    default_value (optional): The default value to return if the attribute is not set.

    Returns:
    The value of the thread-local attribute or the default value if the attribute is not set.
    """
    return getattr(app_thread_local, attr_name, default_value)

def set_thread_local_attribute(attr_name, value):
    """
    Set a thread-local attribute.

    This function assigns a value to a thread-local attribute. It can be used to store data 
    that is specific to a particular thread.

    Parameters:
    attr_name (str): The name of the attribute to set.
    value: The value to set for the attribute.

    Returns:
    None
    """
    return setattr(app_thread_local, attr_name, value)

def set_thread_log_prefix(prefix):
    """
    Set a logging prefix for the current thread.

    This function sets a thread-specific logging prefix, which can be used to differentiate 
    logs coming from different threads.

    Parameters:
    prefix (str): The logging prefix to set for the current thread.

    Returns:
    None
    """
    set_thread_local_attribute('thread_log_prefix', prefix)


default_logger = Logger(log_file_path=None, console_log_on=True)


def init_file_logger(log_file_path, console_log_on=False, max_bytes=50*1024*1024, backup_count=10,
                     logger=None, log_prefix='', replace_new_lines=None):
    """
        Initialize a file logger with specified configurations.

        This function sets up a global logger instance with the option to log to a file and/or console. It configures
        aspects like file path for logging, log rotation based on file size, and backup count for log files. Optionally,
        it can use an existing logger instance.

        Parameters:
        log_file_path (str): Path to the log file.
        console_log_on (bool): Flag to enable or disable logging to the console. Defaults to False.
        max_bytes (int): The maximum file size in bytes before rolling over. Defaults to 50MB.
        backup_count (int): The number of backup files to keep. Defaults to 10.
        logger (Logger, optional): An existing logger instance. If not provided, a new one is created.
        log_prefix (str, optional): A prefix string to be added to each log message.
        replace_new_lines (str, optional): If true, replaces newlines with 
            '\\n' characters. Helpul in cases where we do not want the log 
            line to be split into multiple lines.

        Returns:
            None
    """
    global default_logger
    default_logger = Logger(log_file_path, console_log_on=console_log_on, max_bytes=max_bytes,
                            backup_count=backup_count, logger=logger, log_prefix=log_prefix,
                            replace_new_lines=replace_new_lines)
    

def log_info(*msgs, **kwargs):
    """
    Log informational messages to a file only and not to the console.
    Useful in case of Jupyter notebooks where we do not want to flood the browser with logs.

    This method logs messages at the INFO level to the configured log file without printing them to the console.

    Parameters:
        msgs: Variable length argument list for messages to be logged.
    """
    default_logger.log_info(*msgs, **kwargs)


def log_info_file(*msgs):
    """
    Log informational messages to a file only and not to the console.
    Useful in case of Jupyter notebooks where we do not want to flood the browser with logs.

    This method logs messages at the INFO level to the configured log file without printing them to the console.

    Parameters:
        msgs: Variable length argument list for messages to be logged.
    """
    default_logger.log_info_file(*msgs)


def log_error(*msgs):
    """
    Log error messages.

    This method logs messages at the ERROR level. It supports logging both to the console and to a file.

    Parameters:
        msgs: Variable length argument list for error messages to be logged.
    """
    default_logger.log_error(*msgs)


"""
Log error messages to a file only and not to the console.
Useful in case of Jupyter notebooks where we do not want to flood the browser with logs.

This method logs messages at the ERROR level to the configured log file without printing them to the console.

Parameters:
    msgs: Variable length argument list for error messages to be logged.
"""
def log_error_file(*msgs):
    default_logger.log_error_file(*msgs)


"""
Log a traceback of the current exception with an optional additional message.

This method is typically used in exception handling blocks to log the detailed traceback of an exception.
It logs at the ERROR level and supports both console and file logging.

Parameters:
    extra_info_str (str): Additional information to prepend to the traceback message.
"""
def log_traceback(extra_info_str=''):
    default_logger.log_traceback(extra_info_str)


def log_traceback_file(extra_info_str=''):
    """
    Log a traceback of the current exception to a file only and not to the console, 
    with an optional additional message.
    Useful in case of Jupyter notebooks where we do not want to flood the browser with logs.

    Similar to log_traceback, but this method logs the detailed traceback of an exception only to the configured log file.

    Parameters:
        extra_info_str (str): Additional information to prepend to the traceback message.
    """
    default_logger.log_traceback_file(extra_info_str='')
