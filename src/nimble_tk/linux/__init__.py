import zlib
import datetime

# import common
from .. import common

from typing import Optional, Callable, Tuple, Any
import datetime

"""
Utility functions for Linux 
"""

import subprocess

# for remote ssh, keys need to be exchanged between systems


def run_command(command: str, log: bool = True, 
                log_info: Callable[[str], Any] = common.log_info, 
                env: Optional[dict] = None, 
                fail_on_error: bool = True, 
                track_time: bool = False
            ) -> Tuple[int, Optional[datetime.timedelta]]:
    """
    A utility function to execute linux commands.

    Example usage:
        run_command("aws s3 sync s3://... ")
        or
        cmd("ls -lrth")

    Args:
        command (str): The command to be executed.
        log (bool): Flag to enable logging.
        log_info (Callable[[str], Any]): Logging function to use. Can be overriden 
            using `common.log_info_file` to avoid logging on jupyter 
            notbook console.
        env (Optional[dict]): Environment variables for the command.
        fail_on_error (bool): If True, raises an error if the command execution fails.
        track_time (bool): If True, tracks and returns the execution time.

    Returns:
        Tuple[int, Optional[datetime.timedelta]]: A tuple containing the return code of the command, 
        and the elapsed time (datetime.timedelta) if track_time is True, else None.
    """
    if track_time:
        start = datetime.datetime.now()
    final_command = command

    if log:
        log_info('Executing command: %s - env: %s' % (final_command, env))
    p = subprocess.Popen(final_command, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, stdin=None, shell=True, env=env)
    for line in iter(p.stdout.readline, b''):
        if log:
            log_info(line.decode('utf-8').strip())
    p.terminate()
    p.wait()
    if p.returncode != 0 and fail_on_error:
        raise ValueError('Process exit status: %s' % p.returncode)
    if log:
        log_info('command return output', p.returncode)

    if track_time:
        end = datetime.datetime.now()
        elapsed = end - start
        return p.returncode, elapsed
    else:
        return p.returncode


cmd = run_command


def stream_gzip_decompress(stream):
    """
    Generator function that decompresses a gzip-compressed stream.

    Args:
        stream (iterable): An iterable stream of compressed data.

    Yields:
        str: Decompressed data, line by line.
    """
    # offset 32 to skip the header
    dec = zlib.decompressobj(32 + zlib.MAX_WBITS)
    rem = ''
    for chunk in stream:
        rv = rem + dec.decompress(chunk)
        if rv:
            split = rv.split('\n')
            for line in split[:-1]:
                yield line
            rem = split[-1]


def run_command_return_iter(command, compress=False, env=None, log=True):
    """
        Executes a command and returns an iterator to its output, optionally compressing the output.

        Args:
            command (str): The command to execute.
            compress (bool): If True, compresses the command output.
            env (dict, optional): Environment variables for the command.
            log (bool): Flag to enable logging.

        Returns:
            tuple: A tuple containing an iterator to the command output and the process object.
    """

    if compress:
        command += ' | gzip -c '
    final_command = command

    if log:
        log.log_info('Executing command: %s' % final_command)
    p = subprocess.Popen(final_command, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, stdin=None, shell=True, env=env)
    if not compress:
        return common.IterWrapper(iter(p.stdout.readline, b'')), p
    else:
        return common.IterWrapper(stream_gzip_decompress(iter(p.stdout.readline, b''))), p

# for remote ssh, keys need to be exchanged between systems


def run_command_iter_output(command, log=True, env=None):
    """
    Executes a command and yields its output line by line.

    Args:
        command (str): The command to execute.
        log (bool): Flag to enable logging.
        env (dict, optional): Environment variables for the command.

    Yields:
        str: The output of the command, line by line.
    """

    final_command = command
    if log:
        log.log_info('Executing command: %s, env: %s' % (final_command, env))
    p = subprocess.Popen(final_command, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, stdin=None, shell=True, env=env)
    for line in iter(p.stdout.readline, b''):
        yield line.decode('utf-8')
    p.terminate()
    p.wait()
    if log:
        log.log_info('command return output', p.returncode)


def run_command_return_output(command, log=True):
    """
    Executes a command and returns its output as a single string.

    Args:
        command (str): The command to execute.
        log (bool): Flag to enable logging.

    Returns:
        str: The output of the command.
    """
    output_list = []
    for line in run_command_iter_output(command, log=log):
        line = line.strip('\n')
        output_list.append(line)
    return '\n'.join(output_list)
