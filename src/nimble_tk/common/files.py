import base64
import pickle
import pathlib
import os
import datetime

import logger as logger

# File utilities
import shutil


def rmdir(path):
    """
    Remove a directory and all its contents.

    This function deletes the specified directory path along with all its contents. It checks if the path exists 
    before attempting to remove it to prevent errors.

    Parameters:
    path (str): The file system path to the directory to be removed.

    Returns:
    None
    """
    if os.path.exists(path):
        shutil.rmtree(path)


def is_file_recent_enough(file_path: str, max_age_days: int) -> bool:
    """Utility function to check if the given file is older than the given 
    number of days.

    Args:
        file_path (str): Path of the file to check
        max_age_days (int): Max age in days beyond which to return False 

    Returns:
        bool: False if the file is older than the given number of days. 
              True otherwise. 
    """
    path = pathlib.Path(file_path)
    if not path.exists():
        return False
    last_modified_ts = path.stat().st_mtime
    diff_days = (datetime.datetime.now().timestamp() -
                 last_modified_ts) / (60*60*24)
    if diff_days < max_age_days:
        logger.log_info(
            f"File: {file_path} - age_days: {diff_days} - recent enough (required {max_age_days})")
        return True
    else:
        logger.log_info(
            f"File: {file_path} - age_days: {diff_days} - not recent enough (required {max_age_days})")
        return False


def read_file(filepath: str) -> str:
    """Simply reads a file's contents. Useful for reading small text files.

    Args:
        filepath (str): The location of the file to read

    Returns:
        str: A string containing all of the file's contents
    """
    with open(filepath, 'r') as reader:
        return '\n'.join(reader.readlines())


def write_to_file(filepath: str, content: object, strip: bool = True) -> None:
    """
    Simply writes the given content to the given file. 
    Useful for reading small text files. 

    Args:
        filepath (str): The location of the file to write
        content (object): If the given variable is not a string, 
            it is converted to string using str(content).

    Returns:
        None
    """
    if type(content) != str:
        content = str(content)
    with open(filepath, 'w') as writer:
        writer.write(content.strip())


def append_to_file(filepath, content, strip=True):
    """
    Simply appends the given content to the given file. 
    Useful for reading small text files. 

    Args:
        filepath (str): The location of the file to write
        content (object): If the given variable is not a string, 
            it is converted to string using str(content).

    Returns:
        None
    """
    if type(content) != str:
        content = str(content)
    with open(filepath, 'a') as appender:
        appender.write(content.strip())

# ------------------------------------


def pickle_dump(obj: object, filepath: str, remove: list = []) -> None:
    """A wrapper around pickle.dump(). 

    Args:
        obj (object): The object to pickle
        filepath (str): The path to the pickle file 
        remove (list, optional): To remove any variables from the object 
            before pickling.

    Returns:
        None
    """
    for key in remove:
        obj.__dict__.pop(key)
    with open(filepath, 'wb') as file:
        pickle.dump(obj, file)


def pickle_load(filepath: str, put: dict = {}) -> object:
    """A wrapper around pickle.load(). 

    Args:
        filepath (str): The path to the pickle file 
        remove (list, optional): To remove any variables from the object 
            before pickling.

    Returns:
        object: The unpickled object
    """
    with open(filepath, 'rb') as file:
        obj = pickle.load(file)
    for key, attr in put.items():
        obj.__dict__[key] = attr
    return obj


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
