import os


def get_default_data_path():
    """
    Returns the absolute path to the data folder
    :return:
    """
    return f'{os.path.dirname(os.path.realpath(__file__))}/../data'


def get_default_log_path():
    """
    Returns the absolute path to the log folder
    :return:
    """
    return f'{os.path.dirname(os.path.realpath(__file__))}/../logs'
