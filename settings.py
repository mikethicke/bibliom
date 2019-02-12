"""
Reads and stores settings from config file.
"""

import configparser

def load_settings(file_name=None):
    """
    Load settings from config file and return them.
    """
    if file_name is None:
        file_name = 'bibliodb.cfg'
    settings = configparser.ConfigParser()
    settings.read(file_name)
    return settings
