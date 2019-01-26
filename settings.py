"""
Reads and stores settings from config file.
"""

import configparser

def load_settings():
    """
    Load settings from config file and return them.
    """
    settings = configparser.ConfigParser()
    settings.read('bibliodb.cfg')
    return settings

