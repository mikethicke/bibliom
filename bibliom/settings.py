"""
Reads and stores settings from config file.
"""
import os
import configparser

def load_settings(file_name=None):
    """
    Load settings from config file and return them.
    """
    if file_name is None:
        file_name = os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            'config',
            'bibliodb.cfg')
    settings = configparser.ConfigParser()
    settings.read(file_name)
    return settings

def get_default_settings_dict(file_name=None):
    """
    Returns default settings from config file as dict.
    """
    settings = load_settings(file_name)
    config_items = settings.items('DEFAULT')
    options = {key:value for (key, value) in config_items}
    return options

def get_settings_for_config(config_name, file_name=None):
    """
    Returns settings for database as dict.
    """
    settings = load_settings(file_name)
    config_items = settings.items(config_name)
    options = {key:value for (key, value) in config_items}
    return options
