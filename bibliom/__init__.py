"""
This package contains scripts for parsing, storing, and analyzing bibliographic
information, especially for the purposes of citation analysis.s
"""
import logging
from logging.handlers import RotatingFileHandler
import os

DEBUG_MODE = True

# Adding custom log level for verbose info
# https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility
VERBOSE_INFO = 15
logging.addLevelName(VERBOSE_INFO, "VERBOSE_INFO")
setattr(logging, 'VERBOSE_INFO', VERBOSE_INFO)

def verbose_info(self, message, *args, **kws):
    """
    Callback for logging verbose info messages.
    """
    if self.isEnabledFor(VERBOSE_INFO):
        # Yes, logger takes its '*args' as 'args'.
        self._log(VERBOSE_INFO, message, args, **kws)

logging.Logger.verbose_info = verbose_info

logging.getLogger(__name__).addHandler(logging.NullHandler())

if DEBUG_MODE:
    debug_handler = RotatingFileHandler(os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),
        'logs',
        'debug.log'
    ), maxBytes=200000, backupCount=10)
    debug_formatter = logging.Formatter(
        '%(asctime)s - %(name)s(%(lineno)04d) - %(levelname)s - %(message)s'
    )
    debug_handler.setLevel(logging.DEBUG)
    debug_handler.setFormatter(debug_formatter)
    logging.getLogger(__name__).addHandler(debug_handler)
    logging.getLogger(__name__).setLevel(logging.DEBUG)

logging.getLogger(__name__).debug("Init complete.")
