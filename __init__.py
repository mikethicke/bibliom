"""
This package contains scripts for parsing, storing, and analyzing bibliographic
information, especially for the purposes of citation analysis.s
"""
import logging

# Adding custom log level for verbose info
# https://stackoverflow.com/questions/2183233/how-to-add-a-custom-loglevel-to-pythons-logging-facility
VERBOSE_INFO = 15
logging.addLevelName(VERBOSE_INFO, "VERBOSE_INFO")
def verbose_info(self, message, *args, **kws):
    """
    Callback for logging verbose info messages.
    """
    if self.isEnabledFor(VERBOSE_INFO):
        # Yes, logger takes its '*args' as 'args'.
        self._log(VERBOSE_INFO, message, args, **kws) 
logging.Logger.verbose_info = verbose_info

logging.getLogger(__name__).addHandler(logging.NullHandler())
