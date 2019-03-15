"""
Package-wide constants.
"""

# Above this number of table rows, will log progress when syncing to db.
INFO_THRESHOLD = 10000

# Frequency to log progress when syncing to db.
REPORT_FREQUENCY = 5000

# Max retries on database queries
MAX_DB_RETRIES = 5
