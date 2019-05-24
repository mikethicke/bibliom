"""
This module contains exception classes for biblio-package exceptions.
"""
import MySQLdb

class BiblioException(Exception):
    """
    Base class for biblio exceptions.
    """

class UnknownDatabaseError(BiblioException, MySQLdb.OperationalError):
    """
    Raised when attempt to connect to database but that database does not
    exist.
    """

class FailedDatabaseCreationError(BiblioException):
    """
    Raised when attempt to create new database but failed.
    """

class DBUnsyncedError(BiblioException):
    """
    Raised when operation requires first syncing to db, but no sync has been
    done.
    """

class DBIntegrityError(BiblioException):
    """
    Raise when an assumption about the content of the database has been violated,
    suggesting that the integrity of its data is suspect.
    """

class ParsingError(BiblioException):
    """
    Raised when there is an error parsing records.
    """

class FileParseError(ParsingError):
    """
    Raised when a text file cannot be decoded or parsed.
    """
