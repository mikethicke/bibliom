"""
Script for importing bibliographic records into an SQL database.

Import citation records located at file or directory or url into database. If
no database is specified, use database from configuration file (bibiodb.cfg by
default).

Options:

-d <name>   : Database name
-u <name>   : Database username
-p <pw>     : Database password

-c          : Clear all records before import (dangerous!)
-o          : Overwrite duplicates
-s          : Skip duplicates
-m          : Merge duplicates

-f <format> : Import records of <format> (see below). If no format is specified,
              script will attempt to guess format from first record.

-g <file>   : Use configuration file <file>

-l <file>   : Log to file
-v          : Verbose
-q          : Quiet

-h          : Print this help text.

Formats:
    WOK         : Web of Science / Web of Knowledge
    WOKCH       : Web of Science / Web of Knowldge citation history
    CR          : Crossref
"""

import os
import argparse
from textwrap import dedent

import parsers
import dbmanager
import settings
import parser_db_adapter
import exceptions

def parse_args():
    """
    Parses command line arguments using argparse and returns parser object
    with parsed arguments.
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Script for importing bibliographic records into an SQL database.",
        epilog=dedent(
            """
            Available formats:
                WOK         : Web of Science / Web of Knowledge
                WOKCH       : Web of Science / Web of Knowldge citation history
                CR          : Crossref
            """
        ))

    parser.add_argument(
        "target",
        metavar="file|directory",
        help="File or directory to parse and store in database.")
    parser.add_argument(
        "-d", "--database",
        help="Database name.")
    parser.add_argument(
        "-u", "--user",
        help="Database username.")
    parser.add_argument(
        "-p", "--password",
        help="Database password.")
    parser.add_argument(
        "-r", "--recursive",
        help="Recursively parse directories",
        action="store_true"
    )
    parser.add_argument(
        "-c", "--clear",
        help="Drop all records before import (dangerous!)",
        action="store_true")
    duplicates = parser.add_mutually_exclusive_group()
    duplicates.add_argument(
        "-o", "--overwrite",
        help="Overwrite duplicate records.",
        action="store_true")
    duplicates.add_argument(
        "-s", "--skip",
        help="Skip duplicate records (default).",
        action="store_true",
        default=True)
    duplicates.add_argument(
        "-m", "--merge",
        help="Merge duplicate records.",
        action="store_true")
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument(
        "-v", "--verbose",
        help="Output detailed progress information to console."
    )
    verbosity.add_argument(
        "-q", "--quiet",
        help="Output no progress information to console."
    )
    available_formats = [bparser.format_arg() for bparser in parsers.Parser.parser_classes()]
    parser.add_argument(
        "-f", "--format",
        help="Import records matching format (see below).",
        choices=available_formats)
    parser.add_argument(
        "-g", "--config",
        help="Use configuration file.")
    parser.add_argument(
        "-l", "--log",
        help="Log to file",
        nargs="?",
        const="console")

    return parser.parse_args()

def resolve_options():
    """
    Combines options set in configuration file and command line arguments
    and returns a dictionary of options. Command line arguments take precedence
    over configuration file options. Configuration file section is determined
    by database name in command line.
    """
    options = {}
    parsed_args = parse_args()
    if parsed_args.config:
        config_file = parsed_args.config
    else:
        config_file = None
    config_settings = settings.load_settings(config_file)
    if parsed_args.database and parsed_args.database in config_settings.sections():
        config_section = parsed_args.database 
    else:
        config_section = 'DEFAULT'
    config_items = config_settings.items(config_section)
    options = {key:value for (key, value) in config_items}
    parsed_args_dict = vars(parsed_args)
    for key, value in parsed_args_dict.items():
        if options.get(key) is None or value is not None:
            options[key] = value

    return options

def get_parser(options):
    """
    Gets instance of parser.
    """
    parser_class = None
    if options['format']:
        parser_class = parsers.Parser.get_parser_from_format_arg(options['format'])
    elif os.path.isfile(options['target']):
        parser_class = parsers.Parser.get_parser_for_file(options['target'])
    elif os.path.isdir(options['target']):
        parser_class = parsers.Parser.get_parser_for_directory(options['target'])
    if not parser_class:
        raise SystemExit
    return parser_class()

def main():
    """
    Main program
    """
    options = resolve_options()
    if options['clear']:
        answer = input(
            "Delete ALL existing records from database? This cannot be undone."
            + " (Enter 'yes' to confirm): ")
        if answer.upper() != "YES":
            raise SystemExit

    try:
        manager = dbmanager.DBManager(options['database'], options['user'], options['password'])
        must_create_database = False
    except exceptions.UnknownDatabaseError:
        manager = None
        must_create_database = True
    
    if must_create_database:
        answer = input("Database %s not found. Create as new database (y/n)? " % options['database'])
        if answer.upper() == "Y" or answer.upper() == "YES":
            manager = dbmanager.DBManager(
                name=None,
                user=options['user'],
                password=options['password']
            )
            manager.create_database(options['database'])
        else:
            raise SystemExit
    
    if options['clear']:
        manager.reset_database()
    
    the_parser = get_parser(options)

    if os.path.isfile(options['target']):
        the_parser.parse_file(options['target'])
    elif os.path.isdir(options['target']):
        if options['recursive']:
            the_parser.recursive_parse(options['target'])
        else:
            the_parser.parse_directory(options['target'])
    else:
        raise SystemExit
    
    parser_db_adapter.parsed_records_to_db(the_parser, manager)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass