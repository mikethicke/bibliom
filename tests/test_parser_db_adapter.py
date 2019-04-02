"""
Unit tests for parser_db_adapter.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition
import logging

import pytest

from bibliom import parser_db_adapter
from bibliom.parsers import Parser, WOKParser, WCHParser
from bibliom.dbtable import DBTable
from bibliom import publication_objects

@pytest.mark.usefixtures('file_paths')
@pytest.mark.usefixtures('class_manager')
class TestParserDBAdapter():
    """
    Unit tests for parser_db_adapter.py
    """
    def test_wok_to_db(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParserDBAdapter.test_wok_to_db')
        self.manager.reset_database()
        parser = Parser.get_parser_for_file(self.file_paths['WOK']['file'])
        assert isinstance(parser, WOKParser)
        parser.parse_file()
        parser_db_adapter.parsed_records_to_db(
            parser,
            self.manager,
            duplicates=DBTable.Duplicates.INSERT
        )
        papers = publication_objects.Paper.fetch_entities(
            db_table=DBTable.get_table_object(self.manager, 'paper'),
            where_dict={'title': 'NOT NULL'}
        )
        assert len(papers) == 500

    def test_wch_to_db(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParserDBAdapter.test_wch_to_db')
        self.manager.reset_database()
        parser = Parser.get_parser_for_file(self.file_paths['WCH']['file'])
        assert isinstance(parser, WCHParser)
        parser.parse_file()
        parser_db_adapter.parsed_records_to_db(
            parser,
            self.manager
        )
        papers = publication_objects.Paper.fetch_entities(
            db_table=DBTable.get_table_object(self.manager, 'paper'),
            where_dict={'title': 'NOT NULL', 'citation_history': 'NOT NULL'}
        )
        assert len(papers) == 500
