"""
Unit tests for parser_db_adapter.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition

import pytest

from bibliom import parser_db_adapter
from bibliom.parsers import Parser
from bibliom import dbmanager
from bibliom import publication_objects

@pytest.mark.usefixtures('file_paths')
@pytest.mark.usefixtures('class_manager')
class TestParserDBAdapter():
    """
    Unit tests for parser_db_adapter.py
    """
    def test_wok_to_db(self):
        self.manager.reset_database()
        parser_class = Parser.get_parser_for_file(self.file_paths['WOK']['file'])
        parser = parser_class()
        parser.parse_file(self.file_paths['WOK']['file'])
        parser_db_adapter._wok_to_db(
            parser,
            self.manager,
            duplicates=dbmanager.DBTable.Duplicates.SKIP
        )
        papers = publication_objects.Paper.fetch_entities(
            db_table=self.manager.get_table_object('paper'),
            where_dict={'title': 'NOT NULL'}
        )
        assert len(papers) == 500
