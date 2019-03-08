"""
Unit tests for parsers.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition

import logging

import pytest

from bibliom.parsers import Parser, WOKParser

@pytest.mark.usefixtures('test_data')
@pytest.mark.usefixtures('file_paths')
class TestParser():
    """
    Unit tests for Parser class.

    Parser is an abstract class so tests of instance methods will be done through subclasses.
    """
    def test_parser_classes(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_parser_classes')
        parser_classes = Parser.parser_classes()
        assert isinstance(parser_classes, list)
        parser_instance = parser_classes[0]()
        isinstance(parser_instance, Parser)

    def test_get_parser_for_content(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_get_parser_for_content')
        parser_class = Parser.get_parser_for_content(self.test_data['WOK'])
        parser = parser_class()
        assert isinstance(parser, WOKParser)

        parser_class = Parser.get_parser_for_content(self.test_data['junk'])
        assert parser_class is None

        parser_class = Parser.get_parser_for_content('')
        assert parser_class is None

        with pytest.raises(TypeError):
            parser_class = Parser.get_parser_for_content([])

    def test_get_parser_from_format_arg(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_get_parser_from_format_arg')
        parser_class = Parser.get_parser_from_format_arg('WOK')
        parser = parser_class()
        assert isinstance(parser, WOKParser)

        parser_class = Parser.get_parser_from_format_arg('asdfdsa')
        assert parser_class is None

        parser_class = Parser.get_parser_from_format_arg('')
        assert parser_class is None

        with pytest.raises(TypeError):
            parser_class = Parser.get_parser_from_format_arg([])

    def test_get_parser_for_file(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_get_parser_for_file')
        parser_class = Parser.get_parser_for_file(self.file_paths['WOK']['file'])
        parser = parser_class()
        assert isinstance(parser, WOKParser)

        parser_class = Parser.get_parser_for_file(self.file_paths['junk']['file'])
        assert parser_class is None

        with pytest.raises(FileNotFoundError):
            parser_class = Parser.get_parser_for_file('not-a-file-path')

    def test_get_parser_for_directory(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_get_parser_for_directory')
        parser_class = Parser.get_parser_for_directory(self.file_paths['WOK']['dir'])
        parser = parser_class()
        assert isinstance(parser, WOKParser)

        parser_class = Parser.get_parser_for_directory(self.file_paths['junk']['dir'])
        assert parser_class is None

        with pytest.raises(FileNotFoundError):
            parser_class = Parser.get_parser_for_directory('not-a-file-path')

@pytest.mark.usefixtures('test_data')
@pytest.mark.usefixtures('file_paths')
class TestWOKParser():
    """
    Tests for WOKParser class.
    """
    def test_parse_content_item(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWOKParser.test_parse_content_item')
        parser = WOKParser()
        parser.parse_content_item(self.test_data['WOK'])
        assert parser.parsed_list[0]['DOI'] == '10.1021/ja402927u'
        assert parser.parsed_list[0]['Document Title'] == (
            'RETRACTED: Mutational Analysis of 48G7 Reveals that Somatic ' +
            'Hypermutation Affects Both Antibody Stability and Binding Affinity ' +
            '(Retracted article. See vol. 140, pg. 1976, 2018)'
        )
        assert parser.parsed_list[0]['Authors'][0] == 'Sun, SB'

        parser = WOKParser('Unique Article Identifier')
        parser.parse_content_item(self.test_data['WOK'])
        assert parser.parsed_dict['WOS:000321810400005']['DOI'] == '10.1021/ja402927u'
        assert parser.parsed_list[0]['DOI'] == '10.1021/ja402927u'

        parser = WOKParser('Non-existent-field')
        parser.parse_content_item(self.test_data['WOK'])
        assert parser.parsed_dict['id-0']['DOI'] == '10.1021/ja402927u'

    def test_is_parsable_file(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWOKParser.test_is_parsable_file')
        parser = WOKParser()
        assert parser.is_parsable_file(self.file_paths['WOK']['file'])
        assert not parser.is_parsable_file(self.file_paths['junk']['file'])

    def test_parse_file(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWOKParser.test_parse_file')
        parser = WOKParser("DOI")
        parser.parse_file(self.file_paths['WOK']['file'])
        record_dict = parser.parsed_dict['10.1016/j.solmat.2018.05.055']
        assert record_dict['Unique Article Identifier'] == 'WOS:000437816100063'

        with pytest.raises(ValueError):
            parser.parse_file(self.file_paths['junk']['file'])

    def test_parse_directory(self):
        """
        Also tests base Parser class & Parser.parse_files.
        """
        logging.getLogger('bibliom.pytest').debug('-->TestWOKParser.test_parse_directory')
        parser = WOKParser("DOI")
        parser.parse_directory(self.file_paths['WOK']['dir'])
        assert (
            parser.parsed_dict['10.1080/1068316X.2017.1360889']['Unique Article Identifier'] ==
            "WOS:000432698200001")
        parser2 = WOKParser()
        parser2.parse_directory(self.file_paths['junk']['dir'])
        assert not parser2.parsed_list

    def test_parse_directories(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWOKParser.test_parse_directories')
        parser = WOKParser("Unique Article Identifier")
        parser.parse_directories(self.file_paths['WOK']['dirs'])
        assert parser.parsed_dict['WOS:000299096400065']['DOI'] == '10.1016/j.egypro.2011.10.265'

    def test_recursive_parse(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWOKParser.test_recursive_parse')
        parser = WOKParser("Unique Article Identifier")
        parser.recursive_parse(self.file_paths['WOK']['dir'])
        assert parser.parsed_dict['WOS:000299096400065']['DOI'] == '10.1016/j.egypro.2011.10.265'
