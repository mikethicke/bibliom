"""
Unit tests for parsers.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition

import logging

import pytest

from bibliom.parsers import Parser, WOKParser, WCHParser
from bibliom import exceptions

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
        parser = Parser.get_parser_for_content(self.test_data['WOK'])
        assert isinstance(parser, WOKParser)

        parser = Parser.get_parser_for_content(self.test_data['junk'])
        assert parser is None

        parser = Parser.get_parser_for_content('')
        assert parser is None

        with pytest.raises(TypeError):
            parser = Parser.get_parser_for_content([])

    def test_get_parser_from_format_arg(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_get_parser_from_format_arg')
        parser = Parser.get_parser_from_format_arg('WOK')
        assert isinstance(parser, WOKParser)
        parser = Parser.get_parser_from_format_arg('WCH')
        assert isinstance(parser, WCHParser)

        parser = Parser.get_parser_from_format_arg('asdfdsa')
        assert parser is None

        parser = Parser.get_parser_from_format_arg('')
        assert parser is None

        with pytest.raises(TypeError):
            parser = Parser.get_parser_from_format_arg([])

    def test_get_parser_for_file(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_get_parser_for_file')

        parser = Parser.get_parser_for_file(self.file_paths['WOK']['file'])
        assert isinstance(parser, WOKParser)

        parser = Parser.get_parser_for_file(self.file_paths['WCH']['file'])
        assert isinstance(parser, WCHParser)

        parser = Parser.get_parser_for_file(self.file_paths['junk']['file'])
        assert parser is None

        with pytest.raises(FileNotFoundError):
            parser = Parser.get_parser_for_file('not-a-file-path')

    def test_get_parser_for_directory(self):
        logging.getLogger('bibliom.pytest').debug('-->TestParser.test_get_parser_for_directory')

        parser = Parser.get_parser_for_directory(self.file_paths['WOK']['dir'])
        assert isinstance(parser, WOKParser)

        parser = Parser.get_parser_for_directory(self.file_paths['WCH']['dir'])
        assert isinstance(parser, WCHParser)

        parser = Parser.get_parser_for_directory(self.file_paths['junk']['dir'])
        assert parser is None

        with pytest.raises(FileNotFoundError):
            parser = Parser.get_parser_for_directory('not-a-file-path')

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

        with pytest.raises(exceptions.FileParseError):
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

@pytest.mark.usefixtures('test_data')
@pytest.mark.usefixtures('file_paths')
class TestWCHParser():
    """
    Unit tests for WCHParser class.
    """
    def test_format_arg(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWCHParser.test_format_arg')
        assert WCHParser.format_arg() == 'WCH'

    def test_csv_line_to_list(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWCHParser.test_csv_line_to_list')
        test_line = '"RETRACTED: Ileal-lymphoid-nodular hyperplasia, non-specific colitis, and pervasive developmental disorder in children (Retracted article. See vol 375, pg 445, 2010)","Wakefield, AJ; Murch, SH; Anthony, A; Linnell, J; Casson, DM; Malik, M; Berelowitz, M; Dhillon, AP; Thomson, MA; Harvey, P; Valentine, A; Davies, SE; Walker-Smith, JA","","","","LANCET","FEB 28 1998","1998","351","9103","","","","637","641","","10.1016/S0140-6736(97)11096-0","","","1233","58.71","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","0","34","28","31","53","70","63","60","47","52","56","54","61","80","86","80","65","70","70","58","58","56"'
        line_list = WCHParser._csv_line_to_list(test_line)
        assert isinstance(line_list, list)
        for item in line_list:
            assert isinstance(item, str)
        for i, ch in enumerate(line_list[0]):
            assert ch == test_line[i+1]
    
    def test_is_parsable_file(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWCHParser.test_is_parsable_file') 
        parser = WCHParser()
        assert parser.is_parsable_file(self.file_paths['WCH']['file'])
        assert not parser.is_parsable_file(self.file_paths['WOK']['file'])
        assert not parser.is_parsable_file(self.file_paths['junk']['file'])

    def test_parse_file(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWCHParser.test_parse_file')
        parser = WCHParser('DOI')
        parser.parse_file(self.file_paths['WCH']['file'])
        record_dict = parser.parsed_dict['10.1038/nature07404']
        assert record_dict['Source Title'] == 'NATURE'
        assert len(record_dict['Citation History'].items()) == 39
        assert record_dict['Citation History'][2018] == 8

        with pytest.raises(exceptions.FileParseError):
            parser.parse_file(self.file_paths['junk']['file'])

    def test_parse_directory(self):
        logging.getLogger('bibliom.pytest').debug('-->TestWCHParser.test_parse_file')
        parser = WCHParser('DOI')
        parser.parse_directory(self.file_paths['WCH']['dir'])
        assert parser.parsed_dict['10.1172/JCI60214']['Source Title'] == 'JOURNAL OF CLINICAL INVESTIGATION'
        assert parser.parsed_dict['10.1172/JCI60214']['Citation History'][2017] == 7

        parser2 = WCHParser()
        parser2.parse_directory(self.file_paths['junk']['dir'])
        assert not parser2.parsed_list
 











