"""
Parsers transform inputs from various sources to (lists of) dictionaries.
The parsers in this module don't interact with the database or publication
objects directly.
"""

from abc import ABC, abstractmethod
import os
import re
import chardet

def detect_encoding(data):
    """
    Returns string containing (best guess of) text's encoding.
    """
    return chardet.detect(data)['encoding']

class Parser(ABC):
    """
    A parser parses publication items (articles, books, etc.) into dictionaries.
    Each parsed item is added to the parsed_items list. If an ID field is designated,
    parsed items are instead added to a dictionary indexed by that ID.
    """
     # Prefix for missing ids when parsing to dict. Followed by an incrimenting integer for each
     # record not containing the index field.
    MISSING_ID_PREFIX = 'id-'

    def __init__(self, id_field=None, encoding=None):
        self.parsed_list = []
        self.parsed_dict = {}
        self.id_field = id_field
        self.encoding = encoding

    def __getattribute__(self, name):
        if name == 'parsed_list':
            if (not object.__getattribute__(self, 'parsed_list')
                    and object.__getattribute__(self, 'parsed_dict')):
                return list(object.__getattribute__(self, 'parsed_dict').values())
            else:
                return object.__getattribute__(self, 'parsed_list')
        else:
            return object.__getattribute__(self, name)

    @staticmethod
    def parser_classes():
        """
        Returns all classes that (directly) inherit from Parser.
        """
        return Parser.__subclasses__()

    @staticmethod
    def get_parser_for_content(content):
        """
        Returns a class capable of parsing content, or False if none found.
        """
        if not isinstance(content, str):
            raise TypeError("content must be a string containing content to be parsed")
        for parser_class in Parser.parser_classes():
            if parser_class.is_parsable(content):
                return parser_class
        return None

    @staticmethod
    def get_parser_from_format_arg(format_arg):
        """
        Returns a parser class corresponding to formatting argument, or False
        if none found.
        """
        if not isinstance(format_arg, str):
            raise TypeError(
                'format_arg must be string containing format code for content to be parsed')
        for parser_class in Parser.parser_classes():
            if parser_class.format_arg() == format_arg:
                return parser_class
        return None

    @staticmethod
    def get_parser_for_file(file_path):
        """
        Returns a class capable of parsing file, or False if none found.
        """
        for parser_class in Parser.parser_classes():
            if parser_class.is_parsable_file(file_path):
                return parser_class
        return None

    @staticmethod
    def get_parser_for_directory(directory_path):
        """
        Returns a class capable of parsing files in directory, or False if
        none found.
        """
        files = os.listdir(directory_path)
        if not str(directory_path).endswith('/'):
            directory_path += '/'
        for file_name in files:
            if not file_name.startswith('.'):
                parser_class = Parser.get_parser_for_file(directory_path + file_name)
                if parser_class:
                    return parser_class
        return None

    @classmethod
    def is_parsable(cls, content):
        """
        Returns true if content is parsable by this class, false otherwise.
        """
        test_parser = cls()
        try:
            test_parser.parse_content_item(content)
            return bool(test_parser.parsed_list)
        except: # Any exception should just cause this function to return false.
            return False

    @classmethod
    def is_parsable_file(cls, file_path, encoding=None):
        """
        Returns true if file is parsable by this class, false otherwise.
        """
        # By default this isn't implemented, but descendents aren't *required* to
        # implement it, so just return False by default.
        return False

    @staticmethod
    @abstractmethod
    def format_arg():
        """
        Returns a string containing the format argument corresponding to this class
        """

    @abstractmethod
    def parse_content_item(self, content):
        """
        Parses a content item into a dictionary and adds to self.parsed_list or
        self.parsed_dict.
        """

    @abstractmethod
    def parse_file(self, file_path):
        """
        Parses a text file containing publication items.
        """

    def parse_files(self, file_paths):
        """
        Parses a list of files.
        """
        for file_path in file_paths:
            if os.path.isfile(file_path):
                try:
                    self.parse_file(file_path)
                except ValueError:
                    pass

    def parse_directory(self, directory_path):
        """
        Parses all files in a directory.
        """
        files = os.listdir(directory_path)
        if not str(directory_path).endswith('/'):
            directory_path += '/'
        file_paths = []
        for file in files:
            if not file.startswith('.'):
                file_paths.append(directory_path + file)
        self.parse_files(file_paths)

    def parse_directories(self, directories):
        """
        Parse all files in a list of directories.
        """
        for directory in directories:
            self.parse_directory(directory)

    def recursive_parse(self, directory_path):
        """
        Parse all files in directory_path, and all subdirectories.
        """
        files = [os.path.join(root, name)
                 for root, _, files in os.walk(directory_path)
                 for name in files]
        self.parse_files(files)

class WOKParser(Parser):
    """
    Parses Web of Knowledge / Web of Science files.
    """

    @staticmethod
    def format_arg():
        """
        Format argument code for class.
        """
        return 'WOK'

    #Map from Web of Knowledge field codes to more informative names.
    _field_tags = {
        'FN' : 'File Name',
        'VR' : 'Version Number',
        'PT' : 'Publication Type', #(J=Journal; B=Book; S=Series)
        'AU' : 'Authors',
        'AF' : 'Author Full Name',
        'CA' : 'Group Authors',
        'TI' : 'Document Title',
        'ED' : 'Editors',
        'SO' : 'Publication Name',
        'SE' : 'Book Series Title',
        'BS' : 'Book Series Subtitle',
        'LA' : 'Language',
        'DT' : 'Document Type',
        'CT' : 'Conference Title',
        'CY' : 'Conference Date',
        'HO' : 'Conference Host',
        'CL' : 'Conference Location',
        'SP' : 'Conference Sponsors',
        'DE' : 'Author Keywords',
        'ID' : 'Keywords',
        'AB' : 'Abstract',
        'C1' : 'Author Address',
        'RP' : 'Reprint Address',
        'EM' : 'E-mail Address',
        'FU' : 'Funding Agency and Grant Number',
        'FX' : 'Funding Text',
        'CR' : 'Cited References',
        'NR' : 'Cited Reference Count',
        'TC' : 'Times Cited',
        'PU' : 'Publisher',
        'PI' : 'Publisher City',
        'PA' : 'Publisher Address',
        'SC' : 'Subject Category',
        'SN' : 'ISSN',
        'BN' : 'ISBN',
        'J9' : '29-Character Source Abbreviation',
        'JI' : 'ISO Source Abbreviation',
        'PD' : 'Publication Date',
        'PY' : 'Year Published',
        'VL' : 'Volume',
        'IS' : 'Issue',
        'PN' : 'Part Number',
        'SU' : 'Supplement',
        'SI' : 'Special Issue',
        'BP' : 'Beginning Page',
        'EP' : 'Ending Page',
        'AR' : 'Article Number',
        'PG' : 'Page Count',
        'DI' : 'DOI',
        'GA' : 'Document Delivery Number',
        'UT' : 'Unique Article Identifier',
        'ER' : 'End of Record',
        'EF' : 'End of File'
    }

    # Determines how different fields will be parsed.
    # _list_tags - each line is an item in a list (eg. authors or citations)
    # _semicolon_list_tags - lines run together, but each item is separated by a ';' (eg. keywords)
    # _multiline_tags - lines run together
    # _paragraph_tags - lines indicate new paragraphs
    # _to_semicolon_list_tags - if in list_tags, also save list as
    #                          semicolon-deliniated list with original tag
    # By default, parser treats tags as multiline
    _list_tags = ['AU', 'AF', 'CA', 'ED', 'CR']
    _semicolon_list_tags = ['ID', 'RI', 'OI']
    _multiline_tags = ['TI']
    _paragraphs_tags = ['AB']
    _to_semicolon_list_tags = ['CR']

    # When parsing file, if file does not start with one of _valid_headers, the file will be skipped
    _valid_headers = ['FN Clarivate Analytics Web of Science']

    def parse_content_item(self, content):
        if not content:
            return None
        lines = content.splitlines()

        record_dict = {
            'content'   : content
        }

        current_key = None
        current_value = None
        semicolon_list = []
        next_missing_id = 0

        for line in lines:
            if not line:
                continue
            if re.match(r'^[A-Z,0-9][A-Z,0-9](?:\s.*$|$)', line):
                if current_key is not None:
                    if isinstance(current_value, str):
                        current_value = current_value.strip()
                    if current_key in self._semicolon_list_tags:
                        current_value = current_value.split(';')
                        current_value = list(map(lambda x: x.strip(), current_value))
                    try:
                        record_dict[WOKParser._field_tags[current_key]] = current_value
                    except KeyError: #field code not in list
                        record_dict[current_key] = current_value
                    if semicolon_list:
                        if record_dict.get(current_key) is not None:
                            current_key = current_key + '2'
                        sl_text = ''
                        for item in semicolon_list:
                            if sl_text != '':
                                sl_text += ';'
                            sl_text += item
                        record_dict[current_key] = sl_text
                        semicolon_list = []
                current_key = line[:2]
                if current_key in self._list_tags:
                    current_value = []
                else: current_value = ''
                line_text = line[3:]
            else:
                line_text = line

            line_text = line_text.strip()

            if current_key in self._list_tags:
                current_value.append(line_text)
                if current_key in self._to_semicolon_list_tags:
                    semicolon_list.append(line_text)
            elif current_key in self._paragraphs_tags:
                if current_value != '':
                    current_value += '\n'
                current_value += line_text
            else:
                if current_value != '':
                    current_value += ' '
                current_value += line_text

        if self.id_field is not None:
            try:
                self.parsed_dict[record_dict[self.id_field]] = record_dict
            except KeyError:
                self.parsed_dict[self.MISSING_ID_PREFIX + str(next_missing_id)] = record_dict
                next_missing_id += 1
        else:
            self.parsed_list.append(record_dict)

        return True

    @classmethod
    def is_parsable_file(cls, file_path, encoding=None):
        with open(file_path, 'rb') as f:
            file_data = f.read()
            if encoding is None:
                encoding = detect_encoding(file_data)
            try:
                file_text = file_data.decode(encoding)
            except UnicodeDecodeError:
                return False
            for header in cls._valid_headers:
                if file_text.startswith(header):
                    return True
        return False

    def parse_file(self, file_path):
        if not self.is_parsable_file(file_path):
            raise ValueError(
                'File %s is not parsable by %s' % (file_path, type(self).__name__))
        with open(file_path, 'rb') as f:
            file_data = f.read()
            if self.encoding is None:
                self.encoding = detect_encoding(file_data)
            file_text = file_data.decode(self.encoding)
            records = re.split(r'\n\s*ER\s*\n', file_text)

        records = list(map(lambda x: x + '\nER', records))

        for record in records:
            if record.startswith('EF'):
                continue
            self.parse_content_item(record)

        return True
