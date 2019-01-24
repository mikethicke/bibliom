"""
Parsers transform inputs from various sources to (lists of) dictionaries.
The parsers in this module don't interact with the database or publication
objects directly.
"""

from abc import ABC, abstractmethod
import os
import re

class Parser(ABC):
    """
    A parser parses publication items (articles, books, etc.) into dictionaries.
    Each parsed item is added to the parsed_items list. If an ID field is designated,
    parsed items are instead added to a dictionary indexed by that ID.
    """
    def __init__(self, id_field=None, encoding='utf8'):
        self.parsed_list = []
        self.parsed_dict = {}
        self.id_field = id_field
        self.encoding = encoding

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

    @abstractmethod
    def parse_url(self, url):
        """
        Parses content items found at a url, possibly by traversing through
        links.
        """

    def parse_files(self, file_paths):
        """
        Parses a list of files.
        """
        for file_path in file_paths:
            self.parse_file(file_path)

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
        lines = content.splitlines()

        record_dict = {
            'content'   : content
        }

        current_key = None
        current_value = None
        semicolon_list = []
        for line in lines:
            if not line:
                continue
            if re.match(r'^[A-Z,0-9][A-Z,0-9](?:\s.*$|$)', line):
                if current_key is not None:
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
                print("WOKParser|parse_content_item: Field "
                      + "%s not found in parsed record" % self.id_field)
                raise
        else:
            self.parsed_list += record_dict

        return True

    def parse_file(self, file_path):
        with open(file_path, 'r', encoding=self.encoding) as f:
            file_text = str(f.read())
            valid_wos = False
            for header in self._valid_headers:
                if file_text.startswith(header):
                    valid_wos = True
                    break
            if valid_wos:
                records = re.split(r'\n\s*ER\s*\n', file_text)
            else:
                return False

        records = list(map(lambda x: x + '\nER', records))

        for record in records:
            if record.startswith('EF'):
                continue
            self.parse_content_item(record)

        return True

    def parse_url(self, url):
        """
        WOK records are download-only.
        """
        raise NotImplementedError("WOK records cannot be parsed from urls.")
