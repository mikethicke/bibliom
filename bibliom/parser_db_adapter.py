"""
Takes parsed record dicts and adds them to database tables.

This module is an adapter between the parsers module and the dbmanager module,
which are both designed to be general purpose. This module adapts the dicts
produced by parsers into database table rows. Any parsing that is particular to
this package take place here rather than in parsers.py.
"""
import re
import logging
import datetime

from bibliom import parsers
from bibliom.dbtable import DBTable
from bibliom.dbentity import DBEntity
from bibliom import publication_objects

REPORT_FREQUENCY = 500

def _parse_wok_date(year_published=None, publication_date=None):
    """
    Parse publication date, which WOK stores in separate fields.
    """
    if year_published is None:
        return None
    date_success = False
    if publication_date is not None:
        text_date = publication_date+ ' ' + year_published
        try:
            pub_date = datetime.datetime.strptime(text_date, '%b %Y').date()
            date_success = True
        except ValueError:
            pass
        if not date_success:
            try:
                pub_date = datetime.datetime.strptime(text_date, '%b %d %Y').date()
                date_success = True
            except ValueError:
                pass
    if not date_success:
        try:
            pub_date = datetime.datetime.strptime(year_published, '%Y').date()
        except Exception as e:
            pub_date = None
    return pub_date

def _wok_to_db(parser, manager, duplicates=None):
    """
    Adds records from Web of Science / Web of Knowledge parser
    to database.
    """
    logging.getLogger(__name__).info("Importing Web of Knowledge records into database.")
    paper_table = DBTable.get_table_object(manager, 'paper')
    author_table = DBTable.get_table_object(manager, 'author')
    journal_table = DBTable.get_table_object(manager, 'journal')
    paper_author_table = DBTable.get_table_object(manager, 'paper_author')
    keyword_table = DBTable.get_table_object(manager, 'paper_keyword')
    new_paper_list = []
    logging.getLogger(__name__).info("Importing %s records into database.", len(parser.parsed_list))
    for count, record in enumerate(parser.parsed_list):
        new_journal = publication_objects.Journal(journal_table)
        new_journal.title = record.get('Publication Name')
        new_journal.issn = record.get('ISSN')
        new_journal.save_to_db(duplicates)

        new_paper = publication_objects.Paper(paper_table)
        new_paper.doi = record.get('DOI')
        new_paper.title = record.get('Document Title')
        new_paper.abstract = record.get('Abstract')
        new_paper.first_page = record.get('Beginning Page')
        new_paper.last_page = record.get('Ending Page')
        new_paper.cited_records = ';'.join(
            (record.get('Cited References') or []))
        new_paper.wos_identifier = record.get('Unique Article Identifier')
        new_paper.total_citations = record.get('Times Cited')
        new_paper.citation_record = record.get('content')
        new_paper.publication_date = _parse_wok_date(
            record.get('Year Published'),
            record.get('Publication Date')
        )

        new_paper.idjournal = new_journal.idjournal

        #
        # Parse retracted papers from their titles
        #
        # Sample data:
        #
        #     RETRACTED: Two-dimensional nanosheets associated with one-dimensional single-
        #     crystalline nanorods self-assembled into three-dimensional flower-like Mn3O4
        #     hierarchical architectures (Retracted article. See vol. 19, pg. 25222, 2017)
        #
        #     RETRACTED: Electrophysiological Evidence for Failures of Item Individuation in Crowded
        #     Visual Displays (Retracted article)
        #
        # Retracted articles always have "RETRACTED: " added to beginning of title. If there is
        # a reference to the retraction, it is at the end and the year is always the last field
        # before the closed parentheses.
        #
        # We will remove the RETRACTED: prefix and the parenthecized reference at the end. If a year
        # is recorded, set new_paper.retracted_year to that year. Add "retracted" to keywords.
        #

        retracted_pattern = r'RETRACTED: (.*)\(Retracted article.*?(\d\d\d\d)?\)'
        m = re.search(retracted_pattern, new_paper.title, flags=re.IGNORECASE)
        if m is not None:
            new_paper.title = m.group(1).strip()
            if m.group(2) is not None:
                new_paper.retracted_year = m.group(2)
            new_paper.was_retracted = True

        new_paper.save_to_db(duplicates)

        for keyword in (record.get('Keywords') or []):
            new_keyword = DBEntity(keyword_table)
            new_keyword.keyword = keyword
            new_keyword.idpaper = new_paper.idpaper

        if new_paper.was_retracted:
            new_keyword = DBEntity(keyword_table)
            new_keyword.keyword = 'retracted'
            new_keyword.idpaper = new_paper.idpaper

        for author_name in (record.get('Authors') or []):
            new_author = publication_objects.Author.from_string(
                author_table, author_name)
            new_author.save_to_db(duplicates)
            new_paper_author = DBEntity(paper_author_table)
            new_paper_author.idauthor = new_author.idauthor
            new_paper_author.idpaper = new_paper.idpaper

        new_paper_list.append(new_paper)
        if (count + 1) % REPORT_FREQUENCY == 0:
            logging.getLogger(__name__).verbose_info(
                "Imported %s / %s records.",
                count + 1,
                len(parser.parsed_list))

        parsed_count = count + 1

    logging.getLogger(__name__).info("Imported %s papers.", parsed_count)
    logging.getLogger(__name__).info("Importing %s keywords.", len(keyword_table.rows))
    keyword_table.sync_to_db()
    logging.getLogger(__name__).info("Importing %s paper authors.", len(paper_author_table.rows))
    paper_author_table.sync_to_db()

    logging.getLogger(__name__).info("Parsing citations.")
    for count, new_paper in enumerate(new_paper_list):
        cited_records = new_paper.cited_records.split(';')
        for record in cited_records:
            target_paper = publication_objects.Paper(paper_table)
            # For DOI matching, see
            # https://www.crossref.org/blog/dois-and-matching-regular-expressions/
            ref_doi_match = re.search(
                pattern=r'((?:10.\d{4,9}/[-._;()/:A-Z0-9]+)|(?:.1002/[^\s]+))',
                string=record,
                flags=re.IGNORECASE)
            if ref_doi_match is not None:
                target_paper.doi = ref_doi_match.group(1)
                target_paper.save_to_db()
                new_paper.cite(target_paper)
        if (count + 1) % REPORT_FREQUENCY == 0:
            logging.getLogger(__name__).verbose_info(
                "Parsed citations for %s / %s papers.",
                count + 1,
                len(new_paper_list))
    logging.getLogger(__name__).info(
        "Importing %s citations into db.",
        len(DBTable.get_table_object(manager, 'citation').rows))
    DBTable.get_table_object(manager, 'citation').sync_to_db()

def _wch_to_db(parser, manager, duplicates=None, parse_authors=False):
    """
    Adds records from Web of Knowledge Citation Histories to database.
    """
    logging.getLogger(__name__).info(
        "Importing Web of Knowledge Citation History records into database.")
    if duplicates is None:
        duplicates = DBTable.Duplicates.INSERT
    paper_table = DBTable.get_table_object(manager, 'paper')
    author_table = DBTable.get_table_object(manager, 'author')
    journal_table = DBTable.get_table_object(manager, 'journal')
    paper_author_table = DBTable.get_table_object(manager, 'paper_author')
    keyword_table = DBTable.get_table_object(manager, 'paper_keyword')

    logging.getLogger(__name__).info(
        "Importing %s records into database.", 
        len(parser.parsed_list)
    )
    for count, record in enumerate(parser.parsed_list):
        new_journal = publication_objects.Journal(journal_table)
        new_journal.title = record.get('Source Title')
        new_journal.save_to_db(duplicates)

        new_paper = publication_objects.Paper(paper_table)
        new_paper.doi = record.get('DOI')
        new_paper.title = record.get('Title')
        new_paper.first_page = record.get('Beginning Page')
        new_paper.last_page = record.get('Ending Page')
        new_paper.total_citations = record.get('Total Citations')
        new_paper.publication_date = _parse_wok_date(
            record.get('Publication Year'),
            record.get('Publication Date'))
        new_paper.yearly_citations = record.get('Citation History')
        new_paper.idjournal = new_journal.idjournal

        # Copy & Paste from _wok_to_db
        retracted_pattern = r'RETRACTED: (.*)\(Retracted article.*?(\d\d\d\d)?\)'
        m = re.search(retracted_pattern, new_paper.title, flags=re.IGNORECASE)
        if m is not None:
            new_paper.title = m.group(1).strip()
            if m.group(2) is not None:
                new_paper.retracted_year = m.group(2)
            new_paper.was_retracted = True

        new_paper.save_to_db(duplicates)

        if new_paper.was_retracted:
            new_keyword = DBEntity(keyword_table)
            new_keyword.keyword = 'retracted'
            new_keyword.idpaper = new_paper.idpaper

        if parse_authors and record.get('Authors'):
            for author_name in record.get('Authors'):
                new_author = publication_objects.Author.from_string(
                    author_table, author_name
                )
                new_author.save_to_db(duplicates)
                new_paper_author = DBEntity(paper_author_table)
                new_paper_author.idauthor = new_author.idauthor
                new_paper_author.idpaper = new_paper.idpaper

        if count % REPORT_FREQUENCY == 0:
            logging.getLogger(__name__).verbose_info(
                "Imported %s / %s records.",
                count,
                len(parser.parsed_list))

        parsed_count = count

    logging.getLogger(__name__).info("Imported %s papers.", parsed_count)
    logging.getLogger(__name__).info("Importing %s keywords.", len(keyword_table.rows))
    keyword_table.sync_to_db()
    if parse_authors:
        keyword_table.sync_to_db()
        logging.getLogger(__name__).info(
            "Importing %s paper authors.",
            len(paper_author_table.rows)
        )
        paper_author_table.sync_to_db()

def parsed_records_to_db(parser, manager, duplicates=None):
    """
    Adds records from parser to dbtables in manager.
    """
    if isinstance(parser, parsers.WOKParser):
        _wok_to_db(parser, manager, duplicates)
    elif isinstance(parser, parsers.WCHParser):
        _wch_to_db(parser, manager, duplicates)
