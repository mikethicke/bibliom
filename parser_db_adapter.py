"""
Takes parsed record dicts and adds them to database tables.

This module is an adapter between the parsers module and the dbmanager module,
which are both designed to be general purpose. This module adapts the dicts
produced by parsers into database table rows.
"""
import re

import parsers
import dbmanager
import publication_objects


def _wok_to_db(parser, manager, duplicates=None):
    """
    Adds records from Web of Science / Web of Knowledge parser
    to database.
    """
    paper_table = manager.get_table_object('paper')
    author_table = manager.get_table_object('author')
    journal_table = manager.get_table_object('journal')
    paper_author_table = manager.get_table_object('paper_author')
    keyword_table = manager.get_table_object('paper_keyword')
    new_paper_list = []
    for record in parser.parsed_list:
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

        retracted_pattern = r'RETRACTED: (.*) \(Retracted article.*?(\d\d\d\d)?\)'
        m = re.search(retracted_pattern, new_paper.title)
        if m is not None:
            new_paper.title = m.group(1)
            if m.group(2) is not None:
                new_paper.retracted_year = m.group(2)
            new_paper.was_retracted = True
        
        new_paper.save_to_db(duplicates)

        for keyword in (record.get('Keywords') or []):
            new_keyword = dbmanager.DBEntity(keyword_table)
            new_keyword.keyword = keyword
            new_keyword.idpaper = new_paper.idpaper
        
        if new_paper.was_retracted:
            new_keyword = dbmanager.DBEntity(keyword_table)
            new_keyword.keyword = 'retracted'
            new_keyword.idpaper = new_paper.idpaper

        for author_name in (record.get('Authors') or []):
            new_author = publication_objects.Author.from_string(
                author_table, author_name)
            new_author.save_to_db(duplicates)
            new_paper_author = dbmanager.DBEntity(paper_author_table)
            new_paper_author.idauthor = new_author.idauthor
            new_paper_author.idpaper = new_paper.idpaper

        new_paper_list.append(new_paper)

    keyword_table.sync_to_db()
    paper_author_table.sync_to_db()

    for new_paper in new_paper_list:
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
    manager.get_table_object('citation').sync_to_db()


def parsed_records_to_db(parser, manager):
    """
    Adds records from parser to dbtables in manager.
    """
    if isinstance(parser, parsers.WOKParser):
        _wok_to_db(parser, manager)
