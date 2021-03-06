"""
Unit tests for publication_objects.py
"""

# pylint: disable=no-member, missing-docstring, len-as-condition
import logging

import pytest

from bibliom.publication_objects import Paper, Author, Journal, Citation
from bibliom.dbtable import DBTable
from bibliom import exceptions

@pytest.mark.usefixtures('class_manager')
class TestPaper():
    """
    Unit tests for Paper class.
    """
    def test_init(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_init')
        new_paper = Paper(manager=self.manager)
        assert isinstance(new_paper, Paper)
        assert not new_paper.was_retracted

        paper_table = DBTable.get_table_object('paper', self.manager)
        new_paper = Paper(table=paper_table)
        assert isinstance(new_paper, Paper)
        assert not new_paper.was_retracted

        new_paper = Paper(
            table=paper_table,
            fields_dict={
                'title':    "A New Paper",
                'doi':      "10.1231/12312"
            })
        assert new_paper.title == "A New Paper"
        assert new_paper.doi == "10.1231/12312"

        paper = Paper(
            table=paper_table,
            row_key='idpaper' + DBTable.KEY_STR_DELIMITER + '1'
        )
        assert paper.title
    
    def test_fetch(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_fetch')
        paper = Paper.fetch(
            manager=self.manager,
            where_dict={'doi': '10.1016/j.ijhydene.2016.06.178'}
        )
        assert paper.title == (
            'Plasma equilibrium reconstruction for the nuclear fusion of ' +
            'magnetically confined hydrogen isotopes'
        )
        paper = Paper.fetch(where_dict={'doi': '10.1016/j.ijhydene.2016.06.178'})
        assert paper.title == (
            'Plasma equilibrium reconstruction for the nuclear fusion of ' +
            'magnetically confined hydrogen isotopes'
        )
        paper = Paper.fetch(doi='10.1016/j.ijhydene.2016.06.178')
        assert paper.title == (
            'Plasma equilibrium reconstruction for the nuclear fusion of ' +
            'magnetically confined hydrogen isotopes'
        )

    def test_str(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_str')
        paper_table = DBTable.get_table_object('paper', self.manager)
        new_paper = Paper(
            table=paper_table,
            fields_dict={
                'title':    "A New Paper",
                'doi':      "10.1231/12312"
            })

        assert str(new_paper) == (
            'A New Paper (10.1231/12312)'
        )

    def test_authors(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_authors')
        paper = Paper.fetch(
            where_dict={'doi': '10.1089/ars.2017.7361'}
        )
        assert len(paper.authors) == 2

        new_paper = Paper(
            fields_dict={
                'title':    "A New Paper",
                'doi':      "10.1231/12312"
            }
        )
        assert len(new_paper.authors) == 0

    def test_journal(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_journal')
        paper = Paper.fetch(
            where_dict={'doi': '10.1089/ars.2017.7361'}
        )
        assert paper.journal.title == 'ANTIOXIDANTS & REDOX SIGNALING'

    def test_cited_papers(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_cited_papers')
        paper = Paper.fetch(
            where_dict={'doi': '10.1089/ars.2017.7361'}
        )
        assert len(paper.cited_papers) == 177

        new_paper = Paper(
            fields_dict={
                'title':    "A New Paper",
                'doi':      "10.1231/12312"
            }
        )
        assert len(new_paper.cited_papers) == 0

    def test_citing_papers(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_citing_papers')
        paper = Paper.fetch(
            where_dict={'doi': '10.1016/j.ijhydene.2016.06.178'}
        )
        assert len(paper.citing_papers) == 5

        new_paper = Paper(
            fields_dict={
                'title':    "A New Paper",
                'doi':      "10.1231/12312"
            }
        )
        assert len(new_paper.citing_papers) == 0

    def test_cite(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestPaper.test_cite')
        source_paper = Paper.fetch(
            where_dict={'doi': '10.1016/j.ijhydene.2016.06.178'}
        )
        target_paper = Paper.fetch(
            where_dict={'doi': '10.1016/j.ijhydene.2016.07.026'}
        )
        new_citation = source_paper.cite(target_paper)
        new_citation.save_to_db()

        found = False
        for paper in source_paper.cited_papers:
            if paper.doi == '10.1016/j.ijhydene.2016.07.026':
                found = True
                break
        assert found

        found = False
        for paper in target_paper.citing_papers:
            if paper.doi == '10.1016/j.ijhydene.2016.06.178':
                found = True
                break
        assert found

        new_paper = Paper()
        with pytest.raises(exceptions.DBUnsyncedError):
            new_paper.cite(target_paper)
        with pytest.raises(exceptions.DBUnsyncedError):
            source_paper.cite(new_paper)

        with pytest.raises(TypeError):
            source_paper.cite("hello")

@pytest.mark.usefixtures('class_manager')
class TestAuthor():
    """
    Unit tests for Author class.
    """
    def test_init(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestAuthor.test_init')
        author_table = DBTable.get_table_object('author', self.manager)

        new_author = Author(
            manager=self.manager
        )
        assert isinstance(new_author, Author)

        new_author = Author(
            table=author_table
        )
        assert isinstance(new_author, Author)

        new_author = Author(
            table=author_table,
            fields_dict={
                'last_name':    'Thicke',
                'given_names':  'Mike'
            }
        )
        assert isinstance(new_author, Author)
        assert new_author.last_name == 'Thicke'

        new_author = Author(
            table=author_table,
            row_key='idauthor' + DBTable.KEY_STR_DELIMITER + '1'
        )
        assert isinstance(new_author, Author)
        assert new_author.last_name

    def test_str(self):
        logging.getLogger('bibliom.pytest').debug('-->TestAuthor.test_str')
        new_author = Author(
            manager=self.manager,
            fields_dict={
                'last_name':    'Thicke',
                'given_names':  'Michael Lowell Ellis'
            }
        )
        assert str(new_author) == 'Thicke, Michael Lowell Ellis'

    def test_from_string(self):
        logging.getLogger('bibliom.pytest').debug('-->TestAuthor.test_from_string')
        author_table = DBTable.get_table_object('author', self.manager)
        new_author = Author.from_string(author_table, 'Thicke, Michael Lowell Ellis')
        assert new_author.last_name == 'Thicke'
        assert new_author.given_names == 'Michael Lowell Ellis'

        new_author = Author.from_string(author_table, 'IPCC')
        assert new_author.corporate
        assert new_author.last_name == 'IPCC'

    def test_papers(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestAuthor.test_papers')
        author_table = DBTable.get_table_object('author', self.manager)

        author = Author(
            table=author_table,
            row_key='idauthor' + author_table.KEY_STR_DELIMITER + '1'
        )
        assert len(author.papers) == 1

        new_author = Author(
            manager=self.manager,
            fields_dict={
                'last_name':    'Thicke',
                'given_names':  'Michael Lowell Ellis'
            }
        )
        assert len(new_author.papers) == 0

@pytest.mark.usefixtures('class_manager')
class TestJournal():
    """
    Unit tests for Journal class.
    """
    def test_init(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestJournal.test_init')
        journal_table = DBTable.get_table_object('journal', self.manager)

        journal = Journal(manager=self.manager)
        assert isinstance(journal, Journal)

        journal = Journal(table=journal_table)
        assert isinstance(journal, Journal)

        journal = Journal(
            table=journal_table,
            row_key='idjournal' + journal_table.KEY_STR_DELIMITER + '1')
        assert isinstance(journal, Journal)
        assert isinstance(journal.title, str)

        new_journal = Journal(
            table=journal_table,
            fields_dict={
                'title':    'A Journal'
            }
        )
        assert new_journal.title == 'A Journal'

    def test_papers(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestJournal.test_papers')
        journal_table = DBTable.get_table_object('journal', self.manager)

        journal = Journal.fetch(
            table=journal_table,
            where_dict={
                'issn':     '1876-6102'
            }
        )
        assert len(journal.papers) == 148

@pytest.mark.usefixtures('class_manager')
class TestCitation():
    """
    Unit tests for Citation class.
    """
    def test_init(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestCitation.test_init')
        citation_table = DBTable.get_table_object('citation', self.manager)

        citation = Citation(table=citation_table)
        assert isinstance(citation, Citation)

        citation = Citation(manager=self.manager)
        assert isinstance(citation, Citation)

        citation = Citation(
            table=citation_table,
            row_key=('source_id' +
                     citation_table.KEY_STR_DELIMITER +
                     'target_id' +
                     citation_table.KEY_STR_DELIMITER +
                     '68' +
                     citation_table.KEY_STR_DELIMITER +
                     '75')
        )
        assert isinstance(citation, Citation)

        new_citation = Citation(
            table=citation_table,
            fields_dict={
                'source_id':    100,
                'target_id':    200
            }
        )
        assert isinstance(new_citation, Citation)
        assert new_citation.source_id == 100
        assert new_citation.target_id == 200

    def test_cite(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestCitation.test_cite')
        citation_table = DBTable.get_table_object('citation', self.manager)

        source_paper = Paper.fetch(
            where_dict={'doi': '10.1016/j.ijhydene.2016.06.178'}
        )
        target_paper = Paper.fetch(
            where_dict={'doi': '10.1016/j.ijhydene.2016.07.026'}
        )

        new_citation = Citation(table=citation_table)
        new_citation.cite(source_paper, target_paper)
        assert new_citation.source_id == source_paper.idpaper
        assert new_citation.target_id == target_paper.idpaper

    def test_source_paper_target_paper(self, import_small_database):
        logging.getLogger('bibliom.pytest').debug('-->TestCitation.test_source_paper_target_paper')
        citation_table = DBTable.get_table_object('citation', self.manager)
        paper_table = DBTable.get_table_object( 'paper', self.manager)

        citation = Citation(
            table=citation_table,
            row_key=('source_id' +
                     citation_table.KEY_STR_DELIMITER +
                     'target_id' +
                     citation_table.KEY_STR_DELIMITER +
                     '68' +
                     citation_table.KEY_STR_DELIMITER +
                     '75')
        )
        source_paper = citation.source_paper
        assert source_paper.idpaper == 68
        assert source_paper.doi == '10.1140/epja/i2017-12405-4'
        target_paper = citation.target_paper
        assert target_paper.idpaper == 75
        assert target_paper.doi == '10.1088/1674-1137/41/11/113104'

        paper_1 = Paper(
            table=paper_table,
            row_key='idpaper' + DBTable.KEY_STR_DELIMITER + '1'
        )
        paper_2 = Paper(
            table=paper_table,
            row_key='idpaper' + DBTable.KEY_STR_DELIMITER + '2'
        )
        citation = Citation(table=citation_table)
        citation.source_paper = paper_1
        citation.target_paper = paper_2
        assert citation.source_id == 1
        assert citation.target_id == 2
        citation.save_to_db()
        assert paper_2 in paper_1.cited_papers
        assert paper_1 in paper_2.citing_papers
