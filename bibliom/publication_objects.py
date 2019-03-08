"""
This module implements publication objects for a bibliometric database.
"""
import re

from bibliom.dbmanager import DBEntity
from bibliom import exceptions

class Paper(DBEntity):
    """
    A single publication.
    """
    def __init__(self, db_table=None, db_manager=None, row_key=None, fields_dict=None):
        if db_table is None:
            if db_manager is None:
                raise ValueError(
                    "When initializing, at least one of db_table and db_manager must not be None."
                )
            db_table = db_manager.get_table_object('paper')
        super().__init__(db_table=db_table, row_key=row_key, fields_dict=fields_dict)

        self.was_retracted = False

    def __str__(self):
        return "%s (%s)" % (str(self.title), str(self.doi))

    @property
    def authors(self):
        """
        Get list of authors for a paper.
        """
        if self.idpaper is None:
            return []
        dbm = self.db_table.manager
        pa_table = dbm.get_table_object('paper_author')
        paper_authors = pa_table.fetch_rows({'idpaper':self.idpaper})
        author_ids = [pa['idauthor'] for pa in paper_authors.values()]
        author_table = dbm.get_table_object('author')
        authors = Author.fetch_entities(author_table, {'idauthor':author_ids})
        return authors

    @property
    def journal(self):
        """
        Get journal containing paper.
        """
        if self.idjournal is None:
            return None
        journal_table = self.db_table.manager.get_table_object('journal')
        journal = Journal.fetch_entity(journal_table, {'idjournal': self.idjournal})
        return journal

    @property
    def cited_papers(self):
        """
        Returns list of papers cited by this paper.
        """
        if self.idpaper is None:
            return []
        dbm = self.db_table.manager
        citation_table = dbm.get_table_object('citation')
        citations = Citation.fetch_entities(citation_table, {'source_id':self.idpaper})
        cited_papers = [Paper.fetch_entity(self.db_table, {'idpaper':c.target_id})
                        for c in citations]
        return cited_papers

    @property
    def citing_papers(self):
        """
        Returns list of papers citing this paper.
        """
        if self.idpaper is None:
            return []
        dbm = self.db_table.manager
        citation_table = dbm.get_table_object('citation')
        citations = Citation.fetch_entities(citation_table, {'target_id':self.idpaper})
        citing_papers = [Paper.fetch_entity(self.db_table, {'idpaper':c.source_id})
                         for c in citations]
        return citing_papers

    def cite(self, target):
        """
        Create a citation from self to target paper and return it.
        """
        if not isinstance(target, DBEntity):
            raise TypeError("Target of citation must be a DBEntity object")
        if self.idpaper is None or target.idpaper is None:
            raise exceptions.DBUnsyncedError(
                "Source and target entities must be synced to DB before citation."
            )
        new_citation = Citation(db_manager=self.db_table.manager)
        new_citation.cite(self, target)
        return new_citation

class Author(DBEntity):
    """
    A single Author.
    """
    def __init__(self, db_table=None, db_manager=None, row_key=None, fields_dict=None):
        if db_table is None:
            db_table = db_manager.get_table_object('author')
        super().__init__(db_table=db_table, row_key=row_key, fields_dict=fields_dict)

    def __str__(self):
        return "%s, %s" % (str(self.last_name), str(self.given_names))

    @classmethod
    def from_string(cls, db_table, author_str):
        """
        Creates an author from a string.
        """
        new_author = cls(db_table)
        m = re.match(r'(.*), (.*)', author_str)
        if m is not None:
            new_author.last_name = m.group(1)
            new_author.given_names = m.group(2)
        else:
            new_author.last_name = author_str
            new_author.corporate = True
        return new_author

    @property
    def papers(self):
        """
        Get list of papers written by author.
        """
        if self.idauthor is None:
            return []
        dbm = self.db_table.manager
        pa_table = dbm.get_table_object('paper_author')
        paper_authors = pa_table.fetch_rows({'idauthor':self.idauthor})
        paper_ids = [pa['idpaper'] for pa in paper_authors.values()]
        paper_table = dbm.get_table_object('paper')
        papers = Paper.fetch_entities(paper_table, {'idpaper':paper_ids})
        return papers

class Journal(DBEntity):
    """
    A single journal.
    """
    def __init__(self, db_table=None, db_manager=None, row_key=None, fields_dict=None):
        if db_table is None:
            db_table = db_manager.get_table_object('journal')
        super().__init__(db_table=db_table, row_key=row_key, fields_dict=fields_dict)

    def __str__(self):
        return str(self.title)

    @property
    def papers(self):
        """
        Get list of papers contained in journal.
        """
        paper_table = self.db_table.manager.get_table_object('paper')
        papers = Paper.fetch_entities(paper_table, {'idjournal':self.idjournal})
        return papers

class Citation(DBEntity):
    """
    A single citation from source paper to target paper.
    """
    def __init__(self, db_table=None, db_manager=None, row_key=None, fields_dict=None):
        if db_table is None:
            db_table = db_manager.get_table_object('citation')
        super().__init__(db_table=db_table, row_key=row_key, fields_dict=fields_dict)

    def cite(self, source_paper, target_paper):
        """
        Set citation source and target.
        """
        self.source_id = source_paper.idpaper
        self.target_id = target_paper.idpaper

    @property
    def source_paper(self):
        """
        Returns Paper object corresponding to source paper.
        """
        paper_table = self.db_table.manager.get_table_object('paper')
        s_paper = Paper.fetch_entity(paper_table, {'idpaper':self.source_id})
        return s_paper

    @source_paper.setter
    def source_paper(self, source):
        """
        Sets source_id to id of source Paper.
        """
        self.source_id = source.idpaper

    @property
    def target_paper(self):
        """
        Returns Paper object corresponding to target paper.
        """
        paper_table = self.db_table.manager.get_table_object('paper')
        t_paper = Paper.fetch_entity(paper_table, {'idpaper':self.target_id})
        return t_paper

    @target_paper.setter
    def target_paper(self, target):
        """
        Sets target_id to id of target Paper.
        """
        self.target_id = target.idpaper
