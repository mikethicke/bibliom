"""
This module implements publication objects for a bibliometric database.
"""
import re

from dbmanager import DBEntity

class Paper(DBEntity):
    """
    A single publication.
    """
    def __init__(self, db_table=None, db_manager=None, row_key=None, fields_dict=None):
        if db_table is None:
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
        journal_table = self.db_table.manager.get_table_object('journal')
        journal = Journal.fetch_entity(journal_table, {'idjournal': self.idjournal})
        return journal

    @property
    def cited_papers(self):
        """
        Returns list of papers cited by this paper.
        """
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
        dbm = self.db_table.manager
        citation_table = dbm.get_table_object('citation')
        citations = Citation.fetch_entities(citation_table, {'target_id':self.idpaper})
        citing_papers = [Paper.fetch_entity(self.db_table, {'idpaper':c.source_id})
                         for c in citations]
        return citing_papers
    
    def cite(self, target):
        """
        Create a citation from self to target paper.
        """
        new_citation = Citation(db_manager=self.db_table.manager)
        new_citation.cite(self, target)

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
        return new_author


    def papers(self):
        """
        Get list of papers written by author.
        """
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


def unit_test():
    """
    Unit tests for publication_objects module.
    """
    from dbmanager import DBManager
    import pprint
    pp = pprint.PrettyPrinter(indent=4)

    database_name = "test_db"
    database_user = "test_user"
    database_password = "jfYf2NoJr4DMHrF,3b"
    db = DBManager(database_name, database_user, database_password)
    paper_table = db.get_table_object('paper')
    paper_rows = paper_table.fetch_rows(limit=10)
    papers = Paper.entities_from_table_rows(paper_table, paper_rows)
    for paper in papers:
        print(paper)
    test_paper = papers[0]
    authors = test_paper.authors
    for author in authors:
        print(author)
    authors[0].last_name += "~add~"
    db.sync_to_db()
    journal = test_paper.journal
    print(journal)
    jpapers = journal.papers
    print(len(jpapers))
    for p in jpapers[:10]:
        print(p)
    print("Cited papers: %s" % len(test_paper.cited_papers))
    for cp in test_paper.cited_papers:
        print(cp)
    citing_papers = test_paper.citing_papers
    print("Citing papers: %s" % len(citing_papers))
    for ccp in citing_papers:
        print(ccp)



if __name__ == "__main__":
    unit_test()
