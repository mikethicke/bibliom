"""
This module implements publication objects for a bibliometric database.
"""
from dbmanager import DBEntity

class Paper(DBEntity):
    """
    A single publication.
    """
    def __init__(self, db_table=None, db_manager=None, row_key=None, fields_dict=None):
        if db_table is None:
            if db_manager is None:
                raise AttributeError("Paper.__init__: Must be initialized with "
                                     + "a DBManager or DBTable object.")
            db_table = db_manager.get_table_object('paper')
        super().__init__(db_table=db_table, row_key=row_key, fields_dict=fields_dict)

    def __str__(self):
        return "%s (%s)" % (str(self.title), str(self.doi))

    def get_authors(self):
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


class Author(DBEntity):
    """
    A single Author.
    """
    def __init__(self, db_table=None, db_manager=None, row_key=None, fields_dict=None):
        if db_table is None:
            if db_manager is None:
                raise AttributeError("Paper.__init__: Must be initialized with "
                                     + "a DBManager or DBTable object.")
            db_table = db_manager.get_table_object('author')
        super().__init__(db_table=db_table, row_key=row_key, fields_dict=fields_dict)

    def __str__(self):
        return "%s, %s" % (str(self.last_name), str(self.given_names))

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
    db.connect()
    paper_table = db.get_table_object('paper')
    paper_rows = paper_table.fetch_rows(limit=10)
    papers = Paper.entities_from_table_rows(paper_table, paper_rows)
    for paper in papers:
        print(paper)
    authors = papers[0].get_authors()
    for author in authors:
        print(author)



if __name__ == "__main__":
    unit_test()
