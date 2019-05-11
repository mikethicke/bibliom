"""
Unit tests for dbtable.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition

import logging

import MySQLdb
import pytest

from bibliom.dbtable import DBTable
from bibliom import exceptions

@pytest.mark.usefixtures('class_manager')
class TestDBTable:
    """
    Tests for DBTable class.
    """
    def test_init(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_init')
        self.manager.reset_database()
        table = DBTable('paper', self.manager)
        assert table.table_name == 'paper'
        assert DBTable.get_table_object( 'paper', self.manager) == table
        with pytest.raises(exceptions.BiblioException):
            duplicate_table = DBTable('paper', self.manager)

    def test_str(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_str')
        table = DBTable.get_table_object( 'paper', self.manager)
        assert str(table) == 'test_db|paper'
    
    def test_repr(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_repr')
        table = DBTable.get_table_object( 'paper', self.manager)
        tr = repr(table)
        assert isinstance(tr, str)

    def test_dict_to_key(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_dict_to_key')
        kd = DBTable.KEY_STR_DELIMITER
        key_dict = {
            'pkey1':    1
        }
        key_str = 'pkey1' + kd + '1'
        assert DBTable.dict_to_key(key_dict) == key_str

        key_dict = {
            'pkey1':    1,
            'pkey2':    2
        }
        key_str = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + '2'
        assert DBTable.dict_to_key(key_dict) == key_str

        with pytest.raises(TypeError):
            key_dict = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + '2'
            key = DBTable.dict_to_key(key_dict)

    def test_key_to_dict(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_key_to_dict')
        kd = DBTable.KEY_STR_DELIMITER
        key_str = 'pkey1' + kd + '1'
        key_dict = {
            'pkey1':    1
        }
        assert DBTable.key_to_dict(key_str) == key_dict

        key_dict = {
            'pkey1':    1,
            'pkey2':    2
        }
        key_str = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + '2'
        assert DBTable.key_to_dict(key_str) == key_dict

        key_dict = {
            'pkey1':    1,
            'pkey2':    'non-number'
        }
        key_str = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + 'non-number'
        assert DBTable.key_to_dict(key_str) == key_dict

        with pytest.raises(TypeError):
            key_str = {
                'pkey1':    1,
                'pkey2':    2
            }
            key_dict = DBTable.key_to_dict(key_str)

        with pytest.raises(ValueError):
            key_str = 'pkey1.1'
            key_dict = DBTable.key_to_dict(key_str)

    def test_table_structure(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_table_structure')
        table = DBTable.get_table_object( 'paper', self.manager)
        assert isinstance(table.table_structure, dict)
        assert table.table_structure

    def test_get_table_object(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_get_table_object')
        table_name = self.manager.list_tables()[0]
        table_object = DBTable.get_table_object(table_name, self.manager)
        assert isinstance(table_object, DBTable)

    def test_get_table_objects(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_get_table_objects')
        assert isinstance(DBTable.get_table_objects(self.manager), dict)
        assert(
            len(self.manager.list_tables()) ==
            len(DBTable.get_table_objects(self.manager).items()))

    def test_insert_row(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_insert_row')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        citation_table = DBTable.get_table_object('citation', self.manager)
        paper_rows = [
            {
                'title':    'A Paper',
                'url':      "http://mikethicke.com",
                'idpaper':  1
            },
            {
                'title':    'Another Paper',
                'idpaper':   2
            },
            {
                'title':    'A Paper'
            },
            {
                'title':    'A Duplicate Paper',
                'abstract': "A duplicate paper's abstract",
                'idpaper':  1
            },
            {
                'tile':     'A Bad Title'
            },
            {
                'title':    'Another Duplicate Paper',
                'abstract': "A duplicate paper's abstract",
                'doi':      "10.1038/nature16193",
                'idpaper':  1
            }
        ]
        citation_rows = [
            {
                'target_id':   2,
                'source_id':   1
            },
            {
                'target_id':   1,
                'source_id':   1
            },
            {
                'target_id':   4,
                'source_id':   1
            },
            {
                'target_id':   1,
                'source_id':   -1
            },
            {
                'target_id':   2,
                'source_id':   1
            }
        ]
        paper_table.insert_row(paper_rows[0])
        paper_table.insert_row(paper_rows[1])
        paper_table.insert_row(paper_rows[2])
        row = paper_table.get_row_by_key('idpaper%%1')
        assert row['title'] == paper_rows[0]['title']

        # Duplicate primary key
        paper_table.insert_row(paper_rows[3], paper_table.Duplicates.SKIP)
        row = paper_table.get_row_by_key('idpaper%%1')
        assert row['title'] == 'A Paper'
        assert row['abstract'] is None
        assert row['url'] == "http://mikethicke.com"

        paper_table.insert_row(paper_rows[3], paper_table.Duplicates.OVERWRITE)
        row = paper_table.get_row_by_key('idpaper%%1')
        assert row['title'] == 'A Duplicate Paper'
        assert row['abstract'] == "A duplicate paper's abstract"
        assert row['url'] == "http://mikethicke.com"

        paper_table.insert_row(paper_rows[3], paper_table.Duplicates.REPLACE)
        row = paper_table.get_row_by_key('idpaper%%1')
        assert row['title'] == 'A Duplicate Paper'
        assert row['abstract'] == "A duplicate paper's abstract"
        assert row['url'] is None

        paper_table.insert_row(paper_rows[0], paper_table.Duplicates.REPLACE)
        paper_table.insert_row(paper_rows[5], paper_table.Duplicates.INSERT)
        row = paper_table.get_row_by_key('idpaper%%1')
        assert row['title'] == 'A Paper'
        assert row['abstract'] == "A duplicate paper's abstract"
        assert row['url'] == "http://mikethicke.com"
        assert row['doi'] == '10.1038/nature16193'

        with pytest.raises(MySQLdb.OperationalError):   # misspelled column name
            paper_table.insert_row(paper_rows[4])

        citation_table.insert_row(citation_rows[0])
        citation_table.insert_row(citation_rows[1])

        with pytest.raises(MySQLdb.IntegrityError):     # citation to non-existent paper
            citation_table.insert_row(citation_rows[2])

        with pytest.raises(MySQLdb.IntegrityError):     # citation to negative id
            citation_table.insert_row(citation_rows[3])

        citation_table.insert_row(citation_rows[4])     # duplicate citation (should have
                                                        # no effect)

    def test_create_new_row(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_create_new_row')
        self.manager.reset_database()
        new_rows = [
            {
                'title':    'A Paper',
                'url':      "http://mikethicke.com",
                'idpaper':  1
            },
            {
                'tile':     'A Bad Title'
            }
        ]
        paper_table = DBTable.get_table_object( 'paper', self.manager)

        new_key = paper_table.create_new_row(new_rows[0])
        new_row = paper_table.get_row_by_key(new_key)
        assert new_row['title'] == 'A Paper'
        assert new_row['doi'] is None

        with pytest.raises(ValueError):
            paper_table.create_new_row(new_rows[1])

    def test_insert_many_new_rows(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_insert_many_new_rows')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        success = author_table.insert_many_new_rows()
        assert success

        for i in range(0, 50):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.create_new_row({
            'last_name':    'Numberer',
            'given_names':  'Num',
            'idauthor':      1
        })
        with pytest.raises(MySQLdb.IntegrityError):
            author_table.insert_many_new_rows()

    def test_get_row_by_key(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_get_row_by_key')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        author_table.insert_row({
            'last_name':    'Numberer',
            'given_names':  'Num',
            'idauthor':      1
        })
        row_key = (
            'idauthor' +
            author_table.KEY_STR_DELIMITER +
            '1'
        )
        row = author_table.get_row_by_key(row_key)
        assert row['given_names'] == 'Num'
        del author_table.rows[row_key]
        row = author_table.get_row_by_key(row_key)
        assert row['given_names'] == 'Num'
        row_key = (
            'idauthor' +
            author_table.KEY_STR_DELIMITER +
            '2'
        )
        row = author_table.get_row_by_key(row_key)
        assert row is None
        row_key = (
            'idauthor' +
            '1'
        )
        with pytest.raises(ValueError):
            row = author_table.get_row_by_key(row_key)

    def test_get_row_by_primary_key(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_get_row_by_primary_key')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        author_table.insert_row({
            'last_name':    'Numberer',
            'given_names':  'Num',
            'idauthor':      1
        })
        row = author_table.get_row_by_primary_key(1)
        assert row['given_names'] == 'Num'
        row = author_table.get_row_by_primary_key(2)
        assert row is None

    def test_delete_rows(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_delete_rows')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.insert_many_new_rows()
        rows = author_table.fetch_rows({'last_name':'Numberer'})
        assert len(rows) == 100
        del_key_list = []
        for i in range(0, 50):
            del_key_list.append(
                'idauthor' +
                author_table.KEY_STR_DELIMITER +
                str(i+1)
            )
        author_table.delete_rows(del_key_list)
        rows = author_table.fetch_rows({'last_name':'Numberer'})
        assert len(rows) == 50

    def test_add_rows(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_add_rows')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        rows = [{'last_name':    'Numberer',
                 'given_names':  str(i+1)}
                for i in range(0, 100)]
        author_table.add_rows(rows)
        rows = {
            ('idauthor' +
             author_table.KEY_STR_DELIMITER +
             str(i)): {
                 'last_name':    'Numberer',
                 'given_names':  str(i+1)
                 }
            for i in range(100, 200)}
        author_table.add_rows(rows)
        bad_row = {
            'last_name':    'Numberer',
            'given_names':  'Num'
        }
        with pytest.raises(TypeError):
            author_table.add_rows(bad_row)

    def test_set_field(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_add_field')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        row_key = paper_table.create_new_row(paper_dict)
        paper_table.set_field(
            row_key,
            'title',
            'A New Title'
        )
        assert paper_table.get_row_by_key(row_key)['title'] == 'A New Title'

        with pytest.raises(ValueError):
            bad_key = (
                'paper' +
                '1'
            )
            paper_table.set_field(
                bad_key,
                'title',
                'A New Title'
            )

        with pytest.raises(ValueError):
            missing_key = (
                'idpaper' +
                paper_table.KEY_STR_DELIMITER +
                str(2)
            )
            paper_table.set_field(
                missing_key,
                'title',
                'A New Title'
            )

    def test_sync_to_db(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_sync_to_db')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.sync_to_db()
        for row_key in author_table.rows.keys():
            assert author_table.row_status[row_key] == author_table.RowStatus.SYNCED
            assert row_key.startswith('idauthor')

        for row_key in author_table.rows.keys():
            author_table.set_field(
                row_key,
                'last_name',
                'Newname'
            )
        for row_key in author_table.rows.keys():
            assert author_table.row_status[row_key] == author_table.RowStatus.UNSYNCED
        author_table.sync_to_db()
        for row_key, row_value in author_table.rows.items():
            assert author_table.row_status[row_key] == author_table.RowStatus.SYNCED
            assert row_value['last_name'] == 'Newname'

    def test_sync_tables_to_db(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_sync_tables_to_db')
        self.manager.reset_database()

        author_table = DBTable.get_table_object( 'author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })

        paper_table = DBTable.get_table_object( 'paper', self.manager)
        for i in range(0, 100):
            paper_table.create_new_row({
                'title':    'Paper %d' % (i+1),
            })

        DBTable.sync_tables_to_db(self.manager)
        for row_key, row_value in author_table.rows.items():
            assert author_table.row_status[row_key] == author_table.RowStatus.SYNCED
        for row_key, row_value in paper_table.rows.items():
            assert paper_table.row_status[row_key] == paper_table.RowStatus.SYNCED

@pytest.mark.usefixtures('import_small_database')
@pytest.mark.usefixtures('class_manager')  
class TestDBTableExistingDB:
    """Tests for DBTable that use test database"""
    def test_head(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBTable.test_head')
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_table.head()


