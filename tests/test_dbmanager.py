"""
Unit tests for dbmanager.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition

import pytest
import MySQLdb

from bibliom import dbmanager
from bibliom import exceptions

DB_NAME = 'test_db'
DB_USER = 'test_user'
DB_PASSWORD = 'jfYf2NoJr4DMHrF,3b'

@pytest.fixture(scope="module")
def connected_manager():
    try:
        manager = dbmanager.DBManager(
            name=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        manager.reset_database()
    except exceptions.UnknownDatabaseError:
        manager = dbmanager.DBManager(
            name=None,
            user=DB_USER,
            password=DB_PASSWORD
        )
        manager.create_database(DB_NAME)
    yield manager

    del manager

@pytest.fixture(scope="class")
def class_manager(request, connected_manager):
    request.cls.manager = connected_manager


@pytest.mark.usefixtures('class_manager')
class TestDBManager():
    """
    Tests for DBManager class.
    """
    def test_init(self):
        with pytest.raises(exceptions.UnknownDatabaseError):
            bad_manager = dbmanager.DBManager(
                name='aaaaadfasfasdfdasfdsa',
                user=DB_USER,
                password=DB_PASSWORD
            )
        with pytest.raises(MySQLdb.Error):
            bad_manager = dbmanager.DBManager(
                name=DB_NAME,
                user='adfdsafadsg',
                password=DB_PASSWORD
            )
        with pytest.raises(MySQLdb.Error):
            bad_manager = dbmanager.DBManager(
                name=DB_NAME,
                user=DB_USER,
                password='sadfdsafsdf'
            )
            assert bad_manager.db is None
        manager = dbmanager.DBManager(
            name=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        assert isinstance(manager.db, MySQLdb.connections.Connection)

    def test_str(self):
        assert str(self.manager) == 'test_db'

    def test_build_where(self):
        # pylint: disable=protected-access
        manager = dbmanager.DBManager()
        assert manager._build_where() == ("1", [])
        assert(manager._build_where({
            'id': 1,
            'name': 'Mike'
        }) == ('`id`=%s AND `name`=%s', [1, 'Mike']))
        assert(manager._build_where({
            'id': 1,
            'name': 'Mike'
        }, True) == ('`id`=%s OR `name`=%s', [1, 'Mike']))
        assert(manager._build_where({
            'id': 1,
            'name': 'NULL'
        }) == ('`id`=%s AND `name` IS NULL', [1]))
        assert(manager._build_where({
            'id': 1,
            'name': 'NOT NULL'
        }) == ('`id`=%s AND `name` IS NOT NULL', [1]))
        assert(manager._build_where({
            'id': [1, 2, 3]
        }) == ('`id` IN (%s, %s, %s)', [1, 2, 3]))
        assert(manager._build_where({
            'id': "> 10"
        }) == ("`id` > '10'", []))
        assert(manager._build_where({
            'name': "%ich%"
        }) == ("`name` LIKE %s", ['%ich%']))

    def test_query_params(self):
        # pylint: disable=protected-access
        manager = dbmanager.DBManager()
        assert manager._query_params({}) is None
        with pytest.raises(TypeError):
            manager._query_params("aaa")
        assert(manager._query_params({
            'id': 1,
            'name': 'Mike'
        }) == {
            'key_str'           : '`id`, `name`',
            'value_alias'       : '%s, %s',
            'value_list'        : [1, 'Mike'],
            'update_str'        : '`id`=%s, `name`=%s',
            'where_or_clause'   : '`id`=%s OR `name`=%s '
        })

    def test_create_database(self):
        manager = dbmanager.DBManager()
        with pytest.raises(ValueError):
            manager.create_database()
        with pytest.raises(ValueError):
            manager.create_database('test_db_2')
        manager.user = DB_USER
        manager.password = DB_PASSWORD
        manager.create_database('test_db_2')
        assert manager.name == 'test_db_2'
        assert isinstance(manager.db, MySQLdb.connections.Connection)

    def test_drop_database(self):
        manager = dbmanager.DBManager(
            name='test_db_2',
            user=DB_USER,
            password=DB_PASSWORD
        )
        manager.drop_database()
        assert manager.db is None

    def test_reset_database(self):
        self.manager.reset_database()
        assert self.manager.name == 'test_db'
        assert isinstance(self.manager.db, MySQLdb.connections.Connection)

    def test_list_tables(self):
        assert isinstance(self.manager.list_tables(), list)

    def test_table_structure(self):
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.table_structure(table_name),
            dict)

    def test_primary_key_list(self):
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.primary_key_list(table_name),
            list
        )

    def test_foreign_key_list(self):
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.foreign_key_list(table_name),
            list
        )

    def test_table_fields(self):
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.table_fields(table_name),
            list
        )
        assert len(self.manager.table_fields(table_name)) > 0

    def test_existing_table_object_keys(self):
        assert isinstance(
            self.manager.existing_table_object_keys(),
            list
        )
        assert len(self.manager.existing_table_object_keys()) == 0

    def test_get_table_object(self):
        table_name = self.manager.list_tables()[0]
        table_object = self.manager.get_table_object(table_name)
        assert isinstance(table_object, dbmanager.DBTable)

    def test_get_table_objects(self):
        assert isinstance(self.manager.get_table_objects(), dict)
        assert(
            len(self.manager.list_tables()) ==
            len(self.manager.get_table_objects().items()))

    def test_insert_row(self):
        table_name = 'author'
        good_row = {
            'last_name':    'Thicke',
            'given_names':  'Michael Lowell Ellis',
            'h-index':      '1',
            'orcid':        '0000-0003-1401-2056',
            'corporate':    False
        }
        row_id_1 = self.manager.insert_row(table_name, good_row)
        assert isinstance(row_id_1, int)
        assert row_id_1 > -1

        good_row_2 = {
            'last_name':    'Thïcké',
            'given_names':  'Michāel',
            'h-index':      '1',
            'orcid':        '0000-0003-1401-2056-2',
            'corporate':    False
        }
        row_id_2 = self.manager.insert_row(table_name, good_row_2)
        assert isinstance(row_id_2, int)
        assert row_id_2 > -1

        good_row_3 = {
            'last_name':    'IPCC',
            'corporate':    True
        }
        row_id_3 = self.manager.insert_row(table_name, good_row_3)
        assert isinstance(row_id_3, int)
        assert row_id_3 > -1

        with pytest.raises(TypeError):
            bad_row = ['Thicke', 'Michael']
            row_id = self.manager.insert_row(table_name, bad_row)

        with pytest.raises(MySQLdb.Error):
            bad_row_2 = {
                'idauthor':     3,
                'last_name':    'Thïcké',
                'given_names':  'Michāel',
                'h-index':      '1',
                'orcid':        '0000-0003-1401-2056-3',
                'corporate':    False
            }
            row_id = self.manager.insert_row(table_name, bad_row_2)

        with pytest.raises(MySQLdb.IntegrityError):
            duplicate_pkey_row = {
                'idauthor':     3,
                'last_name':    'Hoffman-Thicke'
            }
            row_id = self.manager.insert_row(table_name, duplicate_pkey_row)

    def test_fetch_row(self):
        table_name = 'author'
        row = self.manager.fetch_row(table_name, {'last_name': 'Thicke'})
        assert row['given_names'] == 'Michael Lowell Ellis'
        row = self.manager.fetch_row(table_name, {'last_name': 'Hoffman'})
        assert row is None
        with pytest.raises(MySQLdb.OperationalError):
            row = self.manager.fetch_row(table_name, {'llast_name': 'Thicke'})
        row = self.manager.fetch_row(table_name, {'last_name': 'NOT NULL'})
        assert isinstance(row, dict)
        assert len(row.items()) > 0
        row = self.manager.fetch_row(table_name, {'given_names': '%Mich%'})
        assert len(row.items()) > 0

    def test_fetch_rows(self):
        table_name = 'author'
        rows = self.manager.fetch_rows(table_name, {'given_names': '%Mich%'})
        assert isinstance(rows, dict)
        assert len(rows.items()) == 2 # see test_insert_row
        for key, value in rows.items():
            assert isinstance(value, dict)
        with pytest.raises(MySQLdb.OperationalError):
            rows = self.manager.fetch_rows(table_name, {'llast_name': 'Thicke'})

    def test_insert_many_rows(self):
        self.manager.reset_database()
        table_name = 'author'
        rows = []
        for i in range(0, 100):
            rows.append({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        self.manager.insert_many_rows(table_name, rows)
        fetched_rows = self.manager.fetch_rows(table_name, {'last_name': 'Numberer'})
        assert len(fetched_rows) == 100
        with pytest.raises(TypeError):
            bad_row = [['Mike', 'Thicke']]
            bad_row.append(rows)
            self.manager.insert_many_rows(table_name, bad_row)

    def test_update_rows(self):
        table_name = 'author'
        success = self.manager.update_rows(
            table_name=table_name,
            row_dict={'given_names': 'Mike'},
            where_dict={'last_name': 'Numberer'})
        assert success
        success = self.manager.update_rows(
            table_name=table_name,
            row_dict={'given_names': 'Mira'},
            where_dict={'last_name': 'Hoffman'}
        )
        assert not success # No rows in db with last_name == Hoffman
        fetched_rows = self.manager.fetch_rows(table_name, {'last_name': 'Numberer'})
        assert len(fetched_rows) == 100
        assert list(fetched_rows.values())[0]['given_names'] == 'Mike'

    def test_delete_rows(self):
        table_name = 'author'
        success = self.manager.delete_rows(
            table_name=table_name,
            where_dict={'last_name': 'Numberer'})
        assert success
        fetched_rows = self.manager.fetch_rows(table_name, {'last_name': 'Numberer'})
        assert len(fetched_rows) == 0
        success = self.manager.delete_rows(
            table_name=table_name,
            where_dict={'last_name': 'Numberer'})
        assert not success

    def test_import_dict(self):
        self.manager.reset_database()
        tables_dict = {
            'author': [
                {
                    'last_name': 'Thicke',
                    'given_names': 'Mike'
                },
                {
                    'last_name': 'Thicke',
                    'given_names': 'Michael Lowell Ellis'
                }
            ],
            'paper': [
                {
                    'title': 'Towards an analysis of unanalyzability'
                }
            ]
        }
        self.manager.import_dict(tables_dict)
        authors = self.manager.fetch_rows('author', {'last_name': 'Thicke'})
        assert len(authors) == 2
        papers = self.manager.fetch_rows('paper')
        assert len(papers) == 1
        assert list(papers.values())[0]['title'] == 'Towards an analysis of unanalyzability'

    def test_clear_table(self):
        self.manager.clear_table('author')
        authors = self.manager.fetch_rows('author')
        assert len(authors) == 0

@pytest.mark.usefixtures('class_manager')
class TestDBTable:
    """
    Tests for DBTable class.
    """

    def test_init(self):
        self.manager.reset_database()
        table = dbmanager.DBTable(self.manager, 'paper')
        assert table.table_name == 'paper'
        assert self.manager.get_table_object('paper') == table
        with pytest.raises(exceptions.BiblioException):
            duplicate_table = dbmanager.DBTable(self.manager, 'paper')

    def test_str(self):
        table = self.manager.get_table_object('paper')
        assert str(table) == 'test_db|paper'

    def test_dict_to_key(self):
        kd = dbmanager.DBTable.KEY_STR_DELIMITER
        key_dict = {
            'pkey1':    1
        }
        key_str = 'pkey1' + kd + '1'
        assert dbmanager.DBTable.dict_to_key(key_dict) == key_str

        key_dict = {
            'pkey1':    1,
            'pkey2':    2
        }
        key_str = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + '2'
        assert dbmanager.DBTable.dict_to_key(key_dict) == key_str

        with pytest.raises(TypeError):
            key_dict = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + '2'
            key = dbmanager.DBTable.dict_to_key(key_dict)

    def test_key_to_dict(self):
        kd = dbmanager.DBTable.KEY_STR_DELIMITER
        key_str = 'pkey1' + kd + '1'
        key_dict = {
            'pkey1':    1
        }
        assert dbmanager.DBTable.key_to_dict(key_str) == key_dict

        key_dict = {
            'pkey1':    1,
            'pkey2':    2
        }
        key_str = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + '2'
        assert dbmanager.DBTable.key_to_dict(key_str) == key_dict

        key_dict = {
            'pkey1':    1,
            'pkey2':    'non-number'
        }
        key_str = 'pkey1' + kd + 'pkey2' + kd + '1' + kd + 'non-number'
        assert dbmanager.DBTable.key_to_dict(key_str) == key_dict

        with pytest.raises(TypeError):
            key_str = {
                'pkey1':    1,
                'pkey2':    2
            }
            key_dict = dbmanager.DBTable.key_to_dict(key_str)

        with pytest.raises(ValueError):
            key_str = 'pkey1.1'
            key_dict = dbmanager.DBTable.key_to_dict(key_str)

    def test_table_structure(self):
        table = self.manager.get_table_object('paper')
        assert isinstance(table.table_structure(), dict)
        assert table.table_structure()

    def test_insert_row(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        citation_table = self.manager.get_table_object('citation')
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

        paper_table.insert_row(paper_rows[3], paper_table.Duplicates.MERGE)
        row = paper_table.get_row_by_key('idpaper%%1')
        assert row['title'] == 'A Duplicate Paper'
        assert row['abstract'] == "A duplicate paper's abstract"
        assert row['url'] == "http://mikethicke.com"

        paper_table.insert_row(paper_rows[3], paper_table.Duplicates.OVERWRITE)
        row = paper_table.get_row_by_key('idpaper%%1')
        assert row['title'] == 'A Duplicate Paper'
        assert row['abstract'] == "A duplicate paper's abstract"
        assert row['url'] is None

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
        paper_table = self.manager.get_table_object('paper')

        new_key = paper_table.create_new_row(new_rows[0])
        new_row = paper_table.get_row_by_key(new_key)
        assert new_row['title'] == 'A Paper'
        assert new_row['doi'] is None

        with pytest.raises(ValueError):
            paper_table.create_new_row(new_rows[1])

    def test_insert_many_new_rows(self):
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
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
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
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
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
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
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
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

    def test_entity_from_row(self):
        self.manager.reset_database()
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        paper_table = self.manager.get_table_object('paper')
        row_key = paper_table.create_new_row(paper_dict)
        new_entity = paper_table.entity_from_row(row_key)
        assert new_entity.title == 'A Paper'

        with pytest.raises(ValueError):
            bad_entity = paper_table.entity_from_row(
                'idpaper' +
                paper_table.KEY_STR_DELIMITER +
                str(2)
            )

    def test_generate_entities(self):
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        entities = author_table.generate_entities()
        assert len(entities) == 100
        assert isinstance(
            list(entities.values())[0],
            dbmanager.DBEntity)

    def test_add_rows(self):
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
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
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
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
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
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

@pytest.mark.usefixtures('class_manager')
class TestDBEntity():
    """
    Tests for DBEntity class.
    """
    def test_init(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = dbmanager.DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.title == 'A Paper'
        assert entity.idpaper == 1
        entity2 = paper_table.entity_from_row(entity.row_key)
        assert entity2.title == 'A Paper'
        entity3 = dbmanager.DBEntity(paper_table, entity.row_key)
        assert entity3.title == 'A Paper'

        entity4 = dbmanager.DBEntity(paper_table)
        assert entity4.title is None

    def test_getattr(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = dbmanager.DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.title == 'A Paper'

        with pytest.raises(exceptions.BiblioException):
            a = entity.tile # misspelling of field

    def test_setattr(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = dbmanager.DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.title == 'A Paper'
        entity.title = 'Another Title'
        assert entity.title == 'Another Title'
        assert paper_table.rows[entity.row_key]['title'] == 'Another Title'

    def test_entities_from_table_rows(self):
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.insert_many_new_rows()
        entities = dbmanager.DBEntity.entities_from_table_rows(author_table, author_table.rows)
        assert len(entities) == 100
        assert entities[0].last_name == 'Numberer'

        with pytest.raises(TypeError):
            entities = dbmanager.DBEntity.entities_from_table_rows(author_table, [])

        with pytest.raises(TypeError):
            entities = dbmanager.DBEntity.entities_from_table_rows('hello', author_table.rows)

        with pytest.raises(ValueError):
            entities = dbmanager.DBEntity.entities_from_table_rows(author_table, {'hello': 'world'})

        entities = dbmanager.DBEntity.entities_from_table_rows(author_table, {})
        assert entities == []

    def test_fetch_entities(self):
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.insert_many_new_rows()
        entities = dbmanager.DBEntity.fetch_entities(author_table, {'last_name': 'Numberer'})
        assert len(entities) == 100
        assert entities[0].last_name == 'Numberer'

        entities = dbmanager.DBEntity.fetch_entities(author_table, {'last_name': 'Nothing'})
        assert entities == []

        with pytest.raises(TypeError):
            entities = dbmanager.DBEntity.fetch_entities('author', {'last_name': 'Nothing'})

        with pytest.raises(TypeError):
            entities = dbmanager.DBEntity.fetch_entities(author_table, [])

    def test_fetch_entity(self):
        self.manager.reset_database()
        author_table = self.manager.get_table_object('author')
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.insert_many_new_rows()
        entity = dbmanager.DBEntity.fetch_entity(author_table, {'last_name': 'Numberer'})
        assert isinstance(entity, dbmanager.DBEntity)
        assert entity.last_name == 'Numberer'

    def test_fields_dict(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = dbmanager.DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.fields_dict['title'] == 'A Paper'
        assert entity.fields_dict['url'] == "http://mikethicke.com"
        assert entity.fields_dict['content'] is None

    def test_get_field(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = dbmanager.DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.get_field('title') == 'A Paper'

    def test_set_field(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = dbmanager.DBEntity(paper_table, fields_dict=paper_dict)
        entity.set_field('title', 'New Title')
        assert entity.title == 'New Title'

        with pytest.raises(ValueError):
            entity.set_field('nofield', 'some value')

    def test_save_to_db(self):
        self.manager.reset_database()
        paper_table = self.manager.get_table_object('paper')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = dbmanager.DBEntity(paper_table, fields_dict=paper_dict)
        entity.save_to_db()
        del entity
        paper_table.rows = {}
        new_entity = dbmanager.DBEntity.fetch_entity(paper_table, {'title': 'A Paper'})
        assert new_entity.url == "http://mikethicke.com"
        assert new_entity.row_key == 'idpaper' + dbmanager.DBTable.KEY_STR_DELIMITER + '1'
