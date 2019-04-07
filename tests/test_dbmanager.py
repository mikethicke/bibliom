"""
Unit tests for dbmanager.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition

import logging

import MySQLdb
import pytest

from bibliom.dbmanager import DBManager
from bibliom import exceptions

DB_NAME = 'test_db'
DB_USER = 'test_user'
DB_PASSWORD = 'jfYf2NoJr4DMHrF,3b'

@pytest.mark.usefixtures('class_manager')
class TestDBManager():
    """
    Tests for DBManager class.
    """
    def test_init(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_init')
        with pytest.raises(exceptions.UnknownDatabaseError):
            bad_manager = DBManager(
                name='aaaaadfasfasdfdasfdsa',
                user=DB_USER,
                password=DB_PASSWORD
            )
        with pytest.raises(MySQLdb.Error):
            bad_manager = DBManager(
                name=DB_NAME,
                user='adfdsafadsg',
                password=DB_PASSWORD
            )
        with pytest.raises(MySQLdb.Error):
            bad_manager = DBManager(
                name=DB_NAME,
                user=DB_USER,
                password='sadfdsafsdf'
            )
            assert bad_manager.db is None
        manager = DBManager(
            name=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        assert isinstance(manager.db, MySQLdb.connections.Connection)

    def test_str(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_str')
        assert str(self.manager) == 'test_db'

    def test_build_where(self):
        # pylint: disable=protected-access
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_build_where')
        manager = DBManager()
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
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_get_query_params')
        manager = DBManager()
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
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_create_database')
        manager = DBManager()
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
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_drop_database')
        manager = DBManager(
            name='test_db_2',
            user=DB_USER,
            password=DB_PASSWORD
        )
        manager.drop_database()
        assert manager.db is None

    def test_reset_database(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_reset_database')
        self.manager.reset_database()
        assert self.manager.name == 'test_db'
        assert isinstance(self.manager.db, MySQLdb.connections.Connection)

    def test_list_tables(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_list_tables')
        assert isinstance(self.manager.list_tables(), list)

    def test_table_structure(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_table_structure')
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.table_structure(table_name),
            dict)

    def test_primary_key_list(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_primary_key_list')
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.primary_key_list(table_name),
            list
        )

    def test_foreign_key_list(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_foreign_key_list')
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.foreign_key_list(table_name),
            list
        )

    def test_table_fields(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_table_fields')
        table_name = self.manager.list_tables()[0]
        assert isinstance(
            self.manager.table_fields(table_name),
            list
        )
        assert len(self.manager.table_fields(table_name)) > 0

    def test_existing_table_object_keys(self):
        logging.getLogger('bibliom.pytest').debug(
            '-->TestDBManager.test_existing_table_object_keys'
        )
        assert isinstance(
            self.manager.existing_table_object_keys(),
            list
        )
        assert len(self.manager.existing_table_object_keys()) == 0

    def test_insert_row(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_insert_row')
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

    def test_insert_many_rows(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_insert_many_rows')
        self.manager.reset_database()
        table_name = 'author'

        rows = [
            {
                'last_name':    'Thicke',
                'given_names':  'Michael Lowell Ellis',
                'h-index':      '1',
                'orcid':        '0000-0003-1401-2056'
            },
            {
                'last_name':    'Thïcké',
                'given_names':  'Michāel',
                'h-index':      '1',
                'orcid':        '0000-0003-1401-2056-2',
                'corporate':    False
            }
        ]
        self.manager.insert_many_rows(table_name, rows)

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
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_update_rows')
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
        assert fetched_rows[0]['given_names'] == 'Mike'

    def test_fetch_row(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_fetch_row')

        self.manager.reset_database()
        rows = [
            {
                'last_name':    'Thicke',
                'given_names':  'Michael Lowell Ellis',
                'h-index':      '1',
                'orcid':        '0000-0003-1401-2056',
                'corporate':    False
            },
            {
                'last_name':    'Thïcké',
                'given_names':  'Michāel',
                'h-index':      '1',
                'orcid':        '0000-0003-1401-2056-2',
                'corporate':    False
            }
        ]
        table_name = 'author'
        self.manager.insert_many_rows(table_name, rows)
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
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_fetch_rows')

        self.manager.reset_database()
        rows = [
            {
                'last_name':    'Thicke',
                'given_names':  'Michael Lowell Ellis',
                'h-index':      '1',
                'orcid':        '0000-0003-1401-2056',
                'corporate':    False
            },
            {
                'last_name':    'Thïcké',
                'given_names':  'Michāel',
                'h-index':      '1',
                'orcid':        '0000-0003-1401-2056-2',
                'corporate':    False
            }
        ]
        table_name = 'author'
        self.manager.insert_many_rows(table_name, rows)
        rows = self.manager.fetch_rows(table_name, {'given_names': '%Mich%'})
        assert isinstance(rows, list)
        assert len(rows) == 2
        rows2 = self.manager.fetch_rows(table_name, given_names='%Mich%')
        assert rows2 == rows
        for row in rows:
            assert isinstance(row, dict)
        with pytest.raises(MySQLdb.OperationalError): #should this raise a ValueError instead?
            rows = self.manager.fetch_rows(table_name, {'llast_name': 'Thicke'})

    def test_delete_rows(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_delete_rows')
        self.manager.reset_database()
        table_name = 'author'
        rows = []
        for i in range(0, 100):
            rows.append({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        self.manager.insert_many_rows(table_name, rows)
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
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_import_dict')
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
        assert papers[0]['title'] == 'Towards an analysis of unanalyzability'

    def test_clear_table(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBManager.test_clear_table')
        self.manager.clear_table('author')
        authors = self.manager.fetch_rows('author')
        assert len(authors) == 0
