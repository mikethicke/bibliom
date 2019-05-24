"""
Unit tests for dbtable.py
"""

# pylint: disable=unused-variable, missing-docstring, no-member, len-as-condition

import logging

import pytest

from bibliom.dbtable import DBTable
from bibliom.dbentity import DBEntity
from bibliom import exceptions

@pytest.mark.usefixtures('class_manager')
class TestDBEntity():
    """
    Tests for DBEntity class.
    """
    def test_init(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_init')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object('paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.title == 'A Paper'
        assert entity.idpaper == 1

        entity2 = DBEntity.entity_from_row(paper_table, entity.row_key)
        assert entity2.title == 'A Paper'

        entity3 = DBEntity(paper_table, row_key=entity.row_key)
        assert entity3.title == 'A Paper'

        entity4 = DBEntity(paper_table)
        assert entity4.title is None

        entity5 = DBEntity('paper', manager=self.manager, fields_dict=paper_dict)
        assert entity5.title == 'A Paper'
        assert entity5.table == paper_table

        entity6 = DBEntity('paper', fields_dict=paper_dict)
        assert entity6.title == 'A Paper'
        assert entity6.table == paper_table

        entity7 = DBEntity('paper', title='Another Paper')
        assert entity7.title == 'Another Paper'
        assert entity7.table == paper_table

    def test_repr(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_repr')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        rt = repr(entity)
        assert isinstance(rt, str)

    def test_getattr(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_getattr')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.title == 'A Paper'

        with pytest.raises(exceptions.BiblioException):
            a = entity.tile # misspelling of field

    def test_setattr(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_setattr')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.title == 'A Paper'
        entity.title = 'Another Title'
        assert entity.title == 'Another Title'
        assert paper_table.rows[entity.row_key]['title'] == 'Another Title'

    def test_entities_from_table_rows(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_entities_from_table_rows')
        self.manager.reset_database()
        author_table = DBTable.get_table_object('author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.insert_many_new_rows()
        entities = DBEntity.entities_from_table_rows(author_table, author_table.rows)
        assert len(entities) == 100
        assert entities[0].last_name == 'Numberer'

        entities = DBEntity.entities_from_table_rows(author_table, [])
        assert entities == []

        with pytest.raises(TypeError):
            entities = DBEntity.entities_from_table_rows('hello', author_table.rows)

        with pytest.raises(ValueError):
            entities = DBEntity.entities_from_table_rows(author_table, {'hello': 'world'})

        entities = DBEntity.entities_from_table_rows(author_table, {})
        assert entities == []

    def test_fetch_entities(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_fetch_entities')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.insert_many_new_rows()
        entities = DBEntity.fetch_entities(author_table, {'last_name': 'Numberer'})
        assert len(entities) == 100
        assert entities[0].last_name == 'Numberer'

        entities = DBEntity.fetch_entities(author_table, {'last_name': 'Nothing'})
        assert entities == []

        with pytest.raises(TypeError):
            entities = DBEntity.fetch_entities('author', {'last_name': 'Nothing'})

        assert DBEntity.fetch_entities(author_table, []) is None

    def test_fetch_entity(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_fetch_entity')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        author_table.insert_many_new_rows()
        entity = DBEntity.fetch(author_table, {'last_name': 'Numberer'})
        assert isinstance(entity, DBEntity)
        assert entity.last_name == 'Numberer'
        assert entity.orcid is None

    def test_entity_from_row(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_entity_from_row')
        self.manager.reset_database()
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        row_key = paper_table.create_new_row(paper_dict)
        new_entity = DBEntity.entity_from_row(paper_table, row_key)
        assert new_entity.title == 'A Paper'

        with pytest.raises(ValueError):
            bad_entity = DBEntity.entity_from_row(
                paper_table,
                'idpaper' +
                paper_table.KEY_STR_DELIMITER +
                str(2)
            )

    def test_generate_entities(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_generate_entities')
        self.manager.reset_database()
        author_table = DBTable.get_table_object( 'author', self.manager)
        for i in range(0, 100):
            author_table.create_new_row({
                'last_name':    'Numberer',
                'given_names':  str(i+1)
            })
        entities = DBEntity.generate_entities(author_table)
        assert len(entities) == 100
        assert isinstance(
            list(entities.values())[0],
            DBEntity)

    def test_fields_dict(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_fields_dict')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.fields_dict['title'] == 'A Paper'
        assert entity.fields_dict['url'] == "http://mikethicke.com"
        assert entity.fields_dict['content'] is None

    def test_append(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_append')
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
        }
        paper2_dict = {
            'title':        'A Different Paper',
            'url':          "http://mikethicke.com",
            'first_page':   10
        }
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        p1 = DBEntity(paper_table, fields_dict=paper_dict)
        p2 = DBEntity(paper_table, fields_dict=paper2_dict)
        p1.append(p2)
        assert p1.first_page == 10
        assert p1.title == 'A Paper'
        p1.append(p2, overwrite=True)
        assert p1.title == 'A Different Paper'
        with pytest.raises(TypeError):
            p1.append(paper2_dict)

    def test_get_field(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_get_field')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        assert entity.get_field('title') == 'A Paper'

    def test_set_field(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_set_field')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        entity.set_field('title', 'New Title')
        assert entity.title == 'New Title'

        with pytest.raises(ValueError):
            entity.set_field('nofield', 'some value')

    def test_save_to_db(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_save_to_db')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)
        entity.save_to_db()
        del entity
        paper_table.rows = {}
        new_entity = DBEntity.fetch(paper_table, {'title': 'A Paper'})
        assert new_entity.url == "http://mikethicke.com"
        assert new_entity.row_key == 'idpaper' + DBTable.KEY_STR_DELIMITER + '1'

    def test_eq(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_eq')
        self.manager.reset_database()
        paper_table = DBTable.get_table_object( 'paper', self.manager)
        paper_dict = {
            'title':    'A Paper',
            'url':      "http://mikethicke.com",
            'idpaper':  1
        }
        entity = DBEntity(paper_table, fields_dict=paper_dict)

        entity_2 = DBEntity(paper_table)
        assert entity != entity_2

        entity_3 = DBEntity(paper_table, fields_dict=paper_dict)
        assert entity == entity_3

        assert entity is not None
        assert entity != 'A Paper'

    def test_protect_fields(self):
        logging.getLogger('bibliom.pytest').debug('-->TestDBEntity.test_protect_fields')
        entity = DBEntity('paper', title='Paper', protect_fields=True)
        assert entity.title == 'Paper'
        entity.title = 'Another Paper'
        assert entity.title == 'Paper'
        entity.protect_fields = False
        entity.title = 'Another Paper'
        assert entity.title == 'Another Paper'
