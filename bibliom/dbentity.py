"""
DBEntity class.
"""
import logging

from bibliom import exceptions
from bibliom.dbtable import DBTable
from bibliom.dbmanager import DBManager

class DBEntity:
    """
    Class representing a single row in a table. Each DBEntity is associated
    with a DBTable object and conducts all database transactions through that
    object.
    """
    def __init__(self,
                 table,
                 manager=None,
                 row_key=0,
                 fields_dict=None,
                 protect_fields=False,
                 **kwargs):
        if isinstance(table, str):
            if manager is None:
                manager = DBManager.get_manager()
                if manager is None:
                    raise exceptions.BiblioException(
                        'Attempting to create DBEntity, but not given DBTable ' +
                        'instance or manager, and no default manager found.'
                    )
            table = DBTable.get_table_object(table, manager)
        if not isinstance(table, DBTable):
            raise TypeError('table must be DBTable or table name as string')    

        self.__dict__['table'] = table

        self.protect_fields = protect_fields

        if kwargs:
            if not fields_dict:
                fields_dict = {}
            fields_dict = {**fields_dict, **kwargs}

        if row_key:
            self.row_key = row_key
            self.table.get_row_by_key(row_key)
            if fields_dict:
                for key, value in fields_dict:
                    self.set_field(key, value)
        else:
            self.row_key = self.table.create_new_row(fields_dict)

        if self.row_key not in table.entites.keys():
            table.entites[row_key] = self

    def __getattr__(self, attr_name):
        if attr_name in self.table.fields:
            return self.get_field(attr_name)
        raise exceptions.BiblioException(attr_name + ' not in DBTable.fields.')

    def __setattr__(self, attr_name, value):
        if attr_name in self.table.fields:
            if self.protect_fields and self.get_field(attr_name) is not None:
                return
            self.set_field(attr_name, value)
        else:
            object.__setattr__(self, attr_name, value)

    def __eq__(self, other):
        try:
            for field in self.fields_dict.keys():
                if not self.get_field(field) == other.get_field(field):
                    return False
            return True
        except AttributeError:
            return False

    def __repr__(self):
        rep_string = ""
        max_key_length = len(max(self.fields_dict.keys(), key=len))
        max_key_length = max(max_key_length, len("Table"))
        rep_string += "{:{max_key_length}}: {}\n".format(
            "Table: ", self.table.table_name, max_key_length=max_key_length)
        for key, value in self.fields_dict.items():
            if value:
                value = str(value)
                value = value.replace('\n', ' ')
                if len(value) > 50:
                    value = value[:47] + '...'
                rep_string += "{:{max_key_length}}: {}\n".format(
                    key, value, max_key_length=max_key_length)
        return rep_string

    @classmethod
    def entities_from_table_rows(cls, table, rows):
        """
        Returns a list of entities from table corresponding to rows.
        """
        if not rows:
            return []
        if not isinstance(rows, dict):
            raise TypeError('rows must be dictionary of table rows indexed by row_key')
        if not isinstance(table, DBTable):
            raise TypeError('table must be DBTable object')
        entities = [cls(table=table, row_key=key) for key in rows.keys()]
        return entities

    @classmethod
    def fetch_entities(cls, table, where_dict=None, **kwargs):
        """
        Returns a list of entities from table matching where_dict.

        Args:
            table (DBTable): Table object to fetch from.
            where_dict: dictionary of column-value pairs. Value can be
                        "IS NULL", "IS NOT NULL", or a list of values.
                        For comparison operators (>, <, >=, <=, !=) there must
                        be a space between operator and value.
            **kwargs: Each additional keyword argument adds filter to column
                      following rules for where_dict.
        """
        if not isinstance(table, DBTable):
            raise TypeError("table must be DBTable object")
        if where_dict is None:
            where_dict = {}
        if kwargs:
            where_dict = {**where_dict, **kwargs}
        if not where_dict:
            return None
        rows = table.fetch_rows(where_dict)
        return cls.entities_from_table_rows(table, rows)

    @classmethod
    def fetch(cls, table, where_dict=None, **kwargs):
        """
        Returns a single entity from table matching where_dict.

        args:
            table (DBTable): Table to fetch from
            where_dict: dictionary of column-value pairs. Value can be
                        "IS NULL", "IS NOT NULL", or a list of values.
                        For comparison operators (>, <, >=, <=, !=) there must
                        be a space between operator and value.
            **kwargs: Each additional keyword argument adds filter to column
                      following rules for where_dict.
        """
        entity_list = cls.fetch_entities(table, where_dict, **kwargs)
        if entity_list:
            if entity_list[0] is not None:
                logging.getLogger(__name__).debug('Entity.fetch: entity is not None.')
            return entity_list[0]
        return None

    @classmethod
    def entity_from_row(cls, table, row_key):
        """
        Create a new entity associated with row_key.
        """
        row = table.get_row_by_key(row_key)
        if row:
            return cls(table, row_key=row_key)
        raise ValueError("row_key %s not in table %s" % (row_key, table.table_name))

    @classmethod
    def generate_entities(cls, table):
        """
        Returns a dictionary of DBEntities corresponding to table.rows
        """
        entity_dict = {row_key:cls.entity_from_row(table, row_key)
                       for row_key in table.rows.keys()}
        return entity_dict

    @property
    def fields_dict(self):
        """
        Returns dict of fields.
        """
        return self.table.get_row_by_key(self.row_key)

    def append(self, other, overwrite=False):
        """
        Append other's fields to entity. By default, only append fields
        that are not already set. If overwrite is true, overwrite all
        fields.
        """
        if not isinstance(other, DBEntity):
            raise TypeError("other must be of type DBEntity")
        for key, field in self.fields_dict.items():
            if (field is None or overwrite) and other.get_field(key) is not None:
                self.set_field(key, other.get_field(key))

    def get_field(self, field_name):
        """
        Returns the value of a field.
        """
        return self.table.get_row_by_key(self.row_key).get(field_name)

    def set_field(self, field_name, field_value):
        """
        Sets the value of a field.
        """
        if field_name not in self.table.fields:
            raise ValueError(
                "Field %s not in fields for %s table." % (field_name, self.table.table_name)
            )
        self.table.set_field(self.row_key, field_name, field_value)

    def save_to_db(self, duplicates=None):
        """
        Inserts entity into db and updates row_key.
        """
        new_row_key = self.table.insert_row(self.fields_dict, duplicates)
        self.row_key = new_row_key

    def delete_from_db(self):
        """
        Deletes entity from database.
        """
        if self.table.row_status[self.row_key] == DBTable.RowStatus.NEW:
            raise exceptions.DBUnsyncedError(
                'Attempting to delete a row that has not yet been saved to db.'
            )
        self.table.delete_row(self.row_key)

    def undelete(self, duplicates=None):
        """
        Restores a deleted entity and resaves to database.
        """
        if self.table.row_status[self.row_key] != DBTable.RowStatus.DELETED:
            raise exceptions.DBIntegrityError(
                'Attempting to undelete a row that has not been deleted.'
            )
        self.table.row_status[self.row_key] = DBTable.RowStatus.UNSYNCED
        self.save_to_db(duplicates)

