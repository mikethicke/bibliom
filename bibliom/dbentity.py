"""
DBEntity class.
"""

from bibliom import exceptions
from bibliom.dbtable import DBTable

class DBEntity:
    """
    Class representing a single row in a table. Each DBEntity is associated
    with a DBTable object and conducts all database transactions through that
    object.
    """
    def __init__(self,
                 db_table,
                 row_key=0,
                 fields_dict=None):
        self.__dict__['db_table'] = db_table

        if row_key:
            self.row_key = row_key
            self.db_table.get_row_by_key(row_key)
        else:
            self.row_key = self.db_table.create_new_row(fields_dict)

        if self.row_key not in db_table.entites.keys():
            db_table.entites[row_key] = self

    def __getattr__(self, attr_name):
        if attr_name in self.db_table.fields:
            return self.get_field(attr_name)
        raise exceptions.BiblioException(attr_name + ' not in DBTable.fields.')

    def __setattr__(self, attr_name, value):
        if attr_name in self.db_table.fields:
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
        class_name = type(self).__name__
        rep_string += "<<%s instance>>\n" % class_name
        rep_string += "{:{max_key_length}}: {}\n".format(
            "Table: ", self.db_table.table_name, max_key_length=max_key_length)
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
    def entities_from_table_rows(cls, db_table, rows):
        """
        Returns a list of entities from db_table corresponding to rows.
        """
        if not isinstance(rows, dict):
            raise TypeError('rows must be dictionary of table rows indexed by row_key')
        if not isinstance(db_table, DBTable):
            raise TypeError('db_table must be DBTable object')
        entities = [cls(db_table=db_table, row_key=key) for key in rows.keys()]
        return entities

    @classmethod
    def fetch_entities(cls, db_table, where_dict):
        """
        Returns a list of entities from db_table matching where_dict.
        """
        if not isinstance(db_table, DBTable):
            raise TypeError("db_table must be DBTable object")
        rows = db_table.fetch_rows(where_dict)
        return cls.entities_from_table_rows(db_table, rows)

    @classmethod
    def fetch_entity(cls, db_table, where_dict):
        """
        Returns a single entity from db_table matching where_dict.
        """
        entity_list = cls.fetch_entities(db_table, where_dict)
        if entity_list:
            return entity_list[0]
        return None

    @classmethod
    def entity_from_row(cls, db_table, row_key):
        """
        Create a new entity associated with row_key.
        """
        row = db_table.get_row_by_key(row_key)
        if row:
            return cls(db_table, row_key)
        raise ValueError("row_key %s not in table %s" % (row_key, db_table.table_name))

    @classmethod
    def generate_entities(cls, db_table):
        """
        Returns a dictionary of DBEntities corresponding to db_table.rows
        """
        entity_dict = {row_key:cls.entity_from_row(db_table, row_key)
                       for row_key in db_table.rows.keys()}
        return entity_dict

    @property
    def fields_dict(self):
        """
        Returns dict of fields.
        """
        return self.db_table.get_row_by_key(self.row_key)

    def get_field(self, field_name):
        """
        Returns the value of a field.
        """
        return self.db_table.get_row_by_key(self.row_key).get(field_name)

    def set_field(self, field_name, field_value):
        """
        Sets the value of a field.
        """
        if field_name not in self.db_table.fields:
            raise ValueError(
                "Field %s not in fields for %s table." % (field_name, self.db_table.table_name)
            )
        self.db_table.set_field(self.row_key, field_name, field_value)

    def save_to_db(self, duplicates=None):
        """
        Inserts entity into db and updates row_key.
        """
        new_row_key = self.db_table.insert_row(self.fields_dict, duplicates)
        self.row_key = new_row_key
