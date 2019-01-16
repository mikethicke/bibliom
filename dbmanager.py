"""
Module for interfacing with MySQL database. Meant to be generic, so doesn't
assume anything about what tables, etc. are in the database. Project-specific
customizations should be done in another modue.
"""
import re
import MySQLdb

class DBManager:
    """
    Class for managing connection to MySQL database. All SQL should be
    contained within this class, with other classes making requests to this
    class. This should maximize portability.
    """

    KEY_STR_DELIMITER = "%%"

    def __init__(self, name=None, user=None, password=None):
        self.name = name
        self.user = user
        self.password = password

        self.db = None

        self.dbtables = {}

    def __str__(self):
        return self.name

    @staticmethod
    def _build_where(where_dict=None, or_clause=False):
        """
        Builds a where clause for query

        Args:
            where_dict: dictionary of column-value pairs. Value can be
                        "IS NULL", "IS NOT NULL", or a list of values.
                        For comparison operators (>, <, >=, <=, !=) there must
                        be a space between operator and value.

        Returns:
            (where_clause (str), value_list)
        """

        value_list = []
        if where_dict is None:
            return ("1", [])

        if not isinstance(where_dict, dict):
            raise TypeError("where_dict must be dictionary")

        where = ""
        conj = " OR " if or_clause else " AND "
        for where_key, where_value in where_dict.items():
            if where:
                where = where + conj
            if where_value == 'NULL':
                where += "%s IS NULL" % where_key
            elif where_value == 'NOT NULL':
                where += "%s IS NOT NULL" % where_key
            elif isinstance(where_value, list) and where_value:
                if (isinstance(where_value[0], str)
                        and where_value[0].startswith('%')):
                    where += "("
                    first = True
                    for li in where_value:
                        if not first:
                            where += conj
                        else:
                            first = False
                        where += where_key + " LIKE %s"
                        value_list.append(li)
                    where += ")"
                else:
                    where += "%s IN (" % where_key
                    in_clause = ""
                    for li in where_value:
                        if in_clause:
                            in_clause += ", "
                        in_clause += "%s"
                        value_list.append(li)
                    where += in_clause + ")"
            elif str(where_value).startswith('%'):
                where += where_key + " LIKE %s"
                value_list.append(where_value)
            elif (str(where_value).startswith('>')
                  or str(where_value).startswith('<')
                  or str(where_value).startswith('>=')
                  or str(where_value).startswith('<=')
                  or str(where_value).startswith('!=')):
                m = re.match(r'(.*) (.*)', where_value)
                if m is not None:
                    where += "{key} {operator} '{value}'".format(
                        key=where_key,
                        operator=m.group(1),
                        value=MySQLdb.escape_string(str(m.group(2))).decode('UTF-8')
                    )
            else:
                where += where_key + "=%s"
                value_list.append(where_value)

        return (where, value_list)

    @staticmethod
    def _query_params(param_dictionary):
        """
        Helps to build queries from a dictionary of parameters where some of
        the parameters might be empty. This is useful to avoid inadvertently
        blanking database entries.

        Args:
            param_dictionary: a dictionary of db fields and values

        Returns:
            A dictionary:   'key_str': affected db fields,
                            'value_alias': correct number of "%s"s for query string,
                            'value_list': list of values for query,
                            'update_str': "key = %s"s for each key in update query,
                            'where_or_clause': string for where clause connected by "OR"s
        """
        if param_dictionary and isinstance(param_dictionary, dict):
            key_str = ""
            value_list = []
            value_alias = ""
            update_str = ""
            where_or_clause = ""
            for key, value in param_dictionary.items():
                if value is not None:
                    if key_str != "":
                        key_str += ", "
                        update_str += ", "
                        where_or_clause += "OR "
                    key_str += str(key)
                    value_list.append(value)
                    update_str += str(key) + "= %s"
                    where_or_clause += str(key) + "=%s "
            for i in range(0, len(value_list)):
                value_alias += "%s"
                if i < len(value_list) - 1:
                    value_alias += ", "
            return {'key_str'       : key_str,
                    'value_alias'   : value_alias,
                    'value_list'    : value_list,
                    'update_str'    : update_str,
                    'where_or_clause'    : where_or_clause}
        return None

    @staticmethod
    def dict_to_key(key_dict):
        """
        Generates a key string from a dict of column:value.
        """
        key_str = DBManager.KEY_STR_DELIMITER.join(key_dict.keys())
        key_str += (DBManager.KEY_STR_DELIMITER
                    + DBManager.KEY_STR_DELIMITER.join(key_dict.values()))
        return key_str

    @staticmethod
    def key_to_dict(key_str):
        """
        Generates a dictionary of column:value from key_str.
        """
        key_list = key_str.split(DBManager.KEY_STR_DELIMITER)
        key_length = len(key_list) / 2
        dict_keys = key_list[key_length:]
        dict_values = key_list[:key_length]
        key_dict = {dict_keys[i]:dict_values[i] for i in range(0, key_length-1)}
        return key_dict

    def connect(self, charset="utf8mb4", use_unicode=True):
        """
        Connect to the database.
        """
        if (self.user is not None
                and self.password is not None
                and self.name is not None):
            self.db = MySQLdb.connect("localhost",
                                      self.user,
                                      self.password,
                                      self.name,
                                      charset=charset,
                                      use_unicode=use_unicode)

    def close(self):
        """
        Close database connection.
        """
        if self.db is not None:
            try:
                self.db.close()
            except (MySQLdb.Error, MySQLdb.Warning) as e:
                print("In db.close - Error: %s" % (e,))

    def list_tables(self):
        """
        Returns list of tables in database.
        """
        if self.db is not None:
            query = "SHOW TABLES;"
            try:
                cursor = self.db.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
            except MySQLdb.Error as e:
                print("In db.list_tables - Error: %s " % (e,))
                return False
            table_list = [i[0] for i in results]
            return table_list
        return []

    def table_structure(self, table_name):
        """
        Returns structure of table.

        Args:
            table_name (str): Name of a table existing in the database.

        Returns:
            dictionary of dictionaries where outer dictionary is keyed on
            field name, and inner dictionary on attribute names
        """
        if self.db is not None:
            query = "DESCRIBE %s;" % (str(table_name, ))
            try:
                cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
                cursor.execute(query)
                results = cursor.fetchall()
            except MySQLdb.Error as e:
                print("In db.table_structure - Error: %s" % (e,))
                return {}
            table_dict = {}
            for table_field in results:
                field_name = table_field['Field']
                del table_field['Field']
                table_field = {k.lower(): v for k, v in table_field.items()}
                table_dict[field_name] = table_field
            return table_dict
        return {}

    def primary_key_list(self, table_name):
        """
        Returns primary keys of table_name as list.
        """
        table_structure = self.table_structure(table_name)
        key_list = []
        for field, att_dict in table_structure.items():
            if att_dict['key'] == 'PRI':
                key_list.append(field)
        return key_list

    def table_fields(self, table_name):
        """
        Returns list of fields in table_name.
        """
        table_dict = self.table_structure(table_name)
        if table_dict:
            return list(table_dict.keys())
        return []

    def existing_table_object_keys(self):
        """
        Returns list of existing DBTable objects associated with self.
        """
        return self.dbtables.keys()

    def get_table_object(self, table_name):
        """
        If DBTable object for table_name exists, return it. Otherwise
        return new DBTable for table_name and add it to self.dtbales.
        """
        if table_name in self.dbtables.keys():
            return self.dbtables[table_name]
        if table_name in self.list_tables():
            new_dbtable = DBTable(self, table_name)
            return new_dbtable
        return False

    def get_table_objects(self):
        """
        Returns dictionary of DBTable objects, one for each table in database.
        Keys are table names.
        """
        for table_name in self.list_tables():
            self.get_table_object(table_name)
        table_object_dictionary = self.dbtables
        return table_object_dictionary

    def fetch_row(self, table_name, where_dict):
        """
        Fetches a row from table_name matching where_dict
        """
        (where_clause, value_list) = DBManager._build_where(where_dict)
        query = "SELECT * FROM %s WHERE %s" % (table_name, where_clause)
        cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
        try:
            cursor.execute(query, value_list)
        except MySQLdb.Error as e:
            print(e)
        result = cursor.fetchone()
        return result

    def fetch_rows(self, table_name, where_dict=None, limit=0):
        """
        Fetches rows from table_name.

        Args:
            table_name (str): Name of table to fetch from.
            where_dict (dict): Dictionary of column:value pairs
            limit (int): Max rows to return, or 0 for unlimited

        Returns:
            Dictionary of dictionaries where outer dictionary indexed by
            primary key of table and inner dictionary indexed by column
            name.
        """
        (where_clause, value_list) = DBManager._build_where(where_dict)
        query = "SELECT * FROM %s WHERE %s" % (table_name, where_clause)
        if limit:
            query += " LIMIT %s" % limit
        cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
        try:
            cursor.execute(query, value_list)
        except MySQLdb.Error as e:
            print(e)
        results = cursor.fetchall()
        primary_keys = self.primary_key_list(table_name)
        rows_dict = {}
        for result in results:
            key_str = DBManager.dict_to_key({key:result[key] for key in primary_keys})
            rows_dict[key_str] = result
        return rows_dict

    def insert_row(self, table_name, row_dict):
        """
        Inserts a row into table.

        Args:
            table_name (str): Name of table for insertion.
            row_dict (dict): Dictionary of field:value pairs.

        Returns:
            If successful, lastrowid if available, -1 otherwise. False otherwise
        """
        params = DBManager._query_params(row_dict)
        query = ("INSERT INTO %s (%s) VALUES (%s)"
                 % (table_name, params['key_str'], params['value_alias']))
        try:
            cursor = self.db.cursor()
            cursor.execute(query, params['value_list'])
            self.db.commit()
        except MySQLdb.Error as e:
            print(e)
            self.db.rollback()
            return False
        lastrowid = cursor.lastrowid
        if not lastrowid:
            lastrowid = -1
        return lastrowid

    def update_row(self, table_name, key_dict, row_dict):
        """
        Update row with primary key key_dict according to row_dict
        """
        params = DBManager._query_params(row_dict)
        where_clause = DBManager._build_where(key_dict)
        query = ("UPDATE %s SET %s WHERE %s" %
                 table_name,
                 params['update_str'],
                 where_clause)
        try:
            cursor = self.db.cursor()
            cursor.execute(query, params['value_list'])
            self.db.commit()
            return True
        except MySQLdb.Error as e:
            print(e)
            self.db.rollback()
            return False

class DBTable:
    """
    Class representing a table in database. Each DBTable is associated with
    a DMBanager which mediates all transactions with the database.
    """
    NEW_ID_PREFIX = "db_table_new"

    def __init__(self, manager, table_name):
        if table_name in manager.existing_table_object_keys():
            raise AssertionError('Attempted to create a DBTable object, but one '
                                 + ' already exists for this table and manager.')
        self.manager = manager
        self.table_name = table_name
        self.manager.dbtables[table_name] = self
        self.rows = {}
        self.row_status = {}
        self.next_key = 0

    def __str__(self):
        return "%s|%s" % (self.manager, self.table_name)

    def fields_list(self):
        """
        Returns list of fields in table.
        """
        return self.manager.table_fields(self.table_name)

    def table_structure(self):
        """
        Returns structure of table as dictionary of fields.
        """
        return self.manager.table_structure(self.table_name)

    def fetch_rows(self, where_dict=None, limit=0, overwrite=True):
        """
        Fetch rows from database, add to self.rows, and return.

        Args:
            where_dict (dict): Dictionary of column-value pairs
                               @see DBManager._build_where
            limit (int):       Max number of rows to retrieve
            overwrite (bool):  If true, replace existing rows when primary key
                               matches. Otherwise, skip matching rows.
        """
        rows = self.manager.fetch_rows(self.table_name, where_dict, limit)
        for row_key, row_data in rows:
            if overwrite or row_key not in self.rows.keys()
                self.rows[row_key] = row_data
                self.row_status[row_key] = DBTable.RowStatus.SYNCED
        return rows

    def get_row_by_key(self, row_key):
        """
        Returns a row dictionary. If row_key is in self.rows, return that. If not,
        query manager for row, add it to self.rows, and return.
        """
        if row_key in self.rows.keys():
            return self.rows[row_key]
        row = self.manager.fetch_row(self.table_name, DBManager.key_to_dict(row_key))
        self.rows[row_key] = row
        return self.rows[row_key]

    def entity_from_row(self, row_key):
        """
        Create a new entity associated with row_key.
        """
        if row_key in self.rows.keys():
            return DBEntity(self, row_key)
        return DBEntity(self)

    def generate_entities(self):
        """
        Returns a dictionary of DBEntities corresponding to self.rows
        """
        entity_dict = {row_key:self.entity_from_row(row_key)
                       for row_key in self.rows.keys()}
        return entity_dict

    class RowStatus:
        """
        Constants for keeping track of row statuses in table.
        """
        NEW = 0
        SYNCED = 1
        UNSYNCED = 2

    def create_new_row(self, fields_dict=None):
        """
        Adds a new row to self.rows and returns the key for that
        row. Row not added to database until table is synced.
        """
        row_key = DBTable.NEW_ID_PREFIX + DBManager.KEY_STR_DELIMITER + str(self.next_key)
        self.next_key += 1
        if fields_dict is None:
            self.rows[row_key] = {field:None for field in self.fields_list()}
        else:
            self.rows[row_key] = {field:fields_dict[field]
                                  for field in self.fields_list()}
        self.row_status[row_key] = DBTable.RowStatus.NEW
        return row_key

    def set_field(self, row_key, field_name, field_value):
        """
        Sets the value of a field in a row. If row's status is SYNCED, set
        status to UNSYNCED.
        """
        self.rows[row_key][field_name] = field_value
        if self.row_status[row_key] == DBTable.RowStatus.SYNCED:
            self.row_status[row_key] = DBTable.RowStatus.UNSYNCED

    def sync_to_db(self):
        """
        Updates db from self.rows, and sets all row statuses to SYNCED.
        """
        for row_key, row_dict in self.rows.items():
            if self.row_status[row_key] == DBTable.RowStatus.NEW:
                new_pri_key = self.manager.insert_row(self.table_name, row_dict)
                pkl = self.manager.primary_key_list(self.table_name)
                if new_pri_key > 0 and len(pkl) == 1:
                    # Row inserted successfully and primary key returned
                    new_row_key = DBManager.dict_to_key({pkl[0]:new_pri_key})
                elif new_pri_key == -1:
                    # Row inserted successfuly, but no AUTO INCRIMENT primary key
                    # so primary key must be a subset of row_dict.
                    new_key_dict = {key:row_dict[key]
                                    for key in self.manager.primary_key_list(self.table_name)}
                    new_row_key = DBManager.dict_to_key(new_key_dict)
                else:
                    raise RuntimeError('In DBTable.sync_to_db: Primary key is ' +
                                       'neither AUTO INCRIMENT nor subset of row_dict.')
                self.rows[new_row_key] = row_dict
                self.row_status[new_row_key] = DBTable.RowStatus.SYNCED
                del self.rows[row_key]
            elif self.row_status[row_key] == DBTable.RowStatus.UNSYNCED:
                if (self.manager.update_row(self.table_name,
                                            DBManager.key_to_dict(row_key),
                                            row_dict)):
                    self.row_status[row_key] = DBTable.RowStatus.SYNCED
                else:
                    raise RuntimeError('In DBTable.sync_to_db: Row failed to update.')

class DBEntity:
    """
    Class representing a single row in a table. Each DBEntity is associated
    with a DBTable object and conducts all database transactions through that
    object.
    """
    def __init__(self, db_table, row_key=0, fields_dict=None):
        self.db_table = db_table
        if row_key:
            self.row_key = row_key
            self.db_table.get_row_by_key(row_key)
        else:
            self.row_key = self.db_table.create_new_row(fields_dict)

    def get_field(self, field_name):
        """
        Returns the value of a field.
        """
        return self.db_table.get_row_by_key(self.row_key).get(field_name)

    def set_field(self, field_name, field_value):
        """
        Sets the value of a field.
        """
        self.db_table.set_field(self.row_key, field_name, field_value)

def unit_test():
    """
    Runs unit test for module.
    """
    import pprint
    pp = pprint.PrettyPrinter(indent=4)
    print("\n***Testing DBManager***\n")

    database_name = "test_db"
    database_user = "test_user"
    database_password = "jfYf2NoJr4DMHrF,3b"
    print(">>> db = DBManager('%s', '%s', '%s')"
          % (database_name, database_user, database_password))
    db = DBManager(database_name, database_user, database_password)

    print(">>> db.connect()")
    db.connect()

    print(">>> db_table_list = db.list_tables()")
    db_table_list = db.list_tables()
    print(db_table_list)

    print(">>> a_table_structure = db.table_structure('%s')" % db_table_list[0])
    a_table_structure = db.table_structure(db_table_list[0])
    pp.pprint(a_table_structure)

    print(">>> table_field_list = db.table_fields('%s')" % db_table_list[0])
    table_field_list = db.table_fields(db_table_list[0])
    pp.pprint(table_field_list)

    print(">>> db.insert_row(%s, {'last_name': 'Thicke', 'given_names': 'Mike'})"
          % db_table_list[0])
    db.insert_row(db_table_list[0], {'last_name': 'Thicke', 'given_names': 'Mike'})
    print(">>> db.fetch_row(%s, {'last_name': 'Thicke'})" % db_table_list[0])
    row = db.fetch_row(db_table_list[0], {'last_name': 'Thicke'})
    pp.pprint(row)
    print(">>> rows = db.fetch_rows('%s', {'last_name': 'NOT NULL'}, 20)" % db_table_list[0])
    rows = db.fetch_rows(db_table_list[0], {'last_name': 'NOT NULL'}, 20)
    pp.pprint(rows)

    print("\n***Testing DBTable***\n")

    print(">>> a_db_table = DBTable(db, '%s')" % db_table_list[0])
    a_db_table = DBTable(db, db_table_list[0])
    print(a_db_table)
    print("List of existing keys: %s" % db.existing_table_object_keys())
    print(">>> all_table_objects = db.get_table_objects()")
    all_table_objects = db.get_table_objects()
    pp.pprint(db.existing_table_object_keys())
    print(">>> duplicate_db_table = DBTable(db, '%s')" % db_table_list[0])
    print("# This should generate an exception...")
    try:
        duplicate_db_table = DBTable(db, db_table_list[0])
    except AssertionError as e:
        print(e)

    print(">>> db.close()")
    db.close()


if __name__ == "__main__":
    unit_test()
