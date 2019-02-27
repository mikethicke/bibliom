"""
Module for interfacing with MySQL database. Meant to be generic, so doesn't
assume anything about what tables, etc. are in the database. Project-specific
customizations should be done in another modue.
"""
import logging
import re
import os

import MySQLdb

from bibliom import exceptions

# Above this number of table rows, will log progress when syncing to db.
INFO_THRESHOLD = 10000

# Frequency to log progress when syncing to db.
REPORT_FREQUENCY = 5000

class DBManager:
    """
    Class for managing connection to MySQL database. All SQL should be
    contained within this class, with other classes making requests to this
    class. This should maximize portability.
    """

    def __init__(self, name=None, user=None, password=None, charset="utf8mb4", use_unicode=True):
        self.name = name
        self.user = user
        self.password = password

        self.charset = charset
        self.use_unicode = use_unicode
        self.db = None

        self._connect()

        self.dbtables = {}

    def __del__(self):
        self._close()

    def __str__(self):
        return self.name

    def _connect(self):
        """
        Connect to the database.
        """
        if (self.user is not None
                and self.password is not None
                and self.name is not None):
            logging.getLogger(__name__).debug(
                "Connecting to database %s as %s", self.user, self.password)
            try:
                self.db = MySQLdb.connect("localhost",
                                          self.user,
                                          self.password,
                                          self.name,
                                          charset=self.charset,
                                          use_unicode=self.use_unicode)
            except MySQLdb.Error as e:
                logging.getLogger(__name__).exception("Failed to connect to database.")
                #Unknown database
                if e.args[0] == 1049:
                    raise exceptions.UnknownDatabaseError("Database %s not found" % self.name)
                else:
                    raise
            else:
                logging.getLogger(__name__).debug("Successfully connected to database.")

    def _close(self):
        """
        Close database connection.
        """
        if self.db is not None:
            try:
                self.db.close()
                self.db = None
            except (MySQLdb.Error, MySQLdb.Warning):
                logging.getLogger(__name__).exception("Failed to close database connection.")
            else:
                logging.getLogger(__name__).debug("Closed database connection.")


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
                where += "`%s` IS NULL" % where_key
            elif where_value == 'NOT NULL':
                where += "`%s` IS NOT NULL" % where_key
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
                    where += "`%s` IN (" % where_key
                    in_clause = ""
                    for li in where_value:
                        if in_clause:
                            in_clause += ", "
                        in_clause += "%s"
                        value_list.append(li)
                    where += in_clause + ")"
            elif str(where_value).startswith('%'):
                where += "`" + where_key + "` LIKE %s"
                value_list.append(where_value)
            elif (str(where_value).startswith('>')
                  or str(where_value).startswith('<')
                  or str(where_value).startswith('>=')
                  or str(where_value).startswith('<=')
                  or str(where_value).startswith('!=')):
                m = re.match(r'(.*) (.*)', where_value)
                if m is not None:
                    where += "`{key}` {operator} '{value}'".format(
                        key=where_key,
                        operator=m.group(1),
                        value=MySQLdb.escape_string(str(m.group(2))).decode('UTF-8')
                    )
            else:
                where += "`" + where_key + "`=%s"
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
        if param_dictionary:
            if not isinstance(param_dictionary, dict):
                raise TypeError('Needs to be called with a dictionary of db fields and values.')
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
                    key_str += "`" + str(key) + "`"
                    value_list.append(value)
                    update_str += "`" + str(key) + "`=%s"
                    where_or_clause += "`" + str(key) + "`=%s "
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

    def create_database(self, name=None, sql_source_file=None):
        """
        Creates a new database and connects to it.
        """
        if name is None:
            name = self.name
        if name is None:
            raise ValueError("Create database requires database name.")

        logging.getLogger(__name__).debug("Connecting to database %s", name)

        if self.user is None or self.password is None:
            raise ValueError("User and password must be set before creating database.")

        if sql_source_file is None:
            sql_source_file = os.path.join(
                os.path.abspath(os.path.dirname(__file__)),
                'config',
                'create_db_tables.sql')
        try:
            with open(sql_source_file, 'r') as f:
                sql_source = f.read()
            sql_commands = sql_source.split(';')
            sql_commands = [cmd.strip('\n') for cmd in sql_commands]
        except IOError:
            logging.getLogger(__name__).exception("Could not read file %s", sql_source_file)
            raise

        if self.db is not None:
            self._close()

        query = "CREATE DATABASE %s" % name

        try:
            temp_db = MySQLdb.connect("localhost",
                                      self.user,
                                      self.password,
                                      charset=self.charset,
                                      use_unicode=self.use_unicode)
        except MySQLdb.Error:
            logging.getLogger(__name__).exception("Error connecting to database server.")
            raise

        try:
            cursor = temp_db.cursor()
            cursor.execute(query)
        except MySQLdb.Error:
            logging.getLogger(__name__).exception(
                "Error attempting to create new database with name %s", name)
            temp_db.close()
            raise

        try:
            query = "USE %s" % name
            cursor.execute(query)
            for command in sql_commands:
                if command:
                    cursor.execute(command)
            created_tables = True
        except MySQLdb.Error:
            logging.getLogger(__name__).exception("Error creating database tables.")
            created_tables = False

        if not created_tables:
            query = "DROP DATABASE %s" % name
            cursor.execute(query)
            raise exceptions.FailedDatabaseCreationError("Failed to create database tables.")

        temp_db.close()
        self.name = name
        self._connect()

    def drop_database(self):
        """
        Drops current database.
        """
        if self.name is None:
            return
        query = "DROP DATABASE %s" % self.name
        cursor = self.db.cursor()
        try:
            cursor.execute(query)
            self._close()
        except MySQLdb.Error:
            logging.getLogger(__name__).exception("Error dropping database %s", self.name)
            raise

    def reset_database(self, sql_source_file=None):
        """
        Drops database and re-creates.
        """
        try:
            self.drop_database()
        except MySQLdb.OperationalError as e:
            if e.args[0] != 1008: # Database doesn't exist
                raise
        self.create_database(self.name, sql_source_file)
        self.dbtables = {}

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
                print("DBManager.list_tables: %s " % (e,))
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
                print("DBManager.table_structure: %s" % (e,))
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

    def foreign_key_list(self, table_name):
        """
        Returns a list of foreign keys.

        Args:
            table_name (str): The name of the source table
        Returns:
            List of dicts, where each list item corresponds to a foreign key,
            and dictionary fields are:
                column_name: name of column in table_name containing foreign key.
                referenced_table_name: name of referenced table.
                referenced_column_name: name of referenced column.
        """
        query = ("SELECT COLUMN_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_NAME "
                 + "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                 + "WHERE TABLE_NAME = %s "
                 + "AND REFERENCED_TABLE_NAME IS NOT NULL "
                 + "AND TABLE_SCHEMA = %s")
        try:
            cursor = self.db.cursor(MySQLdb.cursors.DictCursor)
            cursor.execute(query, (table_name, self.name))
            results = cursor.fetchall()
        except MySQLdb.Error as e:
            print("DBManager.foreign_key_list: %s" % (e,))
            return []
        key_list = [{'column_name':item['COLUMN_NAME'],
                     'referenced_table_name':item['REFERENCED_TABLE_NAME'],
                     'referenced_column_name':item['REFERENCED_COLUMN_NAME']}
                    for item in results]
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
        return list(self.dbtables.keys())

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
            logging.getLogger(__name__).exception(
                "Failed to fetch row. Query: %s Error: %s", query, e)
            raise
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
            logging.getLogger(__name__).exception(
                "Failed to fetch rows. Query: %s Error: %s", query, e)
            raise
        results = cursor.fetchall()
        primary_keys = self.primary_key_list(table_name)
        rows_dict = {}
        for result in results:
            key_str = DBTable.dict_to_key({key:result[key] for key in primary_keys})
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
            self.db.rollback()
            if e.args[0] == 1062: #Duplicate entry
                raise
            else:
                logging.getLogger(__name__).exception(
                    "Failed to insert row. Query: %s Error: %s", query, e)
                raise

        lastrowid = cursor.lastrowid
        if not lastrowid:
            lastrowid = -1
        return lastrowid

    def insert_many_rows(self, table_name, row_dict_list):
        """
        Inserts many rows into a table.

        Args:
            table_name (str): Name of table for insertion
            row_dict_list [{column:value}]:
                List of row dicts. Each dict must have the same set of
                column:value pairs.

        Returns: row_dict_list updated with auto incremented primary keys if available
        """
        if (not isinstance(row_dict_list, list) or
                not isinstance(row_dict_list[0], dict)):
            raise TypeError("row_dict_list must be list of dicts of column:value pairs.")
        # Get auto increment primary key if one exists
        table_dict = self.table_structure(table_name)
        auto_increment = None
        for key, field in table_dict.items():
            if field['key'] == 'PRI' and field['extra'] == 'auto_increment':
                query = ("SELECT AUTO_INCREMENT " +
                         "FROM information_schema.tables " +
                         "WHERE table_name = '%s' " % table_name +
                         "AND table_schema = DATABASE();")
                cursor = self.db.cursor()
                cursor.execute(query)
                result = cursor.fetchone()
                if result:
                    auto_increment = int(result[0])
                pri_key_field = key
                break

        if auto_increment is not None:
            for row_dict in row_dict_list:
                if row_dict.get(pri_key_field) is not None:
                    continue
                row_dict[pri_key_field] = auto_increment
                auto_increment += 1

        try:
            params = DBManager._query_params(row_dict_list[0])
        except (IndexError, KeyError, TypeError):
            raise TypeError("row_dict_list must be list of dicts of column:value pairs.")

        query = ("INSERT INTO %s (%s) VALUES (%s)"
                 % (table_name, params['key_str'], params['value_alias']))
        logging.getLogger(__name__).debug(
            "Inserting %d rows into table %s. Query: %s",
            len(row_dict_list), table_name, query)
        rows_lists = [list(rd.values()) for rd in row_dict_list]
        rows_lists = [[item for item in row if item] for row in rows_lists]
        try:
            cursor = self.db.cursor()
            cursor.executemany(query, rows_lists)
            self.db.commit()
        except MySQLdb.Error as e:
            logging.getLogger(__name__).exception(
                "Failed to insert rows. Query: %s Error: %s", query, str(e))
            self.db.rollback()
            raise
        return row_dict_list

    def update_rows(self, table_name, row_dict, where_dict):
        """
        Update rows matching where_dict according to row_dict.

        Args:
            table_name (str): Name of the table to update.
            row_dict (dict): Column:value pairs to update
            where_dict (dict): Column:value pairs for where clause.

        Returns:
            True if at least one row is updated, False otherwise.
        """
        params = DBManager._query_params(row_dict)
        (where_clause, where_values) = DBManager._build_where(where_dict)
        query = ("UPDATE %s SET %s WHERE %s" %
                 (table_name,
                  params['update_str'],
                  where_clause))
        try:
            cursor = self.db.cursor()
            cursor.execute(query, params['value_list'] + where_values)
            self.db.commit()
            return cursor.rowcount > 0
        except MySQLdb.Error as e:
            logging.getLogger(__name__).exception(
                "Failed to update row. Query: %s Error %s", query, str(e))
            self.db.rollback()
            raise

    def delete_rows(self, table_name, where_dict, or_clause=False):
        """
        Deletes rows from table_name matching where_dict.

        Returns:
            True if at least one row affected. False otherwise.
        """
        (where_clause, value_list) = DBManager._build_where(where_dict, or_clause)
        query = "DELETE FROM %s WHERE %s" % (table_name, where_clause)
        logging.getLogger(__name__).debug(
            "Deleting rows from database. Query: %s", query)
        try:
            cursor = self.db.cursor()
            cursor.execute(query, value_list)
            self.db.commit()
            return cursor.rowcount > 0
        except MySQLdb.Error as e:
            logging.getLogger(__name__).exception(
                "Failed to delete rows from database. Query: %s Error: %s", query, str(e))
            self.db.rollback()
            return False

    def import_dict(self, db_dict):
        """
        Imports a dict of dicts into database.

        Args:
            db_dict (dict([dict])): Outer dict is indexed by table name and
                                    inner dict is indexed by column name.
        """
        for table_name, table_list in db_dict.items():
            self.insert_many_rows(table_name, table_list)

    def clear_table(self, table_name):
        """
        Delete all rows from table_name.
        """
        self.delete_rows(table_name, where_dict=None)

    def sync_to_db(self):
        """
        Sync all tables to database.
        """
        logging.getLogger(__name__).debug("Syncing all tables to database.")
        self.get_table_objects()
        for table in self.dbtables.values():
            table.sync_to_db()

class DBTable:
    """
    Class representing a table in database. Each DBTable is associated with
    a DMBanager which mediates all transactions with the database.
    """
    NEW_ID_PREFIX = "db_table_new"

    def __init__(self, manager, table_name):
        if table_name in manager.existing_table_object_keys():
            raise exceptions.BiblioException(
                'Attempted to create a DBTable object, but one ' +
                ' already exists for this table and manager.')
        self.manager = manager
        self.table_name = table_name
        self.manager.dbtables[table_name] = self
        self.rows = {}
        self.row_status = {}
        self.entites = {}
        self.next_key = 0
        self.fields = self.manager.table_fields(self.table_name)

    def __str__(self):
        return "%s|%s" % (self.manager, self.table_name)

    KEY_STR_DELIMITER = "%%"

    @staticmethod
    def dict_to_key(key_dict):
        """
        Generates a key string from a dict of column:value.
        """
        if not isinstance(key_dict, dict):
            raise TypeError('key_dict must be a dict of column:value.')
        key_str = DBTable.KEY_STR_DELIMITER.join(key_dict.keys())
        key_str += (DBTable.KEY_STR_DELIMITER
                    + DBTable.KEY_STR_DELIMITER.join(
                        [str(i) for i in key_dict.values()]))
        return key_str

    @staticmethod
    def key_to_dict(key):
        """
        Generates a dictionary of column:value from key.
        """
        if not isinstance(key, str):
            raise TypeError('key must be a codeded string.')
        key_list = str(key).split(DBTable.KEY_STR_DELIMITER)
        key_length = len(key_list) // 2
        dict_keys = key_list[:key_length]
        dict_values = key_list[key_length:]
        if not key_length or len(dict_keys) != len(dict_values):
            raise ValueError("key %s improperly formatted" % key)
        key_dict = {}
        for i in range(0, key_length):
            try:
                key_dict[dict_keys[i]] = int(dict_values[i])
            except ValueError:
                key_dict[dict_keys[i]] = dict_values[i]
        return key_dict

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
        for row_key, row_data in rows.items():
            if overwrite or not row_key in self.rows.keys():
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
        row = self.manager.fetch_row(self.table_name, DBTable.key_to_dict(row_key))
        self.rows[row_key] = row
        return self.rows[row_key]

    def get_row_by_primary_key(self, primary_key):
        """
        Returns a row dictionary where the row's key is created from the primary key.
        Only works for tables with a single-column primary key.
        """
        key_cols = self.manager.primary_key_list(self.table_name)
        if len(key_cols) != 1:
            raise AttributeError("DBTable.get_row_by_primary_key: can only be "
                                 + "called for a table with a single primary key column.")
        row_key = self.dict_to_key({key_cols[0]:primary_key})
        return self.get_row_by_key(row_key)

    class Duplicates:
        """
        How to handle duplicate entries when inserting rows.

        SKIP: Skip inserting the row, leaving original entry unchanged.
        MERGE: New values overwrite old values but unset fields in new row left unchanged.
        OVERWRITE: Old row is dropped and new row inserted in its place.
        """
        SKIP = 1
        MERGE = 2
        OVERWRITE = 3

    def insert_row(self, row_dict, duplicates=None):
        """
        Inserts a row into table.

        Args:
            row_dict (dict): Dictionary of field:value pairs.

        Returns:
            If successful, lastrowid if available, -1 otherwise. False otherwise
        """
        if duplicates is None:
            duplicates = DBTable.Duplicates.SKIP

        try:
            new_pri_key = self.manager.insert_row(self.table_name, row_dict)
            duplicate_entry = False
        except MySQLdb.IntegrityError as e:
            if e.args[0] == 1062: #Duplicate entry
                m = re.match('Duplicate entry \'(.*)\' for key', e.args[1])
                if m is not None:
                    duplicate_value = m.group(1)
                duplicate_entry = True
            else:
                raise

        if duplicate_entry:
            duplicate_key = None
            for key, value in row_dict.items():
                if str(value) == duplicate_value:
                    duplicate_key = key
                    break
            if duplicate_key:
                old_entity = DBEntity.fetch_entity(self, {duplicate_key:duplicate_value})
            else:
                old_entity = DBEntity.fetch_entity(self, row_dict)
                if old_entity:
                    return old_entity.row_key
                else:
                    return None
            if duplicates == self.Duplicates.SKIP:
                return old_entity.row_key
            elif duplicates == self.Duplicates.OVERWRITE:
                self.delete_row(old_entity.row_key)
                return self.insert_row(row_dict, duplicates)
            elif duplicates == self.Duplicates.MERGE:
                for key, value in row_dict.items():
                    if not value:
                        del row_dict[key]
                if self.update_row(old_entity.row_key, row_dict):
                    self.row_status[old_entity.row_key] = DBTable.RowStatus.SYNCED
                    for key, value in row_dict.items():
                        self.rows[old_entity.row_key][key] = value
                return old_entity.row_key
            else:
                raise AttributeError("Parameter 'duplicates' has unknown value.")
        else:
            pkl = self.manager.primary_key_list(self.table_name)
            if new_pri_key > 0 and len(pkl) == 1:
                # Row inserted successfully and primary key returned
                new_row_key = DBTable.dict_to_key({pkl[0]:new_pri_key})
                row_dict[pkl[0]] = new_pri_key
            elif new_pri_key == -1:
                # Row inserted successfuly, but no AUTO INCRIMENT primary key
                # so primary key must be a subset of row_dict.
                new_key_dict = {key:row_dict[key]
                                for key in self.manager.primary_key_list(self.table_name)}
                new_row_key = DBTable.dict_to_key(new_key_dict)
            else:
                raise exceptions.BiblioException(
                    'Primary key is neither AUTO INCRIMENT nor subset of row_dict.')
            self.rows[new_row_key] = {}
            for field_key in self.fields:
                self.rows[new_row_key][field_key] = row_dict.get(field_key)
            self.row_status[new_row_key] = DBTable.RowStatus.SYNCED
            return new_row_key

    def insert_many_new_rows(self):
        """
        Inserts all new rows into database and then deletes them.

        This should be used to insert many new rows when sync_to_db would be
        inefficient.
        """
        new_keys = [key for key, value in self.row_status.items()
                    if value == self.RowStatus.NEW]
        new_rows = [row for key, row in self.rows.items() if key in new_keys]
        updated_rows = self.manager.insert_many_rows(self.table_name, new_rows)
        if updated_rows:
            for key in new_keys:
                del self.row_status[key]
                del self.rows[key]
            pkl = self.manager.primary_key_list(self.table_name)
            for row in updated_rows:
                key_dict = {}
                for key in pkl:
                    key_dict[key] = row[key]
                key_str = self.dict_to_key(key_dict)
                self.rows[key_str] = row
                self.row_status[key_str] = self.RowStatus.SYNCED
            return True
        return False

    def update_row(self, row_key, row_dict):
        """
        Update row with primary key key_dict according to row_dict.

        Args:
            row_key (str or dict): Primary key or key dict of row to update.
            row_dict: Dict of column:value pairs to update row with.

        Returns:
            True if successful, False otherwise.
        """
        return self.manager.update_rows(
            self.table_name,
            row_dict,
            DBTable.key_to_dict(row_key))

    def delete_row(self, row_key):
        """
        Delete row from table_name with key matching key.

        Args:
            table_name (str): Name of the table to delete from.
            key (str, int or dict): Primary key or key dict of row to delete.
        """
        del self.rows[row_key]
        return self.manager.delete_rows(
            self.table_name,
            DBTable.key_to_dict(row_key))

    def delete_rows(self, row_keys):
        """
        Deletes rows from table_name matching where_dict.
        """
        if ((not isinstance(row_keys, list)) or
                row_keys and not isinstance(row_keys[0], str)):
            raise TypeError("row_keys must be list of row keys to delete")
        for row_key in row_keys:
            self.delete_row(row_key)

    def entity_from_row(self, row_key):
        """
        Create a new entity associated with row_key.
        """
        row = self.get_row_by_key(row_key)
        if row:
            return DBEntity(self, row_key)
        raise ValueError("row_key %s not in table %s" % (row_key, self.table_name))

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
        row_key = DBTable.NEW_ID_PREFIX + DBTable.KEY_STR_DELIMITER + str(self.next_key)
        self.next_key += 1
        if fields_dict is None:
            self.rows[row_key] = {field:None for field in self.fields}
        else:
            if not isinstance(fields_dict, dict):
                raise TypeError("fields_dict must by dict of column:value pairs")
            for key in fields_dict.keys():
                if key not in self.fields:
                    raise ValueError("fields_dict contains fields not in %s table" %
                                     self.table_name)
            self.rows[row_key] = {field:fields_dict.get(field)
                                  for field in self.fields}
        self.row_status[row_key] = DBTable.RowStatus.NEW
        return row_key

    def add_rows(self, rows):
        """
        Adds new rows from list of fields dicts or dict of fields dicts.
        """
        if not isinstance(rows, dict) and not isinstance(rows, list):
            raise TypeError("rows must be list of fields dicts or dict of fields dicts")
        if isinstance(rows, dict):
            rows = rows.values()
        for row in rows:
            self.create_new_row(row)

    def set_field(self, row_key, field_name, field_value):
        """
        Sets the value of a field in a row. If row's status is SYNCED, set
        status to UNSYNCED.
        """
        row = self.rows.get(row_key)
        if row is None:
            raise ValueError("row_key %s not found in table %s" %(row_key, self.table_name))
        self.rows[row_key][field_name] = field_value
        if self.row_status[row_key] == DBTable.RowStatus.SYNCED:
            self.row_status[row_key] = DBTable.RowStatus.UNSYNCED

    def sync_to_db(self):
        """
        Updates db from self.rows, and sets all row statuses to SYNCED.

        Returns row_key of last new inserted row.
        """
        logging.getLogger(__name__).debug('Syncing table %s to db.', self.table_name)
        new_row_key = None
        row_keys = list(self.rows.keys())
        for count, row_key in enumerate(row_keys):
            row_dict = self.rows[row_key]
            if self.row_status[row_key] == DBTable.RowStatus.NEW:
                new_row_key = self.insert_row(row_dict)
                self.row_status[new_row_key] = DBTable.RowStatus.SYNCED
                del self.rows[row_key]
            elif self.row_status[row_key] == DBTable.RowStatus.UNSYNCED:
                if (self.manager.update_rows(self.table_name,
                                             row_dict,
                                             DBTable.key_to_dict(row_key))):
                    self.row_status[row_key] = DBTable.RowStatus.SYNCED
                else:
                    raise exceptions.BiblioException('In DBTable.sync_to_db: Row failed to update.')
            if len(row_keys) >= INFO_THRESHOLD and count % REPORT_FREQUENCY == 0:
                logging.getLogger(__name__).verbose_info(
                    "Synced %s / %s rows to db.",
                    count,
                    len(row_keys)
                )
        return new_row_key

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
            self.__dict__[attr_name] = value

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
            raise ValueError("Field %s not in fields for %s table." % (field_name, self.db_table.table_name))
        self.db_table.set_field(self.row_key, field_name, field_value)

    def save_to_db(self, duplicates=None):
        """
        Inserts entity into db and updates row_key.
        """
        new_row_key = self.db_table.insert_row(self.fields_dict, duplicates)
        self.row_key = new_row_key
