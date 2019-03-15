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
from bibliom.constants import MAX_DB_RETRIES

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
        self.close()

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
                "Connecting to database %s as %s", self.name, self.user)
            try:
                self.db = MySQLdb.connect("localhost",
                                          self.user,
                                          self.password,
                                          self.name,
                                          charset=self.charset,
                                          use_unicode=self.use_unicode,
                                          connect_timeout=5)
            except MySQLdb.Error as e:
                logging.getLogger(__name__).exception("Failed to connect to database.")
                #Unknown database
                if e.args[0] == 1049:
                    raise exceptions.UnknownDatabaseError("Database %s not found" % self.name)
                else:
                    raise
            else:
                logging.getLogger(__name__).debug("Successfully connected to database.")

    def close(self):
        """
        Close database connection.
        """
        logging.getLogger(__name__).debug("Closing database connection to %s", self.name)
        if self.db is not None:
            try:
                self.db.close()
                self.db = None
            except (MySQLdb.Error, MySQLdb.Warning):
                logging.getLogger(__name__).exception("Failed to close database connection.")
            else:
                logging.getLogger(__name__).debug("Closed database connection.")

    def _run_sql(self, sql_statements, ignore_exceptions=False):
        """
        Run a list of SQL statements.

        Args:
            sql_statements ([string]): List of SQL statements.

        Returns:
            True if all statements execute sucessfully.
        """
        if self.db is not None:
            cursor = self.db.cursor()
            for statement in sql_statements:
                if statement:
                    try:
                        cursor.execute(statement)
                    except MySQLdb.Error:
                        if not ignore_exceptions:
                            raise
            return True
        return False

    def _run_sql_file(self, sql_source_file):
        """
        Run SQL statements contained in file.
        """
        logging.getLogger(__name__).debug("Running SQL file %s", sql_source_file)
        try:
            with open(sql_source_file, 'r') as f:
                sql_source = f.read()
            sql_statements = re.split(r';\s*$', sql_source, flags=re.MULTILINE)
            sql_statements = [cmd.strip('\n') for cmd in sql_statements]
        except IOError:
            logging.getLogger(__name__).exception("Could not read file %s", sql_source_file)
            raise

        return self._run_sql(sql_statements)

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
    def _query_params(param_dictionary, allow_none=False):
        """
        Helps to build queries from a dictionary of parameters where some of
        the parameters might be empty. This is useful to avoid inadvertently
        blanking database entries.

        Args:
            param_dictionary: a dictionary of db fields and values
            allow_none      : if True, will not skip fields with None as value

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
                if value is not None or allow_none:
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

        logging.getLogger(__name__).debug("Creating database %s", name)

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
            self.close()

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
        logging.getLogger(__name__).debug("Successfully creatted database %s", name)

    def drop_database(self):
        """
        Drops current database.
        """
        logging.getLogger(__name__).debug("Dropping database %s", self.name)
        if self.name is None:
            return
        query = "DROP DATABASE %s" % self.name
        cursor = self.db.cursor()
        retries = 0
        while True:
            try:
                cursor.execute(query)
                self.close()
                logging.getLogger(__name__).debug("Successfully dropped database.")
                return
            except MySQLdb.Error:
                if retries < MAX_DB_RETRIES:
                    continue
                logging.getLogger(__name__).exception("Error dropping database %s", self.name)
                raise

    def reset_database(self, sql_source_file=None):
        """
        Drops database and re-creates.
        """
        logging.getLogger(__name__).debug("Resetting database.")
        try:
            self.drop_database()
        except MySQLdb.OperationalError as e:
            if e.args[0] != 1008: # Database doesn't exist
                raise
        self.create_database(self.name, sql_source_file)
        self.dbtables = {}
        logging.getLogger(__name__).debug("Successfully resetted database.")

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
            List of dictionaries of column-value, or None.
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
        rows = cursor.fetchall()
        return list(rows)

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
            params = DBManager._query_params(
                {field : row_dict_list[0].get(field) for field in self.table_fields(table_name)},
                allow_none=True
            )
        except (IndexError, KeyError, TypeError):
            raise TypeError("row_dict_list must be list of dicts of column:value pairs.")

        query = ("INSERT INTO %s (%s) VALUES (%s)"
                 % (table_name, params['key_str'], params['value_alias']))
        logging.getLogger(__name__).debug(
            "Inserting %d rows into table %s. Query: %s",
            len(row_dict_list), table_name, query)
        rows_lists = [list(rd.values()) for rd in row_dict_list]
        rows_lists = [
            [rd.get(field) for field in self.table_fields(table_name)]
            for rd in row_dict_list
        ]

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
