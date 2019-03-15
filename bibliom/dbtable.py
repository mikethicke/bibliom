"""
DBTable class.
"""
import logging
import re

import MySQLdb

from bibliom import exceptions
from bibliom.constants import INFO_THRESHOLD, REPORT_FREQUENCY

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

    @staticmethod
    def get_table_object(manager, table_name):
        """
        If DBTable object for table_name exists, return it. Otherwise
        return new DBTable for table_name and add it to self.dtbales.
        """
        if table_name in manager.dbtables.keys():
            return manager.dbtables[table_name]
        if table_name in manager.list_tables():
            new_dbtable = DBTable(manager, table_name)
            return new_dbtable
        return False

    @staticmethod
    def get_table_objects(manager):
        """
        Returns dictionary of DBTable objects, one for each table in database.
        Keys are table names.
        """
        for table_name in manager.list_tables():
            DBTable.get_table_object(manager, table_name)
        table_object_dictionary = manager.dbtables
        return table_object_dictionary

    @staticmethod
    def sync_tables_to_db(manager):
        """
        Sync all tables to database.
        """
        logging.getLogger(__name__).debug("Syncing all tables to database.")
        table_dict = DBTable.get_table_objects(manager)
        for table in table_dict.values():
            table.sync_to_db()
        logging.getLogger(__name__).debug("Successfully synced tables to database.")

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

        primary_keys = self.manager.primary_key_list(self.table_name)
        rows_dict = {}
        for row in rows:
            key_str = DBTable.dict_to_key({key:row[key] for key in primary_keys})
            if overwrite or key_str not in self.rows.keys():
                rows_dict[key_str] = row
                self.rows[key_str] = row
                self.row_status[key_str] = DBTable.RowStatus.SYNCED
        return rows_dict


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
            if e.args[0] == 1062: #Duplicate entr
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
                    duplicate_key = DBTable.dict_to_key({key:value})
                    break
            if duplicate_key:
                old_row = self.get_row_by_key(duplicate_key)
            else:
                old_row = self.fetch_rows(row_dict)
                if old_row:
                    return duplicate_key
                else:
                    return None
            if duplicates == self.Duplicates.SKIP:
                return duplicate_key
            elif duplicates == self.Duplicates.OVERWRITE:
                self.delete_row(duplicate_key)
                return self.insert_row(row_dict, duplicates)
            elif duplicates == self.Duplicates.MERGE:
                for key, value in row_dict.items():
                    if not value:
                        del row_dict[key]
                if self.update_row(duplicate_key, row_dict):
                    self.row_status[duplicate_key] = DBTable.RowStatus.SYNCED
                    for key, value in row_dict.items():
                        self.rows[duplicate_key][key] = value
                return duplicate_key
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
