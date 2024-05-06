#!/usr/bin/env python3

# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

# The common modules
import sqlite3
import pandas as pd

# --- returns a db connection object to the db_file
def create_connection(db_file) -> sqlite3.Connection:
    """ Returns a connection object to the given db_file

    :param db_file: The path to the sqlite3 db file
    :return: The sqlite3 conn object
    """
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except sqlite3.Error as e:
        raise e
    
# --- clear the content of a table in the specified DB
def clear_table(db_file, tablename) -> int:
    """ Remove all data from the given table name

    :param db_file: The path to the sqlite3 db file
    :param tablename: The name of the table to be cleared
    :return: The number of rows removed
    """
    with create_connection(db_file) as conn: 
        c = conn.cursor()          
        c.execute(f'DELETE FROM {tablename}' )
        conn.commit()
        return c.rowcount

# return a list of names of the tables
def list_table_names(db_file) -> list:
    """ Return the table names as a list

    :param db_file: The path to the sqlite3 db file
    :return: A list containing table names as strings
    """
    with create_connection(db_file) as conn: 
        c = conn.cursor()   
        results = c.execute('SELECT name FROM sqlite_master WHERE type=\'table\'')
        return [item[0] for item in results]

# --- dump the content of a table as a dataframe
def dump_table_df(db_file, tablename, limit=None, offset=0) -> pd.DataFrame:
    """ Returns the content of the given table name as a pandas dataframe

    :param db_file: The path to the sqlite3 db file
    :param tablename: The name of the table to query as a string
    :param limit: The limit of rows to be returned, defaults to None
    :param offset: The row offset of the result, defaults to 0
    :return: A pandas dataframe
    """
    with create_connection(db_file) as conn: 
        if limit:
            return pd.read_sql(f'SELECT * FROM {tablename} LIMIT {limit} OFFSET {offset}', conn)
        else:
            return pd.read_sql(f'SELECT * FROM {tablename}', conn)

# --- execute sql query the results as a dataframe
def update(db_file, sql, *args) -> int:
    """ Execute a sql update statement, with optional statement parameters as a tuple

    :param db_file: The path to the sqlite3 db file
    :param sql: The sql update statement to be executed
    :return: The number of rows affected
    """
    assert len(args) == 0 or (len(args) == 1 and type(args[0]) == tuple), 'sql statement parameters must be in a tuple'
    with create_connection(db_file) as conn:
        c = conn.cursor()
        c.execute(sql, *args)
        conn.commit()
        return c.rowcount

def update_with_script(db_file, sql_script, *args):
    """Execute a sql script that may contain multiple statement, with optional statement parameters as a tuple

    :param db_file: The path to the sqlite3 db file
    :param sql_script: The sql script as a string
    :return: The number of rows affected
    """
    assert len(args) == 0 or (len(args) == 1 and type(args[0]) == tuple), 'sql statement parameters must be in a tuple'
    with create_connection(db_file) as conn:
        c = conn.cursor()
        c.executescript(sql_script, *args)
        conn.commit()
        return c.rowcount

def update_with_script_no_exception(db_file, sql_script, *args):
    """ Execute a sql script that may contain multiple statement, with optional statement parameters as a tuple, and silent if error happens

    :param db_file: The path to the sqlite3 db file
    :param sql_script: The sql script as a string
    :return: The error as a string if any
    """
    try:
        update_with_script(db_file, sql_script, *args)
        return None
    except Exception as e:
        return str(e)
    
def count_rows(db_file, table_name:str):
    """ Return the number of rows in a table

    :param db_file: The path to the sqlite3 db file
    :param table_name: The name of the table to be queried
    :return: The number of rows
    """
    if table_name is None or ' ' in table_name:
        return 0
    sql = 'SELECT COUNT(*) FROM ' + table_name
    result = query_for_object(db_file, sql, )
    if result is None:
        return 0
    return result
        
# --- execute sql query the results as a dataframe
# formats can be None or one of the following
# 'dict' (default) : dict like {column -> {index -> value}}
# 'list' : dict like {column -> [values]}
# 'series' : dict like {column -> Series(values)}
# 'split' : dict like {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
# 'records' : list like [{column -> value}, … , {column -> value}]
# 'index' : dict like {index -> {column -> value}}
def query(db_file, sql, params=None, format=None):
    """ Execute a sql query statement and returns the result as a pandas dataframe or according to a format.
    The format can be None or one of the following.
        - 'dict' (default) : dict like {column -> {index -> value}}
        - 'list' : dict like {column -> [values]}
        - 'series' : dict like {column -> Series(values)}
        - 'split' : dict like {'index' -> [index], 'columns' -> [columns], 'data' -> [values]}
        - 'records' : list like [{column -> value}, … , {column -> value}]
        - 'index' : dict like {index -> {column -> value}}

    :param db_file: The path to the sqlite3 db file
    :param sql: The sql query statement
    :param params: The additional paramaters for the sql statement as a tuple, defaults to None
    :param format: The format as described above, defaults to None which will return a pandas dataframe
    :return: The result according to the format
    """
    with create_connection(db_file) as conn:
        if params is None:
            df = pd.read_sql(sql, conn)
        else:
            assert type(params) == tuple, 'sql statement parameters must be in a tuple'
            df = pd.read_sql(sql, conn, params=params)
        if format is None:
            return df
        assert format in ['dict', 'list', 'series', 'split', 'records', 'index']
        return df.to_dict(orient=format)

def query_paged(db_file, sql, limit=None, offset=0, params=None, format=None):
    """ A wrapper function for the query method above, with additional parameters limit and offset for paging the results.

    :param db_file: The path to the sqlite3 db file
    :param sql: The sql query statement
    :param limit: The maximum number of rows to return, defaults to None
    :param offset: The row offset, defaults to 0
    :param params: The additional paramaters for the sql statement as a tuple, defaults to None
    :param format: The format as described above, defaults to None which will return a pandas dataframe
    :return: The paged result according to the format
    """
    with create_connection(db_file) as conn:
        if limit:
            sql = f'{sql} LIMIT {limit} OFFSET {offset}'
        return query(db_file, sql, params=params, format=format)

# --- execute sql query that returns one object
def query_for_list(db_file, sql, *args) -> list:
    """ Execute a sql query statement and return the result as a list

    :param db_file: The path to the sqlite3 db file
    :param sql: The sql query statement
    :return: A list containing the rows of the result
    """
    with create_connection(db_file) as conn:
        try:
            results = conn.execute(sql, *args).fetchall()
            result_list = [item[0] for item in results]
            return result_list
        except Exception as e:
            raise

# --- execute sql query that returns a dict of the first row
def query_for_dict(db_file, sql, *args) -> dict:
    """ Execute a sql query statement and return the result as a dict

    :param db_file: The path to the sqlite3 db file
    :param sql: The sql query statement
    :return: A dict containing the first row if more than one row in the result
    """
    with create_connection(db_file) as conn:
        try:
            conn.row_factory = sqlite3.Row
            result = conn.execute(sql, *args).fetchmany(1)
            if len(result) == 0:
                return None
            result_list = {k: result[0][k] for k in result[0].keys()}
            return result_list
        except Exception as e:
            raise

# --- execute sql query that returns one object
def query_for_object(db_file, sql, *args):
    """ Execute a sql query statement and return the result as an object

    :param db_file: The path to the sqlite3 db file
    :param sql: The sql query statement
    :return: An object representing the result
    """
    with create_connection(db_file) as conn:
        try:
            result = conn.execute(sql, *args).fetchone()
            if result is None or len(result) == 0:
                return None
            return result[0]
        except Exception as e:
            raise

# --- execute sql query that returns a list of dicts, each of which is a record (row) in the results
def query_as_list_of_dicts(db_file, sql, *args):
    """ Execute a sql query statement and return the result as a list of dictionaries

    :param db_file: The path to the sqlite3 db file
    :param sql: The sql query statement
    :return: A list containing dictionaries each of which is a row in the results
    """
    with create_connection(db_file) as conn: 
        conn.row_factory = sqlite3.Row
        try:
            results = conn.execute(sql, *args).fetchall()
            result_list = [{k: item[k] for k in item.keys()} for item in results]
            return result_list
        except Exception as e:
            raise