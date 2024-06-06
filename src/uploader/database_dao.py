#!/usr/bin/env python3

# Copyright 2023 - Andrew Kwok Fai LUI, Centre for Robotics
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2023, The CGRAS Project'
__license__ = 'GPL'
__version__ = '0.0.1'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

# ----- the common modules
import os, json, random, copy, glob, collections, sys, time, datetime, shutil
from enum import Enum
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from datetime import datetime as dt
import sqlite3
import uploader.database_setup as database_setup
import tools.db_tools as db_tools
import tools.file_tools as file_tools
from tools.lock_tools import synchronized
from tools.logging_tools import logger

# This class models the management and backup of db files and the folder that contains the files
class DBFileManager():
    DB_FOLDER_NAME = 'sqlite3'
    DB_FILENAME = 'uploader.db'
    def __init__(self):
        try:
            self.user_home = os.path.expanduser('~') 
            self.db_filename = DBFileManager.DB_FILENAME
            self.db_file = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../db', self.db_filename))
            logger.info(f'DBFileManager sqlite db_file: {self.db_file}')
            self.db_parent_folder = file_tools.get_parent(self.db_file)
            # - test if the db_file exists, if not, create one
            if not os.path.isfile(self.db_file):
                database_setup.create_tables(self.db_file)
            else:
                # - making a backup of the database file if it has not been made this day
                self._make_daily_backup()
        except Exception as e:
            raise AssertionError(f'system database setup error: {e}')
        
    def _make_daily_backup(self):
        today = datetime.date.today()
        date_str = today.strftime("%Y-%m-%d")
        daily_backup_filename = f'{date_str}_{self.db_filename}'
        daily_backup_path = os.path.join(self.db_parent_folder, daily_backup_filename)
        if os.path.isfile(daily_backup_path):
            return
        shutil.copyfile(self.db_file, daily_backup_path)
    
    def make_backup(self, label='S', use_move=False):
        now = datetime.now()
        time_str = now.strftime("%Y-%m-%d-%H-%M-%S")
        daily_backup_filename = f'{time_str}-{label}_{self.db_filename}' 
        daily_backup_path = os.path.join(self.db_parent_folder, daily_backup_filename)
        if os.path.isfile(daily_backup_path):
            return
        if use_move:
            shutil.move(self.db_file, daily_backup_path)        
        else:
            shutil.copyfile(self.db_file, daily_backup_path)               
        
    def get_backup_files(self):
        file_info_dict = dict()
        for f in os.listdir(self.db_parent_folder):
            fpath = os.path.join(self.db_parent_folder, f)
            if not os.path.isfile(fpath) :
                continue
            if f == self.db_filename or self.db_filename not in f:
                continue 
            backup_date, _ = f.split('_', 1)
            file_info_dict[backup_date] = f
        return file_info_dict
    
    def restore_backup(self, backup_filename):
        backup_filepath = os.path.join(self.db_parent_folder, backup_filename)
        if not os.path.isfile(backup_filepath) :
            return
        # make a backup of the existing db_file
        self.make_backup(label='RES', use_move=True)
        # make a copy of the backup file and save as db_file
        shutil.copyfile(backup_filepath, self.db_file)
                
# This class is the data access object for the gdrive uploader
class UploaderDAO():
    def __init__(self, config, db_file=None):
        if db_file is None:
            db_file = DBFM.db_file
        self.db_file = db_file
        self.CONFIG = config
        self.cached_started_scan = None # the cached current started scan

    # -------- validating the database
    # return True if there is at least one tile, one tank, one station, and one pattern for the operation
    @synchronized
    def validate_db(self):
        with db_tools.create_connection(self.db_file) as conn:       
            c = conn.cursor() 
            result = c.execute('SELECT COUNT(*) FROM general_config').fetchone()
            if not result or result[0] == 0:
                return False         
        return True

    # -------- general configuration
    @synchronized
    def set_config_value(self, name, value):
        with db_tools.create_connection(self.db_file) as conn:
            c = conn.cursor()
            c.execute('REPLACE INTO general_config (name, value) VALUES (?, ?)', (name, value))
            conn.commit()
        return name   
    
    @synchronized
    def get_config_value(self, name, default=None):
        with db_tools.create_connection(self.db_file) as conn:       
            c = conn.cursor() 
            result = c.execute('SELECT value FROM general_config WHERE name = ?', (name,)).fetchone()
            if result is None:
                return default
            return result[0]

    # --------- the upload file queue
  
    def add_upload_file(self, local_path:str, filename:str, remote_path:str, timestamp:float):
        with db_tools.create_connection(self.db_file) as conn:
            c = conn.cursor()
            c.execute('REPLACE INTO upload_queue (local_path, filename, remote_path, timestamp) VALUES (?, ?, ?, ?)', 
                      (local_path, filename, remote_path, timestamp,))
            conn.commit()
        return

    def error_delay_upload_timestamp(self, local_path:str, timestamp:int=None):
        with db_tools.create_connection(self.db_file) as conn:
            c = conn.cursor()
            if timestamp is not None:
                c.execute('UPDATE upload_queue SET timestamp = ?, error_count = error_count + 1 WHERE local_path = ?', (timestamp, local_path, ))              
            conn.commit()
        return
    
    def list_upload_queue(self):
        sql = 'SELECT * FROM upload_queue'
        return db_tools.query(self.db_file, sql)         
    
    def query_next_upload(self, limit=1):
        sql = 'SELECT * FROM upload_queue WHERE DATETIME(timestamp, "unixepoch", "localtime") < DATETIME("now", "localtime") LIMIT ?'
        return db_tools.query_as_list_of_dicts(self.db_file, sql, (limit,)) 

    def query_next_upload_within_error(self, limit=1, max_error_count=10):
        sql = 'SELECT * FROM upload_queue WHERE DATETIME(timestamp, "unixepoch", "localtime") < DATETIME("now", "localtime") AND error_count <= ? LIMIT ?'
        return db_tools.query_as_list_of_dicts(self.db_file, sql, (max_error_count, limit,)) 

    def remove_upload_file(self, local_path:str):
        sql = 'DELETE FROM upload_queue WHERE local_path = ?'
        return db_tools.update(self.db_file, sql, (local_path,))

    def clear_upload_queue(self):
        sql = 'DELETE FROM upload_queue'
        return db_tools.update(self.db_file, sql)

    # --------- the upload record
    def add_upload_record(self, local_path:str, file_size:int, duration:int):
        with db_tools.create_connection(self.db_file) as conn:
            c = conn.cursor()
            c.execute('INSERT INTO upload_record (timestamp, local_path, file_size, duration) VALUES (?, ?, ?, ?)', 
                      (int(time.time()), local_path, file_size, duration,))
            conn.commit()
        return

    def list_upload_record(self, limit=100):
        sql = 'SELECT * FROM upload_record ORDER BY timestamp DESC LIMIT ?'
        return db_tools.query(self.db_file, sql, (limit,))         

    def get_upload_stat(self):
        sql = 'SELECT COUNT(*) as count, SUM(file_size) as file_size, SUM(duration) as duration FROM upload_record'
        return db_tools.query_for_dict(self.db_file, sql)          

# ------------------------------------------------
# The main program for testing the clearing
# of database tables and creating them
if __name__ == '__main__':
    DBFM = DBFileManager()
    DAO = UploaderDAO(DBFM.db_file)
    # database_setup.drop_tables(DAO.db_file)   
    errors = database_setup.create_tables(DAO.db_file)
    if errors is not None:
        logger.error(f'Error in create tabless: {errors}')
