#!/usr/bin/env python3

# Copyright 2023 - Andrew Kwok Fai LUI, Centre for Robotics
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2023, The CGRAS Project'
__license__ = 'GPL'
__version__ = '0.0.1'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

import sys
from tools import db_tools
from uploader.model import DAO

# The function that supports interactive execution of sql statements 
def run_db(dao):
    while True:
        print(f'''
        (E): Exit
        (Q): Run Query
        (U): Run Update
        ''')
        command = input('Command: ')
        if command == 'E':
            sys.exit(0)
        elif command == 'Q':
            while True:
                sql = input('Enter SQL: ')
                if not sql: break
                try:
                    df = db_tools.query(dao.db_file, sql)
                    print(df)
                except Exception as e:
                    print(f'Error: {e}')
        elif command == 'U':
            while True:
                sql = input('Enter SQL: ')
                if not sql: break
                try:
                    result = db_tools.update(dao.db_file, sql)
                    print(result)
                except Exception as e:
                    print(f'Error: {e}')

# ------------------------------------------------
# The main program for running a command line 
# program for executing sql statements
if __name__ == '__main__':
        run_db(DAO)
