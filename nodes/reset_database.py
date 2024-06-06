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

import sys
import uploader.database_setup as database_setup
from tools.logging_tools import logger
from uploader.model import DAO, DBFM

# -----------------------------------------------------------
# This program is used to clear drop all tables in the
# database and re-create them
if __name__ == '__main__':
    logger.info(f'Reset Database')
    logger.warning(f'Warning! All data will be cleared!')
    if '-y' in sys.argv or '-yes' in sys.argv:
       answer = 'Y'
    else:
        answer = input('Are you sure to reset the database (Y or N): ')
    if answer == 'Y':
        database_setup.drop_tables(DAO.db_file)   
        errors = database_setup.create_tables(DAO.db_file)
        if errors is not None:
            logger.error(f'Error in create tabless: {errors}')
        logger.info(f'Reset Database Completed')