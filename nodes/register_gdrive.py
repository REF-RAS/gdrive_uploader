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

# ----- the common modules
import os, sys
from tools.logging_tools import logger
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

# -----------------------------------------------------------
# This program is used to grant this applicationaccess 
# to a Google Drive account
if __name__ == '__main__':
    logger.warning(f'GoogleDrive File Uploader: Setup Google Drive Account')
    logger.warning(f'This command enables the connection of a target google drive account for file upload')
    settings_file_path = os.path.join(os.path.dirname(__file__), '../config/settings.yaml')
    gauth = GoogleAuth(settings_file=settings_file_path)
    gauth.LocalWebserverAuth()
    # setup user configuration for the secret credential file
    uploader_credential_folder = os.path.join(os.path.expanduser('~'), '.config/gdrive_uploader')
    os.makedirs(uploader_credential_folder, exist_ok=True)
    gauth.SaveCredentialsFile(os.path.join(uploader_credential_folder, 'uploader_credential.json'))
