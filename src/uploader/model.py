# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

# general modules
import os
from enum import Enum
# project modules
import uploader.model_base as model_base
from uploader.database_dao import DBFileManager, UploaderDAO
from tools.yaml_tools import YamlConfig
from tools.logging_tools import logger

class CallbackTypes(Enum):
    TIMER = 0

# The states of the gdrive uploader
class SystemStates(Enum):
    READY = 0
    UPLOADING = 1
    ERROR = -1

# The global variables to be imported by other modules
CALLBACK_MANAGER = model_base.CallbackManager()
CONFIG:YamlConfig = YamlConfig(os.path.join(os.path.dirname(__file__), '../../config/uploader_config.yaml'))
DBFM = DBFileManager()
DAO = UploaderDAO(CONFIG, DBFM.db_file)
STATE = model_base.StateManager(SystemStates.READY)



