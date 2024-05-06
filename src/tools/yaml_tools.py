# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

import yaml, os, numbers
from collections import namedtuple, defaultdict

class YamlConfig():
    """ The class providing easy query of the hierarcy of configurations in yaml
    """
    def __init__(self, config_file:str):
        """the constructor
        :param scene_config_file: the path to the yaml configuration file
        :type scene_config_file: str, optional
        """
        # load data from the config yaml file
        if config_file is None:
            raise AssertionError(f'{__class__.__name__} parameter (config_file) is None')
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)

    # -------------------------------------------
    # model functions

    # returns the value of a named config given as a dot-separated path 
    def query_config(self, name:str, default=None):
        """ returns the value of any named config given as a dot-separated path
        :param name: the dot-separated path
        :type name: str
        :param default: the default value if nothing is found in the path, defaults to None
        :type default: any
        :return: the value of the config key
        :rtype: any
        """
        if name is None:
            return default
        name_as_list = name.split('.')
        pointer = self.config
        # iterate the parts of the config name
        for item in name_as_list:
            if item.isnumeric():
                item = int(item)
                if item < 0 or item >= len(pointer):
                    print(f'query_config: part of the name contains an invalid index {item}')
                    # raise AssertionError(f'query_config: Non-existent config name "{name}", which contains an invalid index {item}')
                    return default
                pointer = pointer[item]
            else:
                if item not in pointer:
                    print(f'query_config: part of the name contains an invalid key {item}')
                    # raise AssertionError(f'query_config: Non-existent the config name "{name}", which contains an invalid key {item}')
                    return default
                pointer = pointer[item]
        return pointer
    
    def get(self, name:str, default=None):
        """ Returns the value given the origin key name at the root level 

        :param name: The key name at the root level
        :param default: The default value, defaults to None
        :return: The value of the config key
        """
        return self.config.get(name, default)