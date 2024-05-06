# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

import os, shutil
from pathlib import Path

# ----- file utilities
# -- returns the filename of a path (the last part of the path)
def get_filename(path:str) -> str:
    """ Return the file name given a path

    :param path: The path of a file
    """
    return Path(path).name

# -- returns the suffix of a path or a filename
def get_suffix(path:str, include_period:bool=True) -> str:
    """ Returns the suffix of a path or a filename

    :param path: The path of a file
    :param include_period: Determine if the period is part of the suffix, defaults to True
    """
    suffix = Path(path).suffix
    if not include_period and len(suffix) > 0:
        return suffix[1:]
    return suffix

# -- replace suffix with another
def replace_suffix(path:str, new_suffix:str) -> str:
    """ Replace suffix of the file given the path with another

    :param path: The path of a file
    :param new_suffix: The new suffix of the file as a string
    """
    index = path.rfind('.')
    if index == -1:
        return f'{path}.{new_suffix}'
    return f'{path[:index]}.{new_suffix}' 

# -- returns the parent folder of a file (path)
def get_parent(path:str) -> str:
    """ Return the parent folder of a file given the path

    :param path: The path of a file
    """
    return os.path.realpath(Path(path).parent)

# -- move the file from source to dest
def move_file(source:str, dest:str):
    """ Rename a file path from the source to the destination

    :param source: The original file path
    :param dest: The new file path
    """
    Path(source).rename(dest)

# -- returns a list of files found in the folder with a suffix (if specified)
# -- the list contains full path of the files if full_path is True
def list_files(folder:str, full_path:bool=False, suffix:str=None) -> list:
    """ Returns a list of files found in the folder with a suffix (if specified) 
        the list contains full path of the files if full_path is True

    :param folder: The folder in which the files are to be listed
    :param full_path: Determine if the results contain full path, defaults to False
    :param suffix: Only include files with this suffix, defaults to None
    """
    if suffix is None:
        filelist = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
    else:
        filelist = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f)) and f.endswith(suffix)]
    if full_path:
        file_list = list(map(lambda f: os.path.join(folder, f), file_list))    
    return filelist

# -- check if child is parent or its subdirectory
def is_subdir(child:str, parent:str) -> bool:
    """ Return True if a folder is the child of a parent folder

    :param child: The child folder as a path string
    :param parent: The parent folder a path string
    """
    if os.path.isdir(child) and os.path.isdir(parent):
        child, parent = os.path.realpath(child), os.path.realpath(parent)
        return child == parent or child.startswith(parent + os.sep)
    else:
        return False

# -- prepend the parent path to the front of the list of files in file_list
def prepend_path(file_list:list, parent:str) -> list:
    """ Prepend the parent path to the front of the list of files in file_list

    :param file_list: A list of file paths
    :param parent: A path to be attached as the parent path of the file path list
    """
    file_list = map(lambda f: os.path.join(parent, f), file_list)
    return list(file_list)

# -- return (one of the) file name starting with a given string in a folder
def search_filename_startswith(folder:str, prefix:str=None):
    """ Return (one of the) file name starting with a given string in a folder

    :param folder: The folder in which the files are to be processed
    :param prefix: The target filename prefix, defaults to None
    """
    for f in os.listdir(folder):
        if prefix is None or f.startswith(prefix):
            return f
    return None

# -- return the size of a folder
def get_folder_size(folder:str) -> int:
    """ Return the size of a folder

    :param folder: The folder in which the file sizes are to be added up
    """
    total = 0
    for path, dirs, files in os.walk(folder):
        for f in files:
            fp = os.path.join(path, f)
            total += os.path.getsize(fp)
    return total

# -- remove all files and directories under a folder (the folder is not deleted)
def remove_folder_content(folder):
    """ Remove all files and directories under a folder (the folder is not deleted)

    :param folder: The folder in which all files and folders are to be deleted
    """
    for root, dirs, files in os.walk(folder):
        for f in files:
            os.unlink(os.path.join(root, f))
        for d in dirs:
            shutil.rmtree(os.path.join(root, d))
    
