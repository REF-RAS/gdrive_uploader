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
from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np 
from tools.logging_tools import logger


def timestamp_to_datestr(x:float) -> str:
    """ Return the date string representation 2024-May-01 00:00:00 from a timestamp

    :param x: _description_
    :return: _description_
    """
    if (np.all(pd.notnull(x))):
        datestr = datetime.fromtimestamp(x).strftime('%Y-%b-%d %H:%M:%S')
        return datestr
    return x

# List of acceptable formats recognized by datetime_to_timestamp
ACCEPTABLE_FORMAT = [
    '%Y/%m/%d %H:%M:%S',
    '%Y-%m-%d %H:%M:%S',
    '%m/%d/%Y %H:%M:%S',
]

def datetime_to_timestamp(the_date, the_time) -> float:
    """ Return the timestamp in seconds from the epoch (a float) from a string representation of datetime

    :param the_date: The date as a string
    :param the_time: The time as a string
    :return: The timestamp
    """
    # returns null if the parameter is one of the null values in pandas or None
    if pd.isnull(the_date) or pd.isnull(the_time) or the_date is None or the_time is None:
        return None
    # converts to string if the date is of pandas timestamp type
    if type(the_date) == pd.Timestamp:
        the_date = str(the_date).split(' ')
        the_date = the_date[0]
    # combine the date and the time as a string
    the_time = str(the_time) 
    the_datetime = f'{the_date} {the_time}' 
    datetime_obj = None
    # iterate through the acceptable formats and try each until one is a successful matchS.
    for format in ACCEPTABLE_FORMAT:
        try:
            datetime_obj = datetime.strptime(the_datetime, format) 
            break
        except Exception as e:
            pass
    else:
        return None
    result = datetime.timestamp(datetime_obj)
    return result