# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

import yaml, os, io, numbers
import pandas as pd
import requests

# The class as the proxy for accessing Google Drive publicly shared linked file
class GoogleDriveProxy():
    @staticmethod
    def download_file_with_fileid(file_id, destination=None) -> bytes:
        """ Download a Google Drive publicly shared file given the Drive file_id to the local destination and the return value

        :param file_id: The Google Drive file_id
        :param destination: The local path as the destination of the downloaded file, defaults to None
        :return: The bytearray representing the downloaded file
        """
        try:
            URL = "https://docs.google.com/uc?export=download"
            session = requests.Session()
            response = session.get(URL, params = { 'id' : file_id }, stream = True)
            token = GoogleDriveProxy.get_confirm_token(response)
            if token:
                params = { 'id' : file_id, 'confirm' : token }
                response = session.get(URL, params = params, stream = True)
            if destination is not None:
                GoogleDriveProxy.save_to_file(response, destination)  
            return GoogleDriveProxy.save_to_bytearray(response)
        except Exception as e:
            pass
        return None
    
    # Internal function
    @staticmethod
    def get_confirm_token(response):
        for key, value in response.cookies.items():
            if key.startswith('download_warning'):
                return value
        return None
    
    # Internal function
    @staticmethod
    def save_to_file(response, out_file=None):
        CHUNK_SIZE = 32768
        with open(out_file, "wb") as f:
            for chunk in response.iter_content(CHUNK_SIZE):
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
    
    # Internal function
    @staticmethod
    def save_to_bytearray(response) -> bytes:
        data = response.raw.read(decode_content=True)
        return data

# --------------------------------------
# The test program
if __name__ == '__main__':
    file_id = ''    # to fill in with the file_id of a publicly shared Google Drive file  
    out_file = os.path.expanduser('~/file.xlsx')
    # content = GoogleDriveProxy.download_file_with_fileid(file_id)
    # df_dict = pd.read_excel(io.BytesIO(content), sheet_name=None)
    # for k in df_dict.keys():
    #     print(k)