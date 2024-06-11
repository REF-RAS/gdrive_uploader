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

# import libraries
import sys, os, signal, time, threading, webbrowser, subprocess, traceback, croniter
from datetime import datetime
# ros modules
import rospy, message_filters, actionlib, rospkg
# project modules
from tools.yaml_tools import YamlConfig
from tools.logging_tools import logger
import tools.file_tools as file_tools
import uploader.model as model
from uploader.model import DAO, CONFIG, UploaderDAO
from uploader.web.dashapp_top import DashAppTop
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEventHandler, FileSystemEvent

# The custom class for handling events of the watchdog modeul
class CustomFileSystemEventHandler(FileSystemEventHandler):
    def __init__(self, filestore_path):
        super(CustomFileSystemEventHandler, self,).__init__()
        self.filestore_path = filestore_path
        self.remote_filestore_path = CONFIG.get('uploader.filestore.remote', '/')
        self.uploader_delay = CONFIG.get('uploader.delay', 30)
        # setup ignore list
        self.ignore_suffix = CONFIG.get('uploader.ignore.suffix', None)
        if self.ignore_suffix is not None and type(self.ignore_suffix) == str:
            self.ignore_suffix = [self.ignore_suffix]
        self.ignore_prefix = CONFIG.get('uploader.ignore.prefix', None)
        if self.ignore_prefix is not None and type(self.ignore_prefix) == str:
            self.ignore_prefix = [self.ignore_prefix]  
            
    def is_in_ignore_lists(self, filename:str) -> bool:
        if self.ignore_suffix is not None and type(self.ignore_suffix) in (tuple, list):
            for prefix in self.ignore_suffix:
                if filename.endswith(prefix):
                    return True
        if self.ignore_prefix is not None and type(self.ignore_prefix) in (tuple, list):
            for prefix in self.ignore_prefix:
                if filename.startswith(prefix):
                    return True        
        return False
    # The callback function when a create event occurs
    def on_created(self, event:FileSystemEvent):
        pass
    # the callback function when a file is modified
    def on_modified(self, event:FileSystemEvent):
        if not event.is_directory:
            # if the event is from a modified file
            local_path = event.src_path
            filename = file_tools.get_filename(local_path)
            if self.is_in_ignore_lists(filename):
                return
            parent_path = file_tools.get_parent(local_path)
            sub_path = parent_path[len(self.filestore_path) + 1:]  # the sub_path cannot start with '/' for os.path.join to work
            remote_path = os.path.join(self.remote_filestore_path, sub_path)
            DAO.add_upload_file(local_path, filename, remote_path, int(time.time()) + CONFIG.get('uploader.delay', self.uploader_delay))
    # the callback function when a file is deleted
    def on_deleted(self, event:FileSystemEvent):
        if not event.is_directory:
            # if the event is from a file
            local_path = event.src_path
            DAO.remove_upload_file(local_path)

class GDriveUploader(object):
    def __init__(self):
        # the configuration and other system parameters
        self.uploader_config:YamlConfig = CONFIG
        self.settings_file_path = os.path.join(os.path.dirname(__file__), '../../config/settings.yaml')
        self.uploader_credential_folder = os.path.join(os.path.expanduser('~'), '.config/gdrive_uploader')
        self.credential_file_path = os.path.join(self.uploader_credential_folder, 'uploader_credential.json')
        # the parameters concerning the uploading operation
        self.uploader_delay = CONFIG.get('uploader.delay', 30)
        self.uploader_max_error_count = CONFIG.get('uploader.error_count.max', 5)
        # setup google drive proxy object
        self.gdrive = self.setup_gdrive_auth()
        if self.gdrive is None:
            logger.error(f'GDriveUploader: The application is unable to upload files to a Google Drive account due to a credential problem.')
            logger.warning(f'GDriveUploader: Ensure the client_id and client_secret for this Google application are set in "config/settings.yaml".')
            logger.warning(f'GDriveUploader: If you have not registered a Google Drive account, execute "register_gdrive.py" of this package to do so.')
            logger.warning(f'GDriveUploader: Terminate the application and fix the problem according to the README.md file.')
        # cache google drive folder id
        self.folder_id_cache = {}
        # prepare operation mode
        self.operation_mode = rospy.get_param('mode')
        if self.operation_mode is None or self.operation_mode == "":
            self.operation_mode = CONFIG.get('uploader.mode', 'web')
        # create lock for synchronization
        self.state_lock = threading.Lock()
        self.log_lock = threading.Lock()
        # create the stop signal handler
        signal.signal(signal.SIGINT, self.stop)
        rospy.on_shutdown(self.cb_shutdown)
        # setup watchdog
        self.filestore_local = CONFIG.get('uploader.filestore.local', None)
        self.filestore_remote = CONFIG.get('uploader.filestore.remote', '/')
        if self.filestore_local is None:
            logger.warning(f'{type(self).__name__}: The local filestore location is not defined in the config file')
            sys.exit(1)
        self.watchdog_thread = self.run_watchdog(self.filestore_local)
        # start uploader
        self._to_stop_uploader = False
        self.uploader_thread = threading.Thread(target=self.run_uploader)
        self.uploader_thread.start()
        logger.info(f'{type(self).__name__}: Started one-way sync from the local "{self.filestore_local}" to the folder "{self.filestore_remote}" on Google Drive.')

        if self.operation_mode == 'web':
            # create the dash application and start it and block the thread
            self.dash_app_operator = DashAppTop()
            self.dash_app_operator.start()
        elif self.operation_mode == 'headless':
            pass
        else:
            logger.warning(f'{type(self).__name__} (__init__): invalid uploader.mode ("{self.operation_mode}") in config')
            sys.exit(0)
        # spin the application if the mode is headless
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.watchdog_thread.stop()
            self.uploader_thread_stop()
        self.watchdog_thread.join()
        self.uploader_thread.join()

    # callback when interrupt signal is received
    def stop(self, *args, **kwargs):
        logger.info(f'GDriveUploader: ros node is being stopped')
        self.watchdog_thread.stop()
        self.uploader_thread_stop()
        sys.exit(0)

    # callback when the ros receives a shutdown signal
    def cb_shutdown(self):
        logger.info(f'GDriveUploader: ros node is being shutdown')

    # callback from the GUI console
    def _console_callback(self, event, *args):
        with self.state_lock:
            pass

    # an internal function for loading the settings and credential files
    def setup_gdrive_auth(self):
        try:
            gauth = GoogleAuth(settings_file=self.settings_file_path)
            gauth.LoadCredentialsFile(credentials_file=self.credential_file_path)
            gdrive = GoogleDrive(gauth) # Create GoogleDrive instance with authenticated GoogleAuth instance
            return gdrive
        except Exception as e:
            logger.warning(f'{type(self).__name__}: Failed to load credentials for connecting to a Google Drive account.')
            # logger.info(f'Reason: {traceback.format_exc()}')
            return None
        
    # configure and execute the watchdog to monitor the filestore
    def run_watchdog(self, filestore_path):
        event_handler = CustomFileSystemEventHandler(filestore_path)
        observer = Observer()
        observer.schedule(event_handler, filestore_path, recursive=True)
        observer.start()
        return observer

    # an internal function for stopping the uploader thread
    def uploader_thread_stop(self):
        self._to_stop_uploader = True
    
    # executes the uploader thread
    def run_uploader(self):
        # clear the upload queue
        DAO.clear_upload_queue()        
        while not self._to_stop_uploader:
            time.sleep(1.0)
            # if the google drive connection is not ready, change the state to ERROR
            if self.gdrive is None:
                model.STATE.update(model.SystemStates.ERROR)
                continue
            # check if there is a file to be uploaded
            next_to_upload = DAO.query_next_upload_within_error(limit=1, max_error_count=self.uploader_max_error_count)
            if len(next_to_upload) == 0:
                continue
            # starts the uploading procedure
            model.STATE.update(model.SystemStates.UPLOADING)
            # compute the parameters for uploading including the local and remote path
            model.STATE.set_var('upload', next_to_upload[0])
            local_path = next_to_upload[0]['local_path']
            remote_path = next_to_upload[0]['remote_path']
            file_size = (os.stat(local_path).st_size)
            start_time = time.time()
            logger.info(f'{type(self).__name__} (run_uploader): Uploading file {local_path} to {remote_path}')
            # check if the folder_id of the remote path is cached in a previous operation
            if remote_path in self.folder_id_cache:
                folder_id = self.folder_id_cache.get(remote_path)
            else:
                folder_id = self.gdrive_traverse_remote_path(remote_path)
            if folder_id is not None:
                # given the folder_id in the Google Drive, upload the file
                try:
                    # check if the file already exists in the foldef of the drive
                    filename = next_to_upload[0]['filename']
                    file_id = self.gdrive_retrieve_file_id(folder_id, filename)
                    # either create a new one or overwrite the existing one
                    if file_id is None:
                        new_file = self.gdrive.CreateFile({'title': filename, 'parents': [{'id': folder_id}], })
                    else:
                        new_file = self.gdrive.CreateFile({'title': filename, 'parents': [{'id': folder_id}], 'id': file_id})
                    new_file.SetContentFile(local_path) 
                    new_file.Upload()
                    # compute the upload time
                    upload_duration = int((time.time() - start_time) * 1000)
                    # update the database
                    DAO.remove_upload_file(local_path)
                    DAO.add_upload_record(local_path, file_size, upload_duration)
                    logger.info(f'{type(self).__name__} (run_uploader): Uploading file SUCCESSFUL (rate: {file_size / upload_duration / 1000} MB/s)')
                    model.STATE.update(model.SystemStates.READY)
                    continue
                except Exception as e:
                    logger.warning(f'{type(self).__name__} (run_uploader): Error: {traceback.format_exc()}')
            # handle upload errors
            DAO.error_delay_upload_timestamp(local_path, int(time.time()) + (self.uploader_delay * (next_to_upload[0]['error_count'] + 2)))
            logger.info(f'{type(self).__name__} (run_uploader): Uploading file FAILED')
            if next_to_upload[0]['error_count'] >= self.uploader_max_error_count:
                logger.info(f'{type(self).__name__} (run_uploader): Maximum error reached for uploading {local_path}')
            model.STATE.update(model.SystemStates.READY)

    # internal function for retrieve the file_id given the filename and the remote folder
    def gdrive_retrieve_file_id(self, folder_id, filename):
        q = f" '{folder_id}' in parents and title='{filename}' and trashed=false" 
        files = self.gdrive.ListFile({'q': q}).GetList()
        if not files or len(files) == 0:
            return None
        return files[0]['id']

    # internal function for traversing the remote path, and if needed, create the folders
    def gdrive_traverse_remote_path(self, remote_path=None):
        """ Return the google file_id for the folder, or None if the remote_path is None

        :param remote_path: The rmeote path on Google Drive, assume to start with '/'
        """
        # ff the path is None or empty string, and return None to assume the root folder
        if remote_path is None or remote_path.strip() == '':
            return None
        # to divide the path into parts
        folders = remote_path.split('/')
        # if the path is '/' return None to indicate the root folder 
        if len(folders) <= 1:
            return None
        # iterate through the parts in the path, and create folders along the way if needed
        folder_id = None
        path = ''
        for folder in folders[1:]:
            # use different query string if the current part is the root or non-root
            if folder_id is None:
                q = f" title='{folder}' and mimeType='application/vnd.google-apps.folder' and trashed=false"            
            else:
                q = f"'{folder_id}' in parents and title='{folder}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            path = '/' + folder
            files = self.gdrive.ListFile({'q': q}).GetList()
            if not files:
                # create the folder if not already exists
                if folder_id is None:
                    new_folder = self.gdrive.CreateFile({'title': folder, 'mimeType': 'application/vnd.google-apps.folder'})
                else:
                    new_folder = self.gdrive.CreateFile({'title': folder, 'parents': [{'id': folder_id}], 'mimeType': 'application/vnd.google-apps.folder'})
                new_folder.Upload()
                folder_id = new_folder['id']
            else:
                folder_id = files[0]['id']
            self.folder_id_cache[path] = folder_id
        return folder_id

# ---------------------------------------------------------
# The main program for running the application
if __name__ == '__main__':
    rospy.init_node('gdrive_uploader_agent')
    the_agent = GDriveUploader()
    DASH_HOST = CONFIG.get('uploader.web.host')
    DASH_PORT = CONFIG.get('uploader.web.host')
    if CONFIG.get('uploader.web.launch_browser', False):
        URL = f'http://{DASH_HOST}:{DASH_PORT}'
        webbrowser.open(URL)

