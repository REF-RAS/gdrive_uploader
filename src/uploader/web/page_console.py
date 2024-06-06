# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

import base64, traceback, numbers, shutil
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from enum import Enum
import dash
from dash import html, dcc, callback, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
# project modules
from dash.exceptions import PreventUpdate
from tools import type_tools
from tools.logging_tools import logger
from uploader.model import DAO, CONFIG
import uploader.model as model
import rospy, rostopic

dash.register_page(__name__)

# -- define the GUI components of this page
class ConsolePage():
    def __init__(self, app, refresh_cycle=5):
        self.app = app 
        self.refresh_cycle = refresh_cycle
        if self.refresh_cycle is None or type(self.refresh_cycle) not in (float, int):
            self.refresh_cycle = 5
        self.worksheet_names = ['Schedule']
        self._define_page()
    
    def layout(self, validate=False):
        return self._layout

    def _define_page(self):

        self._system_status_panel = html.Div([
            html.H4(dbc.Badge('SYSTEM STATUS', className='ms-1 me-2', color='white', text_color='secondary')),
            dbc.Row(dbc.Badge(id='console_system_status_state', className='fs-5', color='white', text_color='primary'), className='mx-auto col-8'),
            dbc.Row(html.P(id='console_system_status_message', style={'text-align':'left', 'font-family':'monospace'}), className='mx-auto col-8'),
            ], className='col-10 mx-auto text-center border mt-2', style={'height': 140,})        

        # -- datatable for displaying rostopics status
        self._upload_stat_datatable = dash_table.DataTable(id='console_upload_stat_table')

        self._upload_stat_panel = html.Div([
            html.H4(dbc.Badge('UPLOAD STAT', className='ms-1 me-2', color='white', text_color='secondary')),
            dbc.Row([self._upload_stat_datatable], className='mx-auto col-12'),
            ], className='col-6 mx-auto text-center border mt-3')

        self._diskspace_datatable = dash_table.DataTable(id='console_diskspace_table')

        self._diskspace_panel = html.Div([
            html.H4(dbc.Badge('DISK SPACE', className='ms-1 me-2', color='white', text_color='secondary')),
            dbc.Row([self._diskspace_datatable], className='mx-auto col-12'),
            ], className='col-6 mx-auto text-center border mt-3')

        # -- datatable for displaying rostopics status 
        self._upload_queue_datatable = dash_table.DataTable(id='console_upload_queue_table')

        self._upload_queue_panel = html.Div([
            html.H4(dbc.Badge('NEXT FILES TO UPLOAD', className='ms-1 me-2', color='white', text_color='secondary')),
            html.P('(at most the next 100 are shown)'),
            dbc.Row([self._upload_queue_datatable], className='mx-auto col-12'),
            ], className='col-10 mx-auto text-center border mt-3')     

        # -- putting the GUI components together 
        rows = html.Div(id='scan-body',children = [
            dcc.Store(id='console_update_store'),
            dbc.Row(html.H4(id = 'title', children = 'System Status Console', className='mt-3 mb-3')),
            self._system_status_panel,
            dbc.Row([self._upload_stat_panel, self._diskspace_panel,], className='col-10 mx-auto text-center mt-3'),
            self._upload_queue_panel,
        ])
        self._layout = dbc.Container(rows, fluid=True)

        self.app.callback(Output('console_upload_stat_table', 'data'),
            [Input('console_update_store', 'data')], prevent_initial_call=False)(self._update_upload_stat_table())   
     
        self.app.callback(Output('console_diskspace_table', 'data'),
            [Input('console_update_store', 'data')], prevent_initial_call=False)(self._update_diskspace_table())  
           
        self.app.callback(Output('console_upload_queue_table', 'data'),
            [Input('console_update_store', 'data')], prevent_initial_call=False)(self._update_upload_queue_table())  
                   
        self.app.callback([Output('console_system_status_state', 'children'),
                           Output('console_system_status_message', 'children'),],
            [Input('console_update_store', 'data')], prevent_initial_call=True)(self._update_system_status_panel())  

        self.app.callback([Output('console_update_store', 'data', allow_duplicate=True)],
            [Input('dashapp_interval_store', 'data')], prevent_initial_call=True)(self._update_all())   
            

    def _define_upload_stat_model(self):
        try:
            stat = DAO.get_upload_stat()
            model = pd.DataFrame(columns=('Parameters', 'Values'))
            model.loc[1] = ['Total Files', stat['count']]
            model.loc[2] = ['Total file Size' , f'{stat["file_size"] / (1024 * 1024):.2f} MB']
            model.loc[3] = ['Upload Rate', f'{stat["file_size"] / stat["duration"] / 1024 * 1000:.2f} KB/s'] 
            return model
        except:
            # logger.error(f'Error: {traceback.format_exc()}')
            return pd.DataFrame()

    def _define_upload_queue_model(self):
        try:
            model = DAO.list_upload_queue()
            model['timestamp'] = model['timestamp'].apply(type_tools.timestamp_to_datestr)
            model.columns = ['Local Path', 'Filename', 'Remote Path', 'Scheduled Upload Time', 'Error Count']
            return model
        except:
            return pd.DataFrame()

    def _define_diskspace_model(self):
        total, used, free = shutil.disk_usage(CONFIG.get('uploader.filestore.local'))
        model = pd.DataFrame(columns=('Parameters', 'Values'))
        model.loc[1] = ['Total', f'{total // (2**30)} GB']
        model.loc[2] = ['Used', f'{used // (2**30)} GB']
        model.loc[3] = ['Free', f'{free // (2**30)} GB']        
        return model

    # -- the callback for table update
    def _update_upload_stat_table(self):
        def update_upload_stat_table(data):
            model = self._define_upload_stat_model()
            if model is None:
                raise PreventUpdate
            return model.to_dict('records')
        return update_upload_stat_table  
    
    # -- the callback for table update
    def _update_system_status_panel(self):
        def update_system_status_panel(data):
            state:model.SystemStates = model.STATE.get()
            message = ''
            if state == model.SystemStates.UPLOADING:
                upload = model.STATE.get_var('upload')
                message = f'Uploading file {upload["filename"]} at local path {upload["local_path"]} to remote path {upload["remote_path"]}'
            return (state.name, message,)
        return update_system_status_panel   
    
        # -- the callback for table diskspace
    def _update_diskspace_table(self):
        def update_diskspace_table(data):
            model = self._define_diskspace_model()
            if model is None:
                raise PreventUpdate
            return model.to_dict('records')
        return update_diskspace_table  
    
    # -- the callback for table bagfiles
    def _update_upload_queue_table(self):
        def update_upload_queue_table(data):
            model = self._define_upload_queue_model()
            if model is None:
                raise PreventUpdate
            return model.to_dict('records')
        return update_upload_queue_table  

    # -- the callback for all update
    def _update_all(self):
        def update_all(n):
            if (n-1) % self.refresh_cycle != 0:
                raise PreventUpdate
            return (1,)
        return update_all  
