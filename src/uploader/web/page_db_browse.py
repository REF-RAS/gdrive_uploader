# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

import plotly.express as px
import plotly.graph_objects as go
from enum import Enum
import dash
from dash import html, dcc, callback, Input, Output, dash_table
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate 
# project modules
from tools import db_tools, type_tools
from tools.logging_tools import logger
from uploader.model import DAO

dash.register_page(__name__)

class DBTableBrowsePage():
    def __init__(self, app):
        self.app = app  
        self._define_page()

    def get_tablenames(self):
        return db_tools.list_table_names(DAO.db_file)    

    def layout(self):
        return self._layout

    def _define_page(self):
        self.tablenames = self.get_tablenames()
        if 'accounts' in self.tablenames:
            self.tablenames.remove('accounts') 
        self._datatable = dash_table.DataTable(id='db_display_table', page_current=0, page_size=100, page_action='custom')
        rows = html.Div(children = [
            dbc.Row(html.H4(id = 'title', children = 'Browse DB Tables', className='mt-3 mb-3')),
            dbc.Row([dbc.Col(dcc.Dropdown(self.tablenames, 'tile', id='db_display_table_dropdown', className='col-12 d-inline-block')
                             , className='col-4'),
                    dbc.Col(dbc.Button('Refresh', id='db_display_table_refresh', n_clicks=0))]),
            dbc.Row([html.Div(html.H6(dbc.Badge('TABLE CONTENT', color='white', text_color='primary'))),
                html.Div(self._datatable)
            ], id='db_display_table_div', style={'display': 'none'})
        ])
        self._layout = dbc.Container(rows, fluid=True) 
        # - define callback
        self.app.callback(Output('db_display_table', 'data'),
                          Output('db_display_table_div', 'style'),
                            [
                            Input('db_display_table_refresh', 'n_clicks'),
                            Input('db_display_table_dropdown', 'value'),
                            Input('db_display_table', "page_current"),
                            Input('db_display_table', "page_size")
                            ], prevent_initial_call=True)(self._update_table())
    
    def _update_table(self):
        def update_table(n_clicks, tablename, page_current, page_size):
            if not tablename:
                raise PreventUpdate
            df = db_tools.dump_table_df(DAO.db_file, tablename, page_size, page_current * page_size)
            if 'timestamp' in df.columns:
                df['timestamp'] = df['timestamp'].apply(type_tools.timestamp_to_datestr)
            return (df.to_dict('records'), {'display': 'block'},)
        return update_table
