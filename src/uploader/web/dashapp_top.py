# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

# Gunicorn deployment
# gunicorn graph:app.server -b :8000

# import libraries
import sys, os, signal, io, time, traceback
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
from dash_auth import BasicAuth
from dash.dependencies import Input, Output, State
from flask import Flask
# ros modules
import rospy, message_filters
# project modules
from tools.yaml_tools import YamlConfig
from tools.logging_tools import logger
import tools.hash_tools as hash_tools
import uploader.model as model
from uploader.model import CONFIG, DAO

SERVER = Flask(__name__)
APP = dash.Dash(__name__, 
                external_stylesheets=[dbc.themes.SPACELAB],  
                server=SERVER,
                meta_tags=[{"name": "viewport", "content": "width=device-width"}],
                suppress_callback_exceptions=True)

APP.config['suppress_callback_exceptions'] = True
_APP_NAME = 'Google Drive Uploader'

# -- load the pages for the dash application
from . import page_console, page_db_browse

# -- Create a dash application object
class DashAppTop():
    def __init__(self):
        global APP
        self.DASH_HOST = CONFIG.get('uploader.web.host')
        self.DASH_PORT = CONFIG.get('uploader.web.port')
        self.SYSTEM_TIMER = CONFIG.get('uploader.system.timer', 1) * 1000

        self.app = APP
        self.flask_server = self.app.server
        self.dash_thread = None
        # - define the pages
        self._console_page = page_console.ConsolePage(self.app, CONFIG.get('uploader.console.refresh', 5))
        self._db_browse_page = page_db_browse.DBTableBrowsePage(self.app)      
        # - initialize the application
        self._define_app()

    def _define_app(self):
        self._navbar_with_menu = dbc.NavbarSimple(
                    children=[
                        dbc.NavItem(dbc.NavLink('Console', href='/page_console')),  
                        # dbc.NavItem(dbc.NavLink('DB', href='/page_db_browser')),                                              
                    ],
                    brand=html.Div([html.H3(_APP_NAME), html.H6('Robotics and Autonomous Systems Group, REF, RI, Queensland University of Technology')]),
                    brand_href='/page_console', color='#ffe6cc', className='fs-4 text')

        self._navbar_simple = dbc.NavbarSimple(
                    brand=html.H3(_APP_NAME), color='#99cccc', className='fs-4 text')      

        self._nav_placeholder = html.Div(id='nav_placeholder')
        
        self.app.layout = html.Div([ 
            # for the dash system timer
            dcc.Store(id='dashapp_interval_store'),
            dcc.Interval(id='system_interval', interval=self.SYSTEM_TIMER, n_intervals=0),
            dcc.Location(id='url', refresh=False),
            self._nav_placeholder, 
            html.Div(id='page_content', children=[]), 
        ])
        # ----- the dash callbacks
        self.app.callback([Output('page_content', 'children'),
                           Output('nav_placeholder', 'children')],
              [Input('url', 'pathname')])(self._display_page())
        
        self.app.callback([Output('dashapp_interval_store', 'data')],
              [Input('system_interval', 'n_intervals')],
              [State('url', 'pathname')])(self._dash_system_timer())
               
    def start(self):
        rospy.loginfo('operation agent: starting the dash flask server')
        # self.app.run_server(host=self.DASH_HOST, port=self.DASH_PORT, debug=CONFIG.get('uploader.web.debug.mode', True))
        self.app.run_server(host=self.DASH_HOST, port=self.DASH_PORT, debug=False)
        
    def stop(self, *args, **kwargs):
        rospy.loginfo('operation agent: the dash flask server is being shutdown')
        time.sleep(2)
        sys.exit(0)

    # -- flask dash callback for url handling
    def _display_page(self):
        def display_page(pathname):
            page_content = ''
            nav_content = self._navbar_with_menu
            try: 
                if pathname == '/page_console':
                    page_content = self._console_page.layout()          
                # elif pathname == '/page_db_browser':
                #     page_content = self._db_browse_page.layout()                                        
                elif pathname == '/': 
                    page_content = self._console_page.layout()
                else: # if redirected to unknown link
                    page_content = '404 Page Error! Please choose a link'
            except Exception as e:
                logger.error(e)
            return (page_content, nav_content,)
        return display_page
    
    # -- dash callback for system interval timer
    def _dash_system_timer(self):
        def dash_system_timer(n, pathname):
            model.CALLBACK_MANAGER.fire_event(model.CallbackTypes.TIMER)
            return (n,)
        return dash_system_timer
    
    @SERVER.route("/<path>")
    def update_title(path):
        APP.title = _APP_NAME
        return APP.index()
        
    