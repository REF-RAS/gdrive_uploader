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
import  threading, time


# --------------------------------------------
# The class models callbacks and manages
# the listener registration and calls
class CallbackManager():
    """ The class models callbacks and manages the listener registration and calls
    """
    def __init__(self):
        self.callbacks = dict()

    def set_listener(self, event, listener) -> None:
        """ Set a function to be the listener for an event, which can be any immutable object

        :param event: The event of any immutable type such as string
        :param listener: The function to be called 
        """
        self.callbacks[event] = listener

    def fire_event(self, event, *args) -> None:
        """ Invoke the listeners of the event

        :param event: The event of any immutable type such as string
        """
        if event in self.callbacks:
            self.callbacks[event](event, *args)

# --------------------------------------------
# The class models and manages the state
# for a state transition machine driven
# application
class StateManager():
    """ The class models and manages the state for a state transition machine driven application
    """
    def __init__(self, initial_state=None):   
        self.state_lock = threading.RLock()
        self.state = initial_state
        self.info = None
        self.last_update_time = None
        self.last_change_time = None
        # variables for the state
        self.vars = dict()
        self.vars_lock = threading.RLock()

    def get(self):
        """ Returns the current state
        """
        self.state_lock.acquire()
        try:
            return self.state
        finally:
            self.state_lock.release()

    def get_state(self):
        """ A wrapper function for :func:`<model_base.StateManager.get>`
        """
        return self.get()

    def get_with_info(self) -> tuple:
        """ Returns the current state and attached info as a tuple
        """
        self.state_lock.acquire()
        try:
            return (self.state, self.info,)
        finally:
            self.state_lock.release()      

    def update(self, new_state, info=None):
        """ Change the state to a new state, with an option to attach an info object

        :param new_state: The new state
        :param info: The new info object, defaults to None
        """
        self.state_lock.acquire()
        try:
            if self.state != new_state:
                self.last_change_time = time.time()
            self.state = new_state
            self.info = info
            self.last_update_time = time.time()
        finally:
            self.state_lock.release()

    def update_state(self, new_state, info=None):
        """ A wrapper function for :func:`<model_base.StateManager.update>`
        """        
        self.update(new_state, info)

    def is_state(self, query_state) -> bool:
        """ Returns True if the current state is the query_state

        :param query_state: The query state for matching
        :return: True if the current state is the query_state
        """
        self.state_lock.acquire()
        try:
            return self.state == query_state
        finally:
            self.state_lock.release()

    def time_lapsed_since_update(self) -> float:
        """ Returns the time since the last state update even if the state has remained unchaged

        :return: The time in seconds as a float
        """
        if self.last_update_time is None:
            return None
        return time.time() - self.last_update_time
    
    def time_lapsed_since_change(self):
        """ Returns the time since the last state change

        :return: The time in seconds as a float
        """
        if self.last_change_time is None:
            return None
        return time.time() - self.last_change_time    
    
    # -- passing variables between states
    def set_var(self, name, value):
        """ Set a custom name value pair to the state manager 

        :param name: The name of the custom variable
        :param value: The value of the custom variable
        """
        self.vars_lock.acquire()
        try:
            self.vars[name] = value
        finally:
            self.vars_lock.release()

    def get_var(self, name, default=None):
        """ Retrieve the value given the name of the variable

        :param name: The name of the custom variable
        :param default: The default value, defaults to None
        :return: The value of the stored variable or the default value if the variable name is non-existent
        """
        self.vars_lock.acquire()
        try:
            if name in self.vars:
                return self.vars[name]
            return default
        finally:
            self.vars_lock.release()
            
    def del_var(self, name) -> None:
        """ Remove the variable from the state manager

        :param name: The name of the custom variable
        """
        self.vars_lock.acquire()
        try:
            if name in self.vars:
                del self.vars[name]      
        finally:
            self.vars_lock.release()

    # The internal function for printing the state of the manager
    def __str__(self):
        return self.state
