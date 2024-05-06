# Copyright 2024 - Andrew Kwok Fai LUI, 
# Robotics and Autonomous Systems Group, REF, RI
# and the Queensland University of Technology

__author__ = 'Andrew Lui'
__copyright__ = 'Copyright 2024'
__license__ = 'GPL'
__version__ = '1.0'
__email__ = 'ak.lui@qut.edu.au'
__status__ = 'Development'

import threading
from threading import Lock, RLock
from wrapt.wrappers import FunctionWrapper, ObjectProxy
from wrapt.decorators import decorator

# Reference: https://wrapt.readthedocs.io/en/latest/examples.html

def synchronized(wrapped):
    def _synchronized_lock(context):
        # Attempt to retrieve the lock for the specific context.

        lock = vars(context).get('_synchronized_lock', None)

        if lock is None:
            # There is no existing lock defined for the context we
            # are dealing with so we need to create one. This needs
            # to be done in a way to guarantee there is only one
            # created, even if multiple threads try and create it at
            # the same time. We can't always use the setdefault()
            # method on the __dict__ for the context. This is the
            # case where the context is a class, as __dict__ is
            # actually a dictproxy. What we therefore do is use a
            # meta lock on this wrapper itself, to control the
            # creation and assignment of the lock attribute against
            # the context.

            meta_lock = vars(synchronized).setdefault(
                    '_synchronized_meta_lock', Lock())

            with meta_lock:
                # We need to check again for whether the lock we want
                # exists in case two threads were trying to create it
                # at the same time and were competing to create the
                # meta lock.

                lock = vars(context).get('_synchronized_lock', None)

                if lock is None:
                    lock = RLock()
                    setattr(context, '_synchronized_lock', lock)

        return lock

    def _synchronized_wrapper(wrapped, instance, args, kwargs):
        # Execute the wrapped function while the lock for the
        # desired context is held. If instance is None then the
        # wrapped function is used as the context.

        with _synchronized_lock(instance or wrapped):
            return wrapped(*args, **kwargs)

    class _FinalDecorator(FunctionWrapper):

        def __enter__(self):
            self._self_lock = _synchronized_lock(self.__wrapped__)
            self._self_lock.acquire()
            return self._self_lock

        def __exit__(self, *args):
            self._self_lock.release()

    return _FinalDecorator(wrapped=wrapped, wrapper=_synchronized_wrapper)

if __name__ == '__main__':
    class Class(object):
        @synchronized
        @classmethod
        def function_cm(cls):
            pass
        def function_im(self):
            with synchronized(Class):
                pass
        @synchronized
        def function_im_1(self):
            pass
        
    @synchronized # lock bound to function1
    def function1():
        pass
    @synchronized # lock bound to function2
    def function2():
        pass

    @synchronized # lock bound to Class
    class Class(object):
        @synchronized # lock bound to instance of Class
        def function_im(self):
            pass
        @synchronized # lock bound to Class
        @classmethod
        def function_cm(cls):
            pass
        @synchronized # lock bound to function_sm
        @staticmethod
        def function_sm():
            pass