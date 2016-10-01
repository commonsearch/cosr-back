from __future__ import absolute_import, division, print_function, unicode_literals

import traceback

#
# Use this file for utility Python functions that are independent from the rest of cosrlib
#


def ignore_exceptions(default):
    """ Wraps a function and catches any exceptions it may raise """
    def outer(fn):
        def wrapped(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception:  # pylint: disable=broad-except
                print("Caught Python exception!")
                traceback.print_exc()
                return default
        return wrapped
    return outer


def ignore_exceptions_generator(fn):
    """ Wraps a generator and catches any exceptions it may raise """
    def wrapped(*args, **kwargs):
        try:
            for x in fn(*args, **kwargs):
                yield x
        except Exception:  # pylint: disable=broad-except
            print("Caught Python exception in generator!")
            traceback.print_exc()
    return wrapped
