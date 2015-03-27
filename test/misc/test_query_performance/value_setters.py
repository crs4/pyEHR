from random import randint, uniform
import sys

SET_INT_VALUE = int
SET_FLOAT_VALUE = float
SET_STRING_VALUE = str
SET_RANDOM_INT = randint
SET_RANDOM_FLOAT = uniform


def set_value(func, *args):
    funcs_map = {
        'SET_INT_VALUE': SET_INT_VALUE,
        'SET_FLOAT_VALUE': SET_FLOAT_VALUE,
        'SET_STRING_VALUE': SET_STRING_VALUE,
        'SET_RANDOM_INT': SET_RANDOM_INT,
        'SET_RANDOM_FLOAT': SET_RANDOM_FLOAT
    }
    if isinstance(func, str):
        try:
            return funcs_map[func](*args)
        except KeyError, ke:
            sys.exit("No function mapped for string '%s'" % ke)
    else:
        return func(*args)
