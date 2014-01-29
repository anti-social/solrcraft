from __future__ import unicode_literals

import inspect
import datetime

from .pysolr import DATETIME_REGEX
from .compat import force_unicode


def instantiate(typeobj, *args, **kwargs):
    if inspect.isclass(typeobj):
        return typeobj(*args, **kwargs)
    return typeobj


def get_to_python(typeobj):
    if hasattr(typeobj, 'to_python'):
        return typeobj.to_python
    return lambda v: v

            
class Type(object):
    def to_python(self, value):
        raise NotImplementedError()


class String(Type):
    def to_python(self, value):
        if value is None:
            return None
        return force_unicode(value)


class Integer(Type):
    MIN_VALUE = -(1 << 31)
    MAX_VALUE = (1 << 31) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Long(Type):
    MIN_VALUE = -(1 << 63)
    MAX_VALUE = (1 << 63) - 1

    def to_python(self, value):
        if value is None:
            return None
        return int(value)


class Float(Type):
    def __init__(self, precision=None):
        self.precision = precision
        
    def to_python(self, value):
        if value is None:
            return None
        if self.precision is not None:
            return round(float(value), self.precision)
        return float(value)


class Boolean(Type):
    def to_python(self, value):
        if value is None:
            return None
        if value is True or value == 'true':
            return True
        elif value is False or value == 'false':
            return False
        raise ValueError("Cannot convert {!r} to boolean".format(value))


class DateTime(Type):
    def to_python(self, value):
        if value is None:
            return None
        m = DATETIME_REGEX.match(value)
        if not m:
            raise ValueError("Cannot convert {!r} to datetime".format(value))
        return datetime.datetime(*map(int, filter(None, m.groups())))


class Text(Type):
    def to_python(self, value):
        if value is None:
            return None
        return force_unicode(value)
