from __future__ import unicode_literals

import inspect
import datetime

from .pysolr import DATETIME_REGEX


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

    def process_result_value(self, value):
        return self.to_python(value)

    def process_param_value(self, value):
        return self.to_python(value)


class Integer(Type):
    def to_python(self, value):
        return int(value)


class Float(Type):
    def to_python(self, value):
        return float(value)


class Boolean(Type):
    def to_python(self, value):
        if value == 'true':
            return True
        elif value == 'false':
            return False
        raise ValueError("Cannot convert {!r} to boolean".format(value))


class DateTime(Type):
    def to_python(self, value):
        m = DATETIME_REGEX.match(value)
        if not m:
            raise ValueError("Cannot convert {!r} to datetime".format(value))
        return datetime.datetime(*map(int, filter(None, m.groups())))
