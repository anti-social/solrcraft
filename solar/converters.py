from __future__ import unicode_literals

import datetime

from .pysolr import DATETIME_REGEX


def int_to_python(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        raise ValueError("Cannot convert {!r} to int".format(value))


def float_to_python(value):
    if value is None:
        return value
    return float(value)


def bool_to_python(value):
    if value == 'true':
        return True
    if value == 'false':
        return False
    raise ValueError("Cannot convert {!r} to boolean".format(value))


def datetime_to_python(value):
    m = DATETIME_REGEX.match(value)
    if not m:
        raise ValueError("Cannot convert {!r} to datetime".format(value))
    return datetime.datetime(*map(int, filter(None, m.groups())))
