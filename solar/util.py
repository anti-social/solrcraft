from __future__ import unicode_literals

import re
import urllib
import logging
from copy import deepcopy
from datetime import datetime, date

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

from .compat import PY2, text_type, string_types, binary_type, int_types, force_unicode
from .tree import Node


CALENDAR_UNITS = ['MILLI', 'MILLISECOND', 'SECOND', 'MINUTE',
                  'HOUR', 'DAY', 'MONTH', 'YEAR']
UNIT_GROUPS =  '|'.join('({}S?)'.format(unit) for unit in CALENDAR_UNITS)
SOLR_DATETIME_RE = re.compile(
    r'^NOW(/({}))?([+-]\d+({}))*$'.format(UNIT_GROUPS, UNIT_GROUPS))

SPECIAL_WORDS = ['AND', 'OR', 'NOT', 'TO']

# See: http://lucene.apache.org/core/3_6_0/queryparsersyntax.html#Escaping%20Special%20Characters
SPECIAL_CHARACTERS =  r'\+-&|!(){}[]^"~*?:'


class SafeString(binary_type):
    pass

class SafeUnicode(text_type):
    pass

ALL = SafeUnicode('*:*')


def process_special_words(value, words=None):
    words = words or SPECIAL_WORDS
    for w in words:
        value = re.sub(r'(\A|\s+)({})(\s+|\Z)'.format(w), lambda m: m.group(0).lower(), value)
    return value

def process_special_characters(value, chars=None):
    chars = chars or SPECIAL_CHARACTERS
    for c in chars:
        value = value.replace(c, r'\{}'.format(c))
    return value

def contains_special_characters(value, chars=None):
    chars = chars or SPECIAL_CHARACTERS
    return any(map(lambda c: c in value, chars))

def safe_solr_input(value):
    if isinstance(value, (SafeString, SafeUnicode)):
        return value

    if not isinstance(value, string_types):
        value = force_unicode(value)
    
    value = process_special_words(value)

    value = process_special_characters(value)

    if isinstance(value, text_type):
        return SafeUnicode(value)
    return SafeString(value)

class X(Node):
    AND = 'AND'
    OR = 'OR'
    default = AND
    
    def __init__(self, *args, **kwargs):
        op = kwargs.pop('_op', self.default).upper()
        if op not in (self.AND, self.OR):
            op = self.default
        # children = []
        # for x in args:
        #     if isinstance(x, X) and len(x.children) == 1:
        #         children.extend(x.children)
        #     else:
        #         children.append(x)
        # children.extend(kwargs.items())
        # if len(children) == 1 and isinstance(children[0], X):
        #     op = children[0].connector
        #     children = children[0].children
        # super(X, self).__init__(children=children, connector=op)
        super(X, self).__init__(children=list(args) + list(kwargs.items()),
                                connector=op)

    def _combine(self, other, conn):
        if not isinstance(other, X):
            raise TypeError(other)
        if self.children and other.children:
            obj = type(self)()
            obj.add(self, conn)
            obj.add(other, conn)
        elif self.children:
            obj = deepcopy(self)
        else:
            obj = deepcopy(other)
        return obj

    def __or__(self, other):
        return self._combine(other, self.OR)

    def __and__(self, other):
        return self._combine(other, self.AND)

    def __invert__(self):
        obj = type(self)()
        obj.add(self, self.AND)
        obj.negate()
        return obj

X_ALL = X(ALL)

class LocalParams(OrderedDict):
    SPECIAL_CHARACTERS = " '" + SPECIAL_CHARACTERS
    
    def update(self, other=None, **kwargs):
        if other is None:
            other = OrderedDict()
        elif isinstance(other, LocalParams):
            other = OrderedDict(other)
        elif hasattr(other, 'keys'):
            other = OrderedDict((k, other[k]) for k in sorted(other.keys()))
        elif isinstance(other, (list, tuple)):
            _other = []
            for p in other:
                if isinstance(p, (list, tuple)):
                    _other.append(p)
                else:
                    _other.append(('type', p))
            other = OrderedDict(_other)
        else:
            other = OrderedDict(type=other)
        
        other.update(sorted(kwargs.items(), key=lambda p: p[0]))
        for k, v in other.items():
            self.add(k, v)
    # replace OrderedDict.__update to fix lp.update(['dismax'])
    _OrderedDict__update = update
    
    def add(self, key, value=None):
        if value is None:
            if isinstance(key, (list, tuple)):
                key, value = key
            else:
                key, value = 'type', key
        
        if contains_special_characters(key, self.SPECIAL_CHARACTERS):
            raise ValueError("Key '{}' contains special characters".format(key))
        if key == 'type' and contains_special_characters(value, self.SPECIAL_CHARACTERS):
            raise ValueError("Type value '{}' contains special characters".format(value))
        
        self[key] = value

    def _quote(self, value, replace_words=True):
        value = force_unicode(value)
        if replace_words:
            value = process_special_words(value)
        if contains_special_characters(value, self.SPECIAL_CHARACTERS):
            return "'{}'".format(value.replace("'", "\\\\'").replace('"', '\\"'))
        return value
    
    def __str__(self):
        if not self:
            return ''

        parts = []
        for key, value in self.items():
            if key == 'type':
                parts.insert(0, value)
            else:
                replace_words = True
                if isinstance(value, X):
                    value = make_fq(value)
                    replace_words = False
                parts.append(
                    '{}={}'.format(
                        key,
                        self._quote(process_value(value, safe=True),
                                    replace_words=replace_words)))
        return '{{!{0}}}'.format(' '.join(parts))

def process_value(v, safe=False):
    from .func import Function

    if v is True:
        return 'true'
    if v is False:
        return 'false'
    if isinstance(v, int_types + (float,)):
        return force_unicode(v)
    if isinstance(v, LocalParams):
        return '"{}"'.format(force_unicode(v))
    if isinstance(v, Function):
        return force_unicode(v)
    if isinstance(v, (datetime, date)):
        return v.strftime('%Y-%m-%dT%H:%M:%SZ')
    if isinstance(v, string_types) and SOLR_DATETIME_RE.match(v):
        return v
    if safe:
        return force_unicode(v)
    return safe_solr_input(v)

def maybe_wrap_parentheses(v):
    if not re.match(r'".*"', v, re.DOTALL) and re.search(r'\s', v):
        return '({})'.format(v)
    return v

def maybe_wrap_literal(v):
    if not re.match(r'".*"', v, re.DOTALL) and re.search(r'\s', v):
        # ' is not in SPECIAL_CHARACTERS
        return "'{}'".format(v.replace(r"'", r"\'"))
    return v

def process_field(field, op, value):
    if op == 'exact':
        return '{}:"{}"'.format(field, process_value(value))
    elif op == 'gte':
        return '{}:[{} TO *]'.format(field, process_value(value))
    elif op == 'lte':
        return '{}:[* TO {}]'.format(field, process_value(value))
    elif op == 'gt':
        return '{}:{{{} TO *}}'.format(field, process_value(value))
    elif op == 'lt':
        return '{}:{{* TO {}}}'.format(field, process_value(value))
    elif op in ('between', 'range'):
        v0 = '*' if value[0] is None else process_value(value[0])
        v1 = '*' if value[1] is None else process_value(value[1])
        if op == 'between':
            return '{}:{{{} TO {}}}'.format(field, v0, v1)
        elif op == 'range':
            return '{}:[{} TO {}]'.format(field, v0, v1)
    elif op == 'in':
        if hasattr(value, '__iter__') and len(value) > 0:
            return '({})'.format(
                ' {} '.format(X.OR).join(
                    ['{}:{}'.format(field, process_value(v)) for v in value]))
        else:
            return '({0}:[* TO *] AND NOT {0}:[* TO *])'.format(field)
    elif op == 'isnull':
        if value:
            return 'NOT {}:[* TO *]'.format(field)
        else:
            return '{}:[* TO *]'.format(field)
    elif op == 'startswith':
        return '{}:{}'.format(
            field, maybe_wrap_parentheses('{}*'.format(process_value(value))))
    elif value is None:
        return 'NOT {}:[* TO *]'.format(field)
    return '{}:{}'.format(field, maybe_wrap_parentheses(process_value(value)))

def fq_from_tuple(x):
    field, op = split_param(x[0])
    field_val = process_field(field, op, x[1])
    return field_val

def make_fq(x, local_params=None):
    def _make_fq(x, level):
        fq = []
        for child in x.children:
            if child is None:
                continue
            if isinstance(child, LocalParams):
                parts = [force_unicode(child)]
            elif isinstance(child, tuple):
                parts = [fq_from_tuple(child)]
            elif isinstance(child, string_types):
                parts = [safe_solr_input(child)]
            else:
                parts = _make_fq(child, level+1)
            fq += parts
        
        if level == 0 and x.connector == X.AND:
            return fq
        if fq:
            fq = ' {} '.format(x.connector).join(fq)
            if len(x.children) > 1:
                fq = '({})'.format(fq)
            if x.negated:
                return ['NOT ({})'.format(fq)]
            return [fq]
        return []

    local_params = local_params or LocalParams()
    return '{}{}'.format(
        force_unicode(local_params),
        (' {} '.format(x.connector)).join(_make_fq(x, 0)))

def make_q(q=None, local_params=None, *args, **kwargs):
    if q is None and not args and not kwargs:
        x = X_ALL
    else:
        x = X(q, *args, **kwargs)
    return make_fq(x, local_params)

def split_param(param):
    field_op = param.split('__')
    if len(field_op) == 1:
        return field_op[0], None
    return field_op[0], field_op[1]

def make_param(param, op):
    return '__'.join((param, op))
