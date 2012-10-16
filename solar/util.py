
import re
import urllib
import logging
from copy import deepcopy
from datetime import datetime, date

from .tree import Node


CALENDAR_UNITS = ['MILLI', 'MILLISECOND', 'SECOND', 'MINUTE',
                  'HOUR', 'DAY', 'MONTH', 'YEAR']
UNIT_GROUPS =  '|'.join('(%sS?)' % unit for unit in CALENDAR_UNITS)
SOLR_DATETIME_RE = re.compile(
    r'^NOW(/(%s))?([+-]\d+(%s))*$' % (UNIT_GROUPS, UNIT_GROUPS))

SPECIAL_WORDS = ['AND', 'OR', 'NOT', 'TO']

# See: http://lucene.apache.org/core/3_6_0/queryparsersyntax.html#Escaping%20Special%20Characters
SPECIAL_CHARACTERS =  r'\+-&|!(){}[]^"~*?:'

class SafeString(str):
    pass

class SafeUnicode(unicode):
    pass

def safe_solr_input(value):
    if isinstance(value, (SafeString, SafeUnicode)):
        return value

    if not isinstance(value, basestring):
        value = unicode(value)
    
    for w in SPECIAL_WORDS:
        value = re.sub(r'(\A|\s+)(%s)(\s+|\Z)' % w, lambda m: m.group(0).lower(), value)

    for c in SPECIAL_CHARACTERS:
        value = value.replace(c, r'\%s' % c)

    if isinstance(value, unicode):
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
        super(X, self).__init__(children=list(args) + kwargs.items(),
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

class LocalParams(object):
    def __init__(self, params=None):
        self.local_params = []
        self.keys = set()

        params = params or []
        if isinstance(params, dict):
            params = sorted(params.items(), key=lambda p: p[0])
            
        for p in params:
            self.add(p)

    def add(self, key, value=None):
        if value is None and isinstance(key, (list, tuple)):
            key, value = key
        self.local_params.append((key, value))
        self.keys.add(key)

    def get(self, key, default=None):
        for k, v in self.local_params:
            if k == key:
                return v
        return default

    def update(self, local_params):
        for k, v in local_params:
            self.add(k, v)
        
    def __contains__(self, key):
        return key in self.keys

    def __iter__(self):
        return iter(self.local_params)
            
    def __str__(self):
        if not self.local_params:
            return ''

        parts = []
        for key, value in self.local_params:
            if value is None:
                parts.append(key)
            else:
                parts.append('%s=%s' % (key, value))
        return '{!%s}' % ' '.join(parts)

def process_value(v):
    if v is True:
        return '1'
    if v is False:
        return '0'
    if isinstance(v, (datetime, date)):
        return v.strftime('%Y-%m-%dT%H:%M:%SZ')
    if isinstance(v, basestring) and SOLR_DATETIME_RE.match(v):
        return v
    return safe_solr_input(v)

def process_field(field, op, value):
    if op == 'exact':
        return '%s:"%s"' % (field, process_value(value))
    elif op == 'gte':
        return '%s:[%s TO *]' % (field, process_value(value))
    elif op == 'lte':
        return '%s:[* TO %s]' % (field, process_value(value))
    elif op == 'gt':
        return '%s:{%s TO *}' % (field, process_value(value))
    elif op == 'lt':
        return '%s:{* TO %s}' % (field, process_value(value))
    elif op == 'between':
        v0 = '*' if value[0] is None else process_value(value[0])
        v1 = '*' if value[1] is None else process_value(value[1])
        return '%s:[%s TO %s]' % (field, v0, v1)
    elif op == 'in':
        if hasattr(value, '__iter__') and len(value) > 0:
            return '(%s)' % (' %s ' % X.OR).join(
                ['%s:%s' % (field, process_value(v)) for v in value])
        else:
            return '%s:[* TO *] AND (NOT %s:[* TO *])' % (field, field)
    elif op == 'isnull':
        if value:
            return '(NOT %s:[* TO *])' % field
        else:
            return '%s:[* TO *]' % field
    elif op == 'startswith':
        return '%s:%s*' % (field, process_value(value))
    elif value is None:
        return '(NOT %s:[* TO *])' % field
    return '%s:%s' % (field, process_value(value))

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
            if isinstance(child, tuple):
                parts = [fq_from_tuple(child)]
            elif isinstance(child, basestring):
                parts = [safe_solr_input(child)]
            else:
                parts = _make_fq(child, level+1)
            fq += parts
        
        if level == 0 and x.connector == X.AND:
            return fq
        if fq:
            fq = (' %s ' % x.connector).join(fq)
            if len(x.children) > 1:
                fq = '(%s)' % fq
            if x.negated:
                return ['(NOT %s)' % fq]
            return [fq]
        return []

    local_params = local_params or LocalParams()
    return '%s%s' % (str(local_params),
                     (' %s ' % x.connector).join(_make_fq(x, 0)))

def split_param(param):
    field_op = param.split('__')
    if len(field_op) == 1:
        return field_op[0], None
    return field_op[0], field_op[1]

def make_param(param, op):
    return '__'.join((param, op))
