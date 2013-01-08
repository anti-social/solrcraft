
import re
import urllib
import logging
from copy import deepcopy
from datetime import datetime, date

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict

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

def process_special_words(value, words=None):
    words = words or SPECIAL_WORDS
    for w in words:
        value = re.sub(r'(\A|\s+)(%s)(\s+|\Z)' % w, lambda m: m.group(0).lower(), value)
    return value

def process_special_characters(value, chars=None):
    chars = chars or SPECIAL_CHARACTERS
    for c in chars:
        value = value.replace(c, r'\%s' % c)
    return value

def contains_special_characters(value, chars=None):
    chars = chars or SPECIAL_CHARACTERS
    return any(map(lambda c: c in value, chars))

def safe_solr_input(value):
    if isinstance(value, (SafeString, SafeUnicode)):
        return value

    if not isinstance(value, basestring):
        value = unicode(value)
    
    value = process_special_words(value)

    value = process_special_characters(value)

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

class LocalParams(OrderedDict):
    SPECIAL_CHARACTERS = " '" + SPECIAL_CHARACTERS
    
    def update(self, other=None, **kwargs):
        if other is None:
            other = OrderedDict()
        elif isinstance(other, LocalParams):
            other = OrderedDict(other)
        elif hasattr(other, 'keys'):
            other = OrderedDict((k, other[k]) for k in sorted(other.keys()))
        elif hasattr(other, '__iter__'):
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
            raise ValueError("Key '%s' contents special characters" % key)
        if key == 'type' and contains_special_characters(value, self.SPECIAL_CHARACTERS):
            raise ValueError("Type value '%s' contents special characters" % value)
        
        self[key] = value

    def _quote(self, value, replace_words=True):
        value = unicode(value)
        if replace_words:
            value = process_special_words(value)
        if contains_special_characters(value, self.SPECIAL_CHARACTERS):
            return "'%s'" % value.replace("'", "\\\\'").replace('"', '\\"')
        return value
    
    def __str__(self):
        if not self:
            return ''

        parts = []
        for key, value in self.items():
            if key == 'type':
                parts.append(value)
            else:
                replace_words = True
                if isinstance(value, X):
                    value = make_fq(value)
                    replace_words = False
                parts.append(
                    '%s=%s' % (
                        key,
                        self._quote(process_value(value, safe=True),
                                    replace_words=replace_words)))
        return '{!%s}' % ' '.join(parts)

def process_value(v, safe=False):
    if v is True:
        return 'true'
    if v is False:
        return 'false'
    if isinstance(v, LocalParams):
        return '"%s"' % unicode(v)
    if isinstance(v, (datetime, date)):
        return v.strftime('%Y-%m-%dT%H:%M:%SZ')
    if isinstance(v, basestring) and SOLR_DATETIME_RE.match(v):
        return v
    if safe:
        return unicode(v)
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
    elif op in ('between', 'range'):
        v0 = '*' if value[0] is None else process_value(value[0])
        v1 = '*' if value[1] is None else process_value(value[1])
        if op == 'between':
            return '%s:{%s TO %s}' % (field, v0, v1)
        elif op == 'range':
            return '%s:[%s TO %s]' % (field, v0, v1)
    elif op == 'in':
        if hasattr(value, '__iter__') and len(value) > 0:
            return '(%s)' % (' %s ' % X.OR).join(
                ['%s:%s' % (field, process_value(v)) for v in value])
        else:
            return '(%s:[* TO *] AND NOT %s:[* TO *])' % (field, field)
    elif op == 'isnull':
        if value:
            return 'NOT %s:[* TO *]' % field
        else:
            return '%s:[* TO *]' % field
    elif op == 'startswith':
        return '%s:%s*' % (field, process_value(value))
    elif value is None:
        return 'NOT %s:[* TO *]' % field
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
            if isinstance(child, LocalParams):
                parts = [unicode(child)]
            elif isinstance(child, tuple):
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
                return ['NOT (%s)' % fq]
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
