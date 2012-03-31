
import re
import urllib
import logging
from datetime import datetime, date

from tree import Node


class SafeString(str):
    pass

class SafeUnicode(unicode):
    pass

def safe_solr_query(q):
    if q is None:
        return SafeString('*:*')
    if isinstance(q, (SafeString, SafeUnicode)):
        return q
    safe1_re = re.compile(r'([^\w\d\s]|[\n\r])', re.U)
    safe2_re = re.compile(r'(\A|\s)(\s*(or|and|not)\s*)+(\s|\Z)', re.U | re.I)
    q = re.sub(safe1_re, ' ', q)
    q = re.sub(safe2_re, ' ', q)
    q = ' '.join(q.split())
    if not q:
        return SafeString('somequerythatneverwillbeusedintheproject')
    if isinstance(q, unicode):
        return SafeUnicode(q)
    return SafeString(q)

class X(Node):
    AND = 'AND'
    OR = 'OR'
    default = AND
    
    def __init__(self, *args, **kwargs):
        op = kwargs.pop('_op', self.default).upper()
        if op not in (self.AND, self.OR):
            op = self.default
        super(X, self).__init__(children=list(args) + kwargs.items(), connector=op)

    def _combine(self, other, conn):
        if not isinstance(other, X):
            raise TypeError(other)
        obj = type(self)()
        obj.add(self, conn)
        obj.add(other, conn)
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

def process_value(v):
    if isinstance(v, (datetime, date)):
        return v.strftime('%Y-%m-%dT%H:%M:%SZ')
    if v is True:
        return '1'
    if v is False:
        return '0'
    return v

def process_field(field, op, value):
    if op == 'gte':
        return '%s:[%s TO *]' % (field, value)
    elif op == 'lte':
        return '%s:[* TO %s]' % (field, value)
    elif op == 'gt':
        return '%s:{%s TO *}' % (field, value)
    elif op == 'lt':
        return '%s:{* TO %s}' % (field, value)
    elif op == 'between':
        v0 = '*' if value[0] is None else value[0]
        v1 = '*' if value[1] is None else value[1]
        return '%s:[%s TO %s]' % (field, v0, v1)
    elif op == 'in':
        if hasattr(value, '__iter__') and len(value) > 0:
            return '(%s)' % (' %s ' % X.OR).join(['%s:%s' % (field, v) for v in value])
        else:
            return None
    if value is None:
        return '-%s:[* TO *]' % field
    return '%s:%s' % (field, value)

def make_fq(x, add_tags=True, as_string=False):
    def _from_tuple(x, level):        
        field, op = split_param(x[0])
        if isinstance(x[1], (tuple, list)):
            value = []
            for v in x[1]:
                value.append(process_value(v))
        else:
            value = process_value(x[1])
        field_val = process_field(field, op, value)
        if field_val:
            return {'fq': [field_val], 'tags': set([field])}
        return {'fq': [], 'tags': set()}

    def _from_string(x, level):
        return {'fq': [x], 'tags': set([x])}
    
    def _add_tag(x, tags, level):
        if level == 0 and len(tags) == 1:
            return '{!tag=%s}%s' % (list(tags)[0], x)
        return x
    
    def _make_fq(x, level):
        output = {'fq': [], 'tags': set()}
        for child in x.children:
            if child is None:
                continue
            if isinstance(child, tuple):
                part = _from_tuple(child, level+1)
            elif isinstance(child, basestring):
                part = _from_string(child, level+1)
            else:
                part = _make_fq(child, level+1)
            output['tags'].update(part['tags'])
            if add_tags:
                output['fq'] += [_add_tag(fq, part['tags'], level) for fq in part['fq']]
            else:
                output['fq'] += part['fq']
        
        neg = '-' if x.negated else ''
        if x.connector == X.AND:
            t = neg + '%s'
        elif x.connector == X.OR:
            t = neg + '(%s)'

        if level == 0 and x.connector == X.AND:
            return output
        if output['fq']:
            fq = (' %s ' % x.connector).join(output['fq'])
            if t.startswith('-') and fq.startswith('-'):
                t = t[1:]
                fq = fq[1:]
            return {'fq': [t % fq], 'tags': output['tags']}
        return {'fq': [], 'tags': set()}

    if as_string:
        return (' %s ' % x.connector).join(_make_fq(x, 0)['fq'])
    return _make_fq(x, 0)['fq']

def get_tag(x):
    if isinstance(x, tuple):
        return x[0]
    if x.connector != x.OR:
        return ''
    tags = set()
    for child in x.children:
        if not isinstance(child, tuple):
            return ''
        tags.add(child[0])
    if not tags or len(tags) > 1:
        return ''
    return tags[0]

def convert_fq_to_filters_map(x):
    if isinstance(x, tuple):
        return {x[0]: set([str(x[1])])}
    elif x.connector == x.AND:
        filters_map = {}
        for child in x.children:
            if child is None:
                continue
            if isinstance(child, tuple):
                filters_map.setdefault(child[0], set()).add(str(child[1]))
            elif child.connector == child.AND and len(child.children) == 1 and isinstance(child.children[0], tuple):
                filters_map.setdefault(child.children[0][0], set()).add(str(child.children[0][1]))
            elif child.connector == child.OR:
                f = {}
                for grandchild in child.children:
                    if grandchild is None:
                        continue
                    if isinstance(grandchild, tuple):
                        f.setdefault(grandchild[0], set()).add(str(grandchild[1]))
                        continue
                    if grandchild.connector == x.AND and len(grandchild.children) == 1 and isinstance(grandchild.children[0], tuple):
                        f.setdefault(grandchild.children[0][0], set()).add(str(grandchild.children[0][1]))
                        continue
                    break
                else:
                    if len(f) == 1:
                        filters_map.update(f)
        return filters_map
    return {}

def prepare_params(params):
    new_params = {}
    for key, val in params.items():
        if isinstance(val, tuple):
            new_params[key] = ','.join(val)
        elif isinstance(val, bool):
            new_params[key] = str(val).lower()
        elif val is None:
            pass
        else:
            new_params[key] = val
    return new_params

def split_param(param):
    field_op = param.split('__')
    if len(field_op) == 1:
        return field_op[0], 'exact'
    return field_op[0], field_op[1]

def make_param(param, op):
    return '__'.join((param, op))

def unpack_tuples(l, n, fillval=None):
    for t in l:
        if len(t) < n:
            yield t + (fillval,)*(n-len(t))
        elif len(t) == n:
            yield t
        else:
            yield t[:n]
