from datetime import datetime, timedelta
from copy import deepcopy
import re
import sys

from util import X, split_param, make_param, process_value


date_formula_re = re.compile('(now)([+\-])([\d]+)d')

def parse_param(arg):
    if isinstance(arg, basestring):
        m = date_formula_re.match(arg)
        if m:
            dt, sign, days = m.groups()
            dt = datetime.now()
            days = int(days)
            if sign == '-':
                arg = 'NOW/DAY-%sDAYS' % days
            else:
                arg = 'NOW/DAY+%sDAYS' % days
    elif arg is True:
        arg = '1'
    elif arg is False:
        arg = '0'
    return arg

def isnull_op(f, v):
    if v == '1':
        return ~X(**{f: '[* TO *]'})
    elif v == '0':
        return X(**{f: '[* TO *]'})
    return

OPERATORS = {
    'exact': lambda f, v: X(**{f: parse_param(v)}),
    'gte': lambda f, v: X(**{f: '[%s TO *]' % parse_param(v)}),
    'lte': lambda f, v: X(**{f: '[* TO %s]' % parse_param(v)}),
    'isnull': isnull_op,
    }

def isnull_op4facet(f, v):
    if str(v) == '1':
        return '-%s:[* TO *]' % f
    elif str(v) == '0':
        return '%s:[* TO *]' % f
    return

OPS_FOR_FACET = {
    'exact': lambda f, v: '%s:%s' % (f, parse_param(v)),
    'gte': lambda f, v: '%s:[%s TO *]' % (f, parse_param(v)),
    'lte': lambda f, v: '%s:[* TO %s]' % (f, parse_param(v)),
    'isnull': isnull_op4facet,
    }


class Facet(object):
    model = None
    field = None
    name = None
    title = None
    help_text = None
    
    select_multiple = True

    default_params = {}
    
    def __init__(self, query, model=None, name=None, title=None, help_text=None):
        self.query = query
        self.model = model or self.model
        self.name = name or self.name
        self.title = title or self.title or self.name
        self.help_text = help_text or self.help_text
        self._values = []
        self._selected_values = []

    def __iter__(self):
        return iter(self.selected_values + self.values)

    def __getitem__(self, k):
        return self.values[k]

    def __unicode__(self):
        return unicode(self.title)

    def __deepcopy__(self, memo):
        facet = self.__class__(self.query, model=self.model, name=self.name, title=self.title, help_text=self.help_text)
        facet._values = deepcopy(self._values, memo)
        facet._selected_values = deepcopy(self._selected_values, memo)
        for new_fv, fv in zip(facet, self):
            new_fv.facet = facet
            if hasattr(self, '_instance'):
                new_fv._instance = self._instance
        return facet
        
    def add_value(self, facet_value):
        facet_value.facet = self
        if facet_value.selected:
            self._selected_values.append(facet_value)
        else:
            self._values.append(facet_value)

    @property
    def values(self):
        return [v for v in self._values if v.count != 0]

    @property
    def selected_values(self):
        return [v for v in self._selected_values if v.count != 0]

    @property
    def all_values(self):
        return self.selected_values + self.values
    
    @property
    def has_selected(self):
        return bool(self._selected_values)
    
    def get_instances(self, ids):
        if self.model:
            return dict([(obj.id, obj) for obj in self.model.query.filter(self.model.id.in_(ids))])
        return {}

    def _populate_instances(self):
        ids = [fv.value for fv in self]
        instances = self.get_instances(ids)
        for fv in self:
            fv._instance = instances.get(fv.value)

def compound_facet_factory(name, title, facets, **attrs):
    def _init(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.name = name
        
    def _add_value(self, facet_value):
        def fv_cmp(fv1, fv2):
            facet_cls1 = self.paramvalue_to_facet_cls_map.get((fv1.param, fv1.value))
            facet_cls2 = self.paramvalue_to_facet_cls_map.get((fv2.param, fv2.value))
            try:
                ix1 = self.facets.index(facet_cls1)
                ix2 = self.facets.index(facet_cls2)
                ret = cmp(ix1, ix2)
                if ret == 0:
                    return cmp(fv1.count, fv2.count)
                return ret
            except ValueError:
                return 0

        facet_cls = self.paramvalue_to_facet_cls_map.get((facet_value.param, facet_value.value))
        facet = facet_cls(self.query)
        facet.add_value(facet_value)
        facet_value._facet = self
        self._values.append(facet_value)
        self._values.sort(cmp=fv_cmp)
        
    attrs['facets'] = facets
    attrs['fields'] = [facet.field for facet in facets]
    attrs['queries'] = []
    attrs['paramvalue_to_facet_cls_map'] = {}
    for facet in facets:
        if hasattr(facet, 'queries') and facet.queries:
            attrs['queries'] += facet.queries
            for q in facet.queries:
                attrs['paramvalue_to_facet_cls_map'][(make_param(*split_param(q[0][0])), process_value(q[0][1]))] = facet
    attrs['name'] = name
    attrs['title'] = title
    attrs['add_value'] = _add_value
    attrs['__init__'] = _init
    return type('CompoundFacet', (Facet,), attrs)

class FacetValue(object):
    def __init__(self, param, value, count, selected=False, title=None, help_text=None):
        self.param = param
        self.facet = None
        self.value = value
        self.count = count
        self.selected = selected
        self._title = title
        self.help_text = help_text

    @property
    def title(self):
        if self._title:
            return self._title
        elif hasattr(self.instance, '__unicode__'):
            return self.instance
        elif hasattr(self.instance, 'name'):
            return self.instance.name
        return self.value

    def __unicode__(self):
        return unicode(self.title)

    def __str__(self):
        return unicode(self)

    def __deepcopy__(self, memo):
        fv = self.__class__(self.param, self.value, self.count,
                            selected=self.selected, title=self._title, help_text=self.help_text)
        return fv

    def with_count(self):
        name = unicode(self)
        if self.selected:
            return u'%s *' % name
        if self.facet and self.facet.has_selected and self.facet.select_multiple:
            return u'%s (+%s)' % (name, self.count)
        return u'%s (%s)' % (name, self.count)

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            if self.facet:
                self.facet._populate_instances()
            else:
                self._instance = None
        return self._instance

    @property
    def count_sign(self):
        if self.facet and self.facet.has_selected and self.facet.select_multiple:
            return '+%s' % self.count
        return str(self.count)

    def get_url(self, *args, **kwargs):
        """
        Proxy method to facet's get_url method.
        """
        if hasattr(self.facet, 'get_url') and callable(self.facet.get_url):
            return self.facet.get_url(self, *args, **kwargs)

    def get_selected_url(self, *args, **kwargs):
        """
        Proxy method to facet's get_selected_url method.
        """
        if hasattr(self.facet, 'get_selected_url') and callable(self.facet.get_selected_url):
            return self.facet.get_selected_url(self, *args, **kwargs)

# class FacetQueryValue(FacetValue):
#     @property
#     def param(self):
#         return self._param
