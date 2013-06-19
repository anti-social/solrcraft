from __future__ import unicode_literals

from copy import deepcopy

from .util import LocalParams, X, make_fq, _pop_from_kwargs
from .types import instantiate, get_to_python
from .pysolr import Solr


def zip_counts(counts, n, over=0):
    acc = ()
    for v in counts:
        acc = acc + (v,)
        if len(acc) % (n + over) == 0:
            yield acc
            acc = acc[n:]
    if len(acc) == n:
        yield acc + (None,) * over


class FacetField(object):
    def __init__(self, field, local_params=None, instance_mapper=None,
                 type=None, **facet_params):
        self.field = field
        self.local_params = LocalParams(local_params)
        self.key = self.local_params.get('key', self.field)
        self.type = instantiate(type)
        self.to_python = get_to_python(self.type)
        self.facet_params = facet_params
        self.values = []
        self._instance_mapper = instance_mapper

    def __deepcopy__(self, memodict):
        # Fix for http://bugs.python.org/issue1515
        # self._instance_mapper can be instance method
        obj = type(self)(self.field, local_params=self.local_params,
                         instance_mapper=self._instance_mapper,
                         type=self.type, **self.facet_params)
        return obj

    def instance_mapper(self, ids):
        if self._instance_mapper:
            return self._instance_mapper(ids)
        return {}
        
    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.field'] = [make_fq(X(self.field), self.local_params)]
        for p, v in self.facet_params.items():
            params['f.{}.facet.{}'.format(self.field, p)] = v
        return params

    def process_data(self, results):
        self.values = []
        raw_facet_fields = results.raw_results.facets['facet_fields']
        facet_data = raw_facet_fields[self.key]
        for val, count in zip_counts(facet_data, 2):
            self.values.append(
                FacetValue(self.to_python(val), count, facet=self))

    def _populate_instances(self):
        values = [fv.value for fv in self.values]
        instances_map = self.instance_mapper(values)
        for fv in self.values:
            fv._instance = instances_map.get(fv.value)


class FacetValue(object):
    def __init__(self, value, count, facet=None):
        self.value = value
        self.count = count
        self.facet = facet
        self.pivot = None

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            if self.facet:
                self.facet._populate_instances()
            else:
                self._instance = None
        return self._instance


class FacetRange(object):
    def __init__(self, field, start, end, gap,
                 local_params=None, type=None, **facet_params):
        self.field = field
        self.orig_start = self.start = start
        self.orig_end = self.end = end
        self.orig_gap = self.gap = gap
        self.local_params = LocalParams(local_params)
        self.key = self.local_params.get('key', self.field)
        self.type = instantiate(type)
        self.to_python = get_to_python(self.type)
        self.facet_params = facet_params
        self.values = []

    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.range'] = [make_fq(X(self.field), self.local_params)]
        params['f.{}.facet.range.start'.format(self.field)] = self.orig_start
        params['f.{}.facet.range.end'.format(self.field)] = self.orig_end
        gap = self.orig_gap
        if isinstance(gap, (list, tuple)):
            gap = ','.join(gap)
        params['f.{}.facet.range.gap'.format(self.field)] = gap
        for p, v in self.facet_params.items():
            params['f.{}.facet.range.{}'.format(self.field, p)] = v
        return params

    def process_data(self, results):
        raw_facet_data = results.raw_results.facets \
                                            .get('facet_ranges', {}) \
                                            .get(self.key, {})
        self.start = self.to_python(raw_facet_data.get('start', self.start))
        self.end = self.to_python(raw_facet_data.get('end', self.end))
        self.gap = raw_facet_data.get('gap', self.gap)
        facet_counts = raw_facet_data.get('counts', [])
        for start, count, end in zip_counts(facet_counts, 2, 1):
            start = self.to_python(start)
            if end is None:
                end = self.end
            else:
                end = self.to_python(end)
            self.values.append(
                FacetRangeValue(start, end, count, facet=self))


class FacetRangeValue(object):
    def __init__(self, start, end, count, facet=None):
        self.count = count
        self.start = start
        self.end = end
        self.facet = facet

        
class FacetQuery(object):
    def __init__(self, fq, local_params=None):
        self.fq = fq
        self.local_params = LocalParams(local_params)
        self.key = self.local_params.get('key',
                                         make_fq(self.fq, self.local_params))
        self.count = None

    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.query'] = [make_fq(self.fq, self.local_params)]
        return params
        
    def process_data(self, results):
        raw_facet_queries = results.raw_results.facets['facet_queries']
        self.count = raw_facet_queries[self.key]


class FacetPivot(object):
    def __init__(self, *fields, **kwargs):
        self.fields = []
        self.instance_mappers = {}
        self.types = {}
        self.to_pythons = {}
        self.facet_params = {}
        for field in fields:
            kw = {}
            if isinstance(field, (list, tuple)):
                if len(field) == 1:
                    field = field[0]
                elif len(field) == 2:
                    field, kw = field
            self.instance_mappers[field] = _pop_from_kwargs(kw, 'instance_mapper')
            self.types[field] = _pop_from_kwargs(kw, 'type')
            self.to_pythons[field] = get_to_python(self.types[field])
            self.facet_params[field] = kw
            self.fields.append(field)
        self.field = self.fields[0]
        self.name = ','.join(self.fields)
        self.local_params = LocalParams(
            _pop_from_kwargs(kwargs, 'local_params'))
        self.key = self.local_params.get('key', self.name)
        self.values = []

    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.pivot'] = [make_fq(X(self.name), self.local_params)]
        for field, facet_params in self.facet_params.items():
            for p, v in facet_params.items():
                params['f.{}.facet.{}'.format(field, p)] = v
        return params

    def get_value(self, value):
        for fv in self.values:
            if fv.value == value:
                return fv
    
    def process_data(self, results):
        self.values = []
        raw_pivot = results.raw_results.facets.get('facet_pivot', {}).get(self.key, {})
        self.process_pivot(raw_pivot, self)

    def process_pivot(self, raw_pivot, root_pivot):
        self.root_pivot = root_pivot
        for facet_data in raw_pivot:
            to_python = root_pivot.to_pythons[self.fields[0]]
            fv = FacetValue(
                to_python(facet_data['value']), facet_data['count'], facet=self)
            if 'pivot' in facet_data:
                fv.pivot = FacetPivot(*self.fields[1:])
                fv.pivot.process_pivot(facet_data['pivot'], root_pivot)
            self.values.append(fv)
        
    def _populate_instances(self, field=None):
        if field is None:
            return self.root_pivot._populate_instances(field=self.field)
        
        facet_values = []
        pivots = [self]
        while pivots:
            next_pivots = []
            for cur_pivot in pivots:
                for fv in cur_pivot.values:
                    if fv.pivot:
                        next_pivots.append(fv.pivot)
                    if cur_pivot.field == field:
                        facet_values.append(fv)
            pivots = next_pivots

        values = set([fv.value for fv in facet_values])
        instance_mapper = self.instance_mappers.get(field)
        instances_map = instance_mapper(values) if instance_mapper else {}
        for fv in facet_values:
            fv._instance = instances_map.get(fv.value)
