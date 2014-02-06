from __future__ import unicode_literals

from copy import deepcopy
from itertools import chain

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
    def __init__(self, field, **facet_params):
        self.field = field
        self.local_params = LocalParams(_pop_from_kwargs(facet_params, 'local_params'))
        self.key = self.local_params.get('key', self.field)
        self.type = instantiate(_pop_from_kwargs(facet_params, 'type'))
        self.to_python = get_to_python(self.type)
        self.instance_mapper = _pop_from_kwargs(facet_params, 'instance_mapper')
        self.mapper_registry = None
        self.facet_params = facet_params
        self.values = []

    def clone(self):
        return self.__class__(
            self.field, local_params=self.local_params,
            instance_mapper=self.instance_mapper, type=self.type,
            **self.facet_params
        )
        
    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.field'] = [make_fq(X(self.field), self.local_params)]
        for p, v in self.facet_params.items():
            params['f.{}.facet.{}'.format(self.field, p)] = v
        return params

    def get_value(self, value):
        for fv in self.values:
            if fv.value == value:
                return fv

    def set_mapper_registry(self, mapper_registry):
        if self.instance_mapper:
            mapper_registry.setdefault(self.instance_mapper, []).append(self)
        self.mapper_registry = mapper_registry

    def process_data(self, results):
        self.values = []
        raw_facet_fields = results.raw_results.facets.get('facet_fields', {})
        facet_data = raw_facet_fields.get(self.key, [])
        for val, count in zip_counts(facet_data, 2):
            self.values.append(
                FacetValue(self.to_python(val), count, facet=self))

    def _populate_instances(self):
        if self.mapper_registry and self.instance_mapper in self.mapper_registry:
            facets = self.mapper_registry[self.instance_mapper]
        else:
            facets = [self]
        values = list(chain(*(f.values for f in facets)))
        if self.instance_mapper:
            instances = self.instance_mapper([v.value for v in values])
        else:
            instances = {}
        for fv in values:
            fv._instance = instances.get(fv.value)


class FacetValue(object):
    def __init__(self, value, count, facet=None):
        self.value = self.orig_value = value
        if facet:
            self.value = facet.to_python(self.orig_value)
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

    def clone(self):
        return self.__class__(
            self.field, self.orig_start, self.orig_end, self.orig_gap,
            local_params=self.local_params, type=self.type,
            **self.facet_params
        )
        
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

    def clone(self):
        return self.__class__(self.fq, local_params=self.local_params)
        
    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.query'] = [make_fq(self.fq, self.local_params)]
        return params
        
    def process_data(self, results):
        raw_facet_queries = results.raw_results.facets['facet_queries']
        self.count = raw_facet_queries[self.key]


class FacetPivot(FacetField):
    def __init__(self, *fields, **kwargs):
        self.fields = fields
        self.kwargs = kwargs

        field, facet_params = self._get_field_and_params(fields[0])
        facet_params.update(kwargs)
        super(FacetPivot, self).__init__(field, **facet_params)

        self.facets = [self]
        for facet_data in fields[1:]:
            field, facet_params = self._get_field_and_params(facet_data)
            self.facets.append(FacetField(field, **facet_params))

        self.field_names = [f.field for f in self.facets]
        self.name = ','.join(self.field_names)
        self.key = self.local_params.get('key', self.name)

    def _get_field_and_params(self, facet_data):
        if not isinstance(facet_data, (list, tuple)):
            facet_data = (facet_data,)
        if len(facet_data) >= 2:
            return facet_data[0], facet_data[1]
        return facet_data[0], {}
        
    def clone(self):
        return self.__class__(*self.fields, **self.kwargs)

    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.pivot'] = [make_fq(X(self.name), self.local_params)]
        for facet in self.facets:
            for p, v in facet.facet_params.items():
                params['f.{}.facet.{}'.format(facet.field, p)] = v
        return params

    def process_data(self, results):
        raw_data = results.raw_results.facets.get('facet_pivot', {}).get(self.key, {})
        self.process_facet(raw_data, self.facets)

    def process_facet(self, raw_data, facets):
        if not facets:
            return

        facet = facets[0]
        next_facet = facets[1] if len(facets) >= 2 else None
        rest_facets = facets[2:]
        for facet_data in raw_data:
            fv = FacetValue(
                facet_data['value'], facet_data['count'], facet=facet,
            )
            if 'pivot' in facet_data and next_facet:
                fv.pivot = next_facet.clone()
                fv.pivot.set_mapper_registry(self.mapper_registry)
                self.process_facet(facet_data['pivot'], [fv.pivot] + rest_facets)
            facet.values.append(fv)
