from copy import deepcopy

from .util import LocalParams, X, make_fq


class FacetField(object):
    def __init__(self, field, local_params=None, instance_mapper=None, **kwargs):
        self.field = field
        self.local_params = local_params or LocalParams()
        self.facet_params = kwargs
        self.values = []
        self._instance_mapper = instance_mapper

    def __deepcopy__(self, memodict):
        # Fix for http://bugs.python.org/issue1515
        # self._instance_mapper can be instance method
        obj = type(self)(self.field, self.local_params,
                         instance_mapper=self._instance_mapper,
                         **self.facet_params)
        obj.values = deepcopy(self.values, memodict)
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
            params['f.%s.facet.%s' % (self.field, p)] = v
        return params

    def process_data(self, results):
        self.values = []
        raw_facet_fields = results.raw_results.facets['facet_fields']
        facet_data = raw_facet_fields[self.local_params.get('key', self.field)]
        for i in xrange(0, len(facet_data), 2):
            self.values.append(FacetValue(facet_data[i], facet_data[i+1], facet=self))

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
        
        
class FacetQuery(object):
    def __init__(self, fq, local_params=None):
        self.fq = fq
        self.local_params = local_params or LocalParams()
        self.count = None

    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.query'] = [make_fq(self.fq, self.local_params)]
        return params
        
    def process_data(self, results):
        raw_facet_queries = results.raw_results.facets['facet_queries']
        self.count = raw_facet_queries[
            self.local_params.get('key', make_fq(self.fq, self.local_params))]


class FacetPivot(object):
    def __init__(self, *fields, **kwargs):
        self.fields = []
        self.instance_mappers = {}
        self.facet_params = {}
        for field in fields:
            instance_mapper = None
            facet_params = None
            if isinstance(field, (list, tuple)):
                if len(field) == 1:
                    field = field[0]
                elif len(field) == 2:
                    field, instance_mapper = field
                elif len(field) == 3:
                    field, instance_mapper, facet_params = field
            self.fields.append(field)
            if instance_mapper:
                self.instance_mappers[field] = instance_mapper
            if facet_params:
                self.facet_params[field] = facet_params
        self.field = self.fields[0]
        self.name = ','.join(self.fields)
        self.local_params = kwargs.pop('local_params', None) or LocalParams()
        self.facet_pivot_params = kwargs
        self.values = []

    def get_key(self):
        return self.local_params.get('key', self.name)

    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.pivot'] = [make_fq(X(self.name), self.local_params)]
        for p, v in self.facet_pivot_params.items():
            params['facet.pivot.%s' % p] = v
        for field, facet_params in self.facet_params.items():
            for p, v in facet_params.items():
                params['f.%s.facet.%s' % (field, p)] = v
        return params

    def get_value(self, value):
        for fv in self.values:
            if fv.value == value:
                return fv
    
    def process_data(self, results):
        self.values = []
        raw_pivot = results.raw_results.facets['facet_pivot'][self.get_key()]
        self.process_pivot(raw_pivot, self)

    def process_pivot(self, raw_pivot, root_pivot):
        self.root_pivot = root_pivot
        for facet_data in raw_pivot:
            fv = FacetValue(facet_data['value'], facet_data['count'], facet=self)
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
