from copy import deepcopy

from .util import X, make_fq


class FacetField(object):
    def __init__(self, field, local_params, instance_mapper=None, **kwargs):
        self.field = field
        self.local_params = local_params
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

    def process_data(self, raw_facet_fields):
        self.values = []
        facet_data = raw_facet_fields[self.local_params.get('key', self.field)]
        for i in xrange(0, len(facet_data), 2):
            self.add_value(FacetValue(facet_data[i], facet_data[i+1]))
        
    def add_value(self, fv):
        fv.facet = self
        self.values.append(fv)

    def _populate_instances(self):
        values = [fv.value for fv in self.values]
        instances_map = self.instance_mapper(values)
        for fv in self.values:
            fv._instance = instances_map.get(fv.value)

class FacetValue(object):
    def __init__(self, value, count):
        self.facet = None
        self.value = value
        self.count = count

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            self.facet._populate_instances()
        return self._instance
        
        
class FacetQuery(object):
    def __init__(self, fq, local_params):
        self.fq = fq
        self.local_params = local_params
        self.count = None

    def get_params(self):
        params = {}
        params['facet'] = True
        params['facet.query'] = [make_fq(self.fq, self.local_params)]
        return params
        
    def process_data(self, raw_facet_queries):
        self.count = raw_facet_queries[
            self.local_params.get('key', make_fq(self.fq, self.local_params))]
