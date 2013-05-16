from __future__ import unicode_literals


def maybe_float(v):
    if v is not None:
        return float(v)


def maybe_int(v):
    if v is not None:
        return int(v)


class StatsMixin(object):
    def __init__(self):
        self.min = None
        self.max = None
        self.sum = None
        self.count = None
        self.missing = None
        self.sum_of_squares = None
        self.mean = None
        self.stddev = None

    def _process_data(self, raw_stats):
        self.min = maybe_float(raw_stats.get('min'))
        self.max = maybe_float(raw_stats.get('max'))
        self.sum = maybe_float(raw_stats.get('sum'))
        self.count = maybe_int(raw_stats.get('count'))
        self.missing = maybe_int(raw_stats.get('missing'))
        self.sum_of_squares = maybe_float(raw_stats.get('sumOfSquares'))
        self.mean = maybe_float(raw_stats.get('mean'))
        self.stddev = maybe_float(raw_stats.get('stddev'))


class Stats(StatsMixin):
    def __init__(self, field, facet_fields=None):
        super(Stats, self).__init__()
        self.field = field
        self.facets = []
        for facet_field in (facet_fields or []):
            if isinstance(facet_field, (tuple, list)):
                if len(facet_field) > 1:
                    facet = StatsFacet(facet_field[0], instance_mapper=facet_field[1])
                else:
                    facet = StatsFacet(facet_field[0])
            else:
                facet = StatsFacet(facet_field)
            self.facets.append(facet)

    def get_params(self):
        params = {}
        params['stats.field'] = [self.field]
        params['f.%s.stats.facet' % self.field] = [
            facet.field for facet in self.facets]
        return params

    def process_data(self, results):
        raw_stats = results.raw_results.stats.get('stats_fields', {}).get(self.field) or {}
        for facet in self.facets:
            facet.process_data(raw_stats.get('facets', {}))
        self._process_data(raw_stats)

    def get_facet(self, facet_field):
        for facet in self.facets:
            if facet.field == facet_field:
                return facet


class StatsFacet(object):
    def __init__(self, field, instance_mapper=None):
        self.field = field
        self._instance_mapper = instance_mapper
        self.values = []

    def process_data(self, raw_data):
        for value, raw_fv_data in raw_data.get(self.field, {}).items():
            fv = StatsFacetValue(value, facet=self)
            fv._process_data(raw_fv_data)
            self.values.append(fv)
        
    def get_value(self, value):
        for fv in self.values:
            if fv.value == value:
                return fv

    def _populate_instances(self):
        values = [fv.value for fv in self.values]
        instances = {}
        if self._instance_mapper:
            instances = self._instance_mapper(values)
        for fv in self.values:
            fv._instance = instances.get(fv.value)


class StatsFacetValue(StatsMixin):
    def __init__(self, value, facet=None):
        super(StatsFacetValue, self).__init__()
        self.value = value
        self.facet = facet

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            if self.facet:
                self.facet._populate_instances()
            else:
                self._instance = None
        return self._instance
