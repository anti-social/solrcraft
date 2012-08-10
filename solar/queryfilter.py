from copy import deepcopy

from .util import X, LocalParams, make_fq


def between_op(f, v):
    v1, v2 = v.split(':')
    return X(**{'%s__between' % f: (v1, v2)})

def isnull_op(f, v):
    if v == '1':
        return X({'%s__isnull' % f: True})
    elif v == '0':
        return X({'%s__isnull' % f: False})
    return

OPERATORS = {
    'exact': lambda f, v: X(**{f: v}),
    'gte': lambda f, v: X(**{'%s__gte' % f: v}),
    'gt': lambda f, v: X(**{'%s__gt' % f: v}),
    'lte': lambda f, v: X(**{'%s__lte' % f: v}),
    'lt': lambda f, v: X(**{'%s__lt' % f: v}),
    'between': between_op,
    'isnull': isnull_op,
    }

class QueryFilter(object):
    def __init__(self, *filters):
        self.filters = []
        self.filter_names = []
        self.ordering_filter = None
        self.params = None
        self.results = None
        for f in filters:
            self.add_filter(f)

    def _convert_params(self, params):
        if hasattr(params, 'getall'):
            # Webob
            new_params = params.dict_of_lists()
        elif hasattr(params, 'getlist'):
            # Django
            new_params = dict(params.lists())
        elif isinstance(params, (list, tuple)):
            # list, tuple
            new_params = dict(params)
        elif isinstance(params, dict):
            # dict
            new_params = deepcopy(params)
        else:
            raise ValueError("'params' must be Webob MultiDict, "
                             "Django QueryDict, list, tuple or dict")
        return new_params

    def apply(self, query, params, exclude=None):
        self.params = self._convert_params(params)
        for filter in self.filters:
            if exclude is None or filter.name not in exclude:
                query = filter.apply(query, self.params)

        if self.ordering_filter:
            query = self.ordering_filter.apply(query, self.params)

        return query

    def add_filter(self, filter):
        if isinstance(filter, basestring):
            filter = Filter(filter)
        if filter.name in self.filter_names:
            return
        self.filters.append(filter)
        self.filter_names.append(filter.name)

    def get_filter(self, name):
        for f in self.filters:
            if f.name == name:
                return f

    def add_ordering(self, ordering_settings=None, order_param='sort'):
        ordering_filter = OrderingFilter(order_param, ordering_settings)
        if self.ordering_filter is None:
            self.ordering_filter = ordering_filter

    def process_results(self, results):
        self.results = results
        for f in self.filters:
            f.process_results(self.results, self.params)
            
            # for fq, local_params in results.query._fq:
            #     if local_params.get('tag') == f.name:
            #         print fq

            # for facet in results.facet_queries:
            #     if facet.local_params.get('ex') == f.name:
            #         print facet

class BaseFilter(object):
    def __init__(self, name):
        self.name = name

    def _filter_and_split_params(self, params):
        """Returns [(operator1, value1), (operator2, value2)]"""
        items = []
        for p, v in sorted(params.items(), key=lambda i: i[0]):
            ops = p.split('__')
            name = ops[0]
            if len(ops) == 1:
                op = 'exact'
            else:
                op = '__'.join(ops[1:])
            if name == self.name:
                for w in v:
                    items.append((op, w))
        return items

    def process_results(self, results, params):
        raise NotImplementedError()

class Filter(BaseFilter):
    fq_connector = X.OR
    
    def _filter_query(self, query, fqs):
        if fqs:
            return query.filter(*fqs, _op=self.fq_connector, _local_params={'tag': self.name})
        return query

    def _make_x(self, op, v):
        op_func = OPERATORS.get(op)
        if op_func:
            return op_func(self.name, v)
        
    def apply(self, query, params):
        fqs = []
        for op, v in self._filter_and_split_params(params):
            x = self._make_x(op, v)
            if x:
                fqs.append(x)
        return self._filter_query(query, fqs)

    def process_results(self, results, params):
        pass

class FacetFilterValue(object):
    def __init__(self, filter_name, facet_value, selected):
        self.filter_name = filter_name
        self.facet_value = facet_value
        self.selected = selected

    @property
    def value(self):
        return self.facet_value.value

    @property
    def count(self):
        return self.facet_value.count
    
    @property
    def instance(self):
        return self.facet_value.instance

class FacetFilter(Filter):
    filter_value_cls = FacetFilterValue
    
    def __init__(self, name, _local_params=None, _instance_mapper=None, **kwargs):
        self.name = name
        self.local_params = LocalParams(_local_params)
        self._instance_mapper = _instance_mapper
        self.kwargs = kwargs
        self.values = []
        self.selected_values = []
        self.all_values = []

    def instance_mapper(self, ids):
        if self._instance_mapper:
            return self._instance_mapper(ids)
        return {}

    def add_value(self, fv):
        self.all_values.append(fv)
        if fv.selected:
            self.selected_values.append(fv)
        else:
            self.values.append(fv)

    def get_value(self, name):
        for filter_value in self.all_values:
            if filter_value.value == name:
                return filter_value

    def apply(self, query, params):
        query = super(FacetFilter, self).apply(query, params)
        local_params = LocalParams({'ex': self.name})
        local_params.update(self.local_params)
        return query.facet_field(
            self.name, _local_params=local_params,
            _instance_mapper=self.instance_mapper, **self.kwargs)

    def process_results(self, results, params):
        for facet in results.facet_fields:
            if facet.local_params.get('ex') == self.name:
                values = params.get(self.name, [])
                for fv in facet.values:
                    selected = fv.value in values
                    self.add_value(self.filter_value_cls(self.name, fv, selected))

class FacetQueryFilterValue(object):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.fq = X(*args, **kwargs)
        self.facet_query = None
        self.selected = False

    @property
    def count(self):
        return self.facet_query.count
                    
class FacetQueryFilter(Filter):
    def __init__(self, name, *args):
        super(FacetQueryFilter, self).__init__(name)
        self.filter_values = args

    def get_value(self, name):
        for filter_value in self.filter_values:
            if filter_value.name == name:
                return filter_value

    def _make_x(self, op, value):
        for filter_value in self.filter_values:
            if filter_value.name == value:
                return filter_value.fq

    def _key_for_filter_value(self, filter_value):
        return '%s__%s' % (self.name, filter_value.name)
                           
    def apply(self, query, params):
        query = super(FacetQueryFilter, self).apply(query, params)
        for filter_value in self.filter_values:
            query = query.facet_query(
                filter_value.fq,
                _local_params=[
                    ('ex', self.name),
                    ('key', self._key_for_filter_value(filter_value))])
        return query

    def process_results(self, results, params):
        for filter_value in self.filter_values:
            for facet_query in results.facet_queries:
                if facet_query.local_params.get('key') == self._key_for_filter_value(filter_value):
                    filter_value.facet_query = facet_query
                    filter_value.selected = filter_value.name in params.get(self.name, [])

class RangeFilter(Filter):
    fq_connector = X.AND
    
    def __init__(self, name):
        super(RangeFilter, self).__init__(name)
        self.from_value = None
        self.to_value = None
        self.stats = None
        self.min = None
        self.max = None
    
    def apply(self, query, params):
        for op, v in self._filter_and_split_params(params):
            if op == 'gte':
                self.from_value = v
            if op == 'lte':
                self.to_value = v
        
        query = super(RangeFilter, self).apply(query, params)
        return query

    def process_results(self, results, params):
        stats_query = results.searcher.search(results.query._q)
        stats_query._fq = deepcopy(results.query._fq)
        stats_query = stats_query.stats(self.name).limit(0)
        self.stats = stats_query.results.get_stats_field(self.name)
        self.min = self.stats.min
        self.max = self.stats.max

class OrderingFilter(BaseFilter):
    def __init__(self, order_param='sort', ordering_settings=None, default_order=None):
        self.name = order_param
        self.ordering_settings = []
        self.order_value = default_order
        if ordering_settings:
            for i, (title, values, help_text) in enumerate(ordering_settings):
                if isinstance(values, basestring):
                    self.ordering_settings.append({'title': title,
                                                   'values': ((values,), None),
                                                   'aliases': (values, None),
                                                   'help_text': help_text})
                    if i == 0:
                        self.order_value = self.order_value or values
                else:
                    if len(values) == 1:
                        self.ordering_settings.append({'title': title,
                                                       'values': (values[0], None),
                                                       'aliases': (values[0][0], None),
                                                       'help_text': help_text})
                        if i == 0:
                            self.order_value = self.order_value or values[0][0]
                    elif len(values) == 2:
                        self.ordering_settings.append({'title': title,
                                                       'values': (values[0], values[1]),
                                                       'aliases': (values[0][0], values[1][0]),
                                                       'help_text': help_text})
                        if i == 0:
                            self.order_value = self.order_value or values[0][0]

    def apply(self, query, params):
        self.order_value = params.get(self.name, self.order_value)

        if self.ordering_settings:
            for order_setting in self.ordering_settings:
                if self.order_value in order_setting['aliases']:
                    return query.order_by(*order_setting['values'][order_setting['aliases'].index(self.order_value)])
            return query
        else:
            return query.order_by(self.order_value)
