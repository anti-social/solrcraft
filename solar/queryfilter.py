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
    'exact': lambda f, v: X(**{'%s__exact' % f: v}),
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

    def add_ordering(self, ordering_filter):
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

    def __init__(self, name, field=None, select_multiple=True):
        super(Filter, self).__init__(name)
        self.field = field or self.name
        self.select_multiple = select_multiple

    
    def _filter_query(self, query, fqs):
        if fqs:
            if self.select_multiple:
                return query.filter(*[fq[0] for fq in fqs], _op=self.fq_connector,
                                    _local_params={'tag': self.name})
            else:
                local_params = LocalParams({'tag': self.name})
                local_params.update(fqs[-1][1])
                return query.filter(fqs[-1][0],
                                    _local_params=local_params)
        return query

    def _make_x(self, op, v):
        op_func = OPERATORS.get(op)
        if op_func:
            return op_func(self.field, v), None
        
    def apply(self, query, params):
        fqs = []
        for op, v in self._filter_and_split_params(params):
            x, local_params = self._make_x(op, v)
            # if x:
            fqs.append((x, local_params))
        return self._filter_query(query, fqs)

    def process_results(self, results, params):
        pass

class FacetFilterValue(object):
    def __init__(self, filter_name, facet_value, selected, title=None, **kwargs):
        self.filter_name = filter_name
        self.facet_value = facet_value
        self.selected = selected
        self._title = title
        self.opts = kwargs

    def __unicode__(self):
        return unicode(self.title)

    @property
    def title(self):
        if self._title:
            return self._title
        elif self.instance:
            return unicode(self.instance)
        return self.value

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
    
    def __init__(self, name, field=None, filter_value_cls=None,
                 _local_params=None, _instance_mapper=None,
                 select_multiple=True, **kwargs):
        super(FacetFilter, self).__init__(name, field=field,
                                          select_multiple=select_multiple)
        self.filter_value_cls = filter_value_cls or self.filter_value_cls
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

    def get_value(self, value):
        for filter_value in self.all_values:
            if filter_value.value == value:
                return filter_value

    def apply(self, query, params):
        query = super(FacetFilter, self).apply(query, params)
        local_params = LocalParams({'ex': self.name})
        local_params.update(self.local_params)
        query = query.facet_field(
            self.field, _local_params=local_params,
            _instance_mapper=self.instance_mapper, **self.kwargs)
        return query

    def process_results(self, results, params):
        self.values = []
        self.all_values = []
        self.selected_values = []
        selected_values = set(params.get(self.name, []))
        for facet in results.facet_fields:
            if facet.local_params.get('ex') == self.name:
                for fv in facet.values:
                    selected = fv.value in selected_values
                    self.add_value(self.filter_value_cls(self.name, fv, selected))
                break

class FacetQueryFilterValue(object):
    def __init__(self, name, fq, _local_params=None, title=None, **kwargs):
        self.filter_name = None
        self.value = name
        self.local_params = LocalParams(_local_params)
        self.fq = fq
        self.title = title
        self.opts = kwargs
        self.facet_query = None
        self.selected = False

    def __unicode__(self):
        return unicode(self.title)
                    
    @property
    def _key(self):
        return '%s__%s' % (self.filter_name, self.value)

    @property
    def count(self):
        return self.facet_query.count

class FacetQueryFilter(Filter):
    def __init__(self, name, *args, **kwargs):
        super(FacetQueryFilter, self).__init__(name, kwargs.get('select_multiple'))
        self.filter_values = args
        for fv in self.filter_values:
            fv.filter_name = self.name

    @property
    def all_values(self):
        return self.filter_values

    @property
    def selected_values(self):
        return [fv for fv in self.filter_values if fv.selected]

    @property
    def values(self):
        return [fv for fv in self.filter_values if not fv.selected]

    def get_value(self, value):
        for filter_value in self.filter_values:
            if filter_value.value == value:
                return filter_value

    def _make_x(self, op, value):
        for filter_value in self.filter_values:
            if filter_value.value == value:
                return filter_value.fq, filter_value.local_params
                           
    def apply(self, query, params):
        query = super(FacetQueryFilter, self).apply(query, params)
        for filter_value in self.filter_values:
            local_params = LocalParams({'key': filter_value._key, 'ex': self.name})
            local_params.update(filter_value.local_params)
            query = query.facet_query(
                filter_value.fq,
                _local_params=local_params)
        return query

    def process_results(self, results, params):
        for filter_value in self.filter_values:
            for facet_query in results.facet_queries:
                if facet_query.local_params.get('key') == filter_value._key:
                    filter_value.facet_query = facet_query
                    filter_value.selected = filter_value.value in params.get(self.name, [])

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

class OrderingValue(object):
    ASC = 'asc'
    DESC = 'desc'
    
    def __init__(self, value, fields, title=None, **kwargs):
        self.value = value.strip()
        if self.value.startswith('-'):
            self.direction = self.DESC
        else:
            self.direction = self.ASC
        if isinstance(fields, basestring):
            self.fields = [fields]
        else:
            self.fields = fields
        self.title = title
        self.opts = kwargs
        self.selected = False

    def __unicode__(self):
        return unicode(self.title)

    @property
    def asc(self):
        return self.direction == self.ASC

    @property
    def desc(self):
        return self.direction == self.DESC

    def apply(self, query, params):
        return query.order_by(*self.fields)
        
class OrderingFilter(BaseFilter):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.values = args
        self.default_value = self.get_value(kwargs.get('default')) or self.values[0]
        
    def get_value(self, value):
        for ordering_value in self.values:
            if ordering_value.value == value:
                return ordering_value

    @property
    def selected_value(self):
        for ordering_value in self.values:
            if ordering_value.selected:
                return ordering_value
        
    def apply(self, query, params):
        ordering_value = None
        orderlist = params.get(self.name)
        if orderlist:
            ordering_value = self.get_value(orderlist[0].strip())

        if not ordering_value:
            ordering_value = self.default_value
        
        ordering_value.selected = True
        return ordering_value.apply(query, params)
