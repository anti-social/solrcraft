from __future__ import unicode_literals

from copy import deepcopy
from itertools import starmap

from .util import X, LocalParams, make_fq, process_value, _pop_from_kwargs
from .types import instantiate, get_to_python, Boolean
from .compat import force_unicode


DEFAULT_OP_SEP = '__'
DEFAULT_VAL_SEP = ':'

def between_op(f, v):
    v1, v2 = v.split(DEFAULT_VAL_SEP)
    return X(**{'{}__between'.format(f): (v1, v2)})


def isnull_op(f, v):
    if v == '1':
        return X(**{'{}__isnull'.format(f): True})
    elif v == '0':
        return X(**{'{}__isnull'.format(f): False})
    return


OPERATORS = {
    'exact': lambda f, v: X(**{'{}__exact'.format(f): v}),
    'gte': lambda f, v: X(**{'{}__gte'.format(f): v}),
    'gt': lambda f, v: X(**{'{}__gt'.format(f): v}),
    'lte': lambda f, v: X(**{'{}__lte'.format(f): v}),
    'lt': lambda f, v: X(**{'{}__lt'.format(f): v}),
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
        if not isinstance(filter, BaseFilter):
            filter = Filter(filter)
        # if filter.name in self.filter_names:
        #     return
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


class BaseFilter(object):
    available_operators = None

    def __init__(self, name, type=None):
        self.name = name
        self.type = instantiate(type)
        self.to_python = (
            self.type.process_param_value
            if hasattr(self.type, 'process_param_value') else
            (lambda v: v)
        )

    def _filter_and_split_params(self, params):
        """Returns [(operator1, value1), (operator2, value2)]"""
        items = []
        for p, v in sorted(starmap(lambda p, v: (force_unicode(p), v), params.items())):
            ops = p.split(DEFAULT_OP_SEP)
            name = ops[0]
            if len(ops) == 1:
                op = 'exact'
            else:
                op = DEFAULT_OP_SEP.join(ops[1:])

            if (
                self.available_operators is not None
                and op not in self.available_operators
            ):
                continue

            if name == self.name:
                if not isinstance(v, (list, tuple)):
                    v = [v]
                for w in v:
                    try:
                        w = self.to_python(w)
                    except ValueError:
                        continue
                    yield op, w

    def process_results(self, results, params):
        raise NotImplementedError()


class Filter(BaseFilter):
    fq_connector = X.OR

    def __init__(self, name, field=None, type=None, local_params=None,
                 select_multiple=True, default=None, **kwargs):
        super(Filter, self).__init__(name, type=type)
        self.field = field or self.name
        self.local_params = LocalParams(
            kwargs.pop('_local_params', local_params))
        self.select_multiple = select_multiple
        self.default = default
    
    def _filter_query(self, query, fqs):
        if not fqs:
            return query

        if self.select_multiple:
            if self.fq_connector == X.OR:
                local_params = LocalParams(self.local_params)
                local_params.merge({'tag': self.name})
                xs = [x for x, lp in fqs if x]
                if xs:
                    return query.filter(*xs,
                                        _local_params=local_params,
                                        _op=self.fq_connector)
            else:
                for x, lp in fqs:
                    if x:
                        local_params = LocalParams(lp)
                        local_params.merge({'tag': self.name})
                        query = query.filter(x, _local_params=local_params)
                return query
        else:
            x, lp = fqs[-1]
            local_params = LocalParams(lp)
            if x or local_params:
                local_params.merge({'tag': self.name})
                return query.filter(x, _local_params=local_params)

        return query

    def _make_x(self, op, v):
        op_func = OPERATORS.get(op)
        if op_func:
            return op_func(self.field, v), self.local_params
        return None, None
        
    def apply(self, query, params):
        fqs = []
        for op, v in self._filter_and_split_params(params):
            fqs.append(self._make_x(op, v))
        if not fqs and self.default:
            fqs.append(self._make_x('exact', self.default))
        return self._filter_query(query, fqs)

    def process_results(self, results, params):
        pass


class FacetFilterValueMixin(object):
    def __unicode__(self):
        return force_unicode(self.title)

    @property
    def title(self):
        if self._title:
            return self._title
        elif self.instance:
            return force_unicode(self.instance)
        return force_unicode(self.value)

    @property
    def filter_name(self):
        return self.filter.name

    @property
    def select_multiple(self):
        return self.filter.select_multiple
        
    @property
    def value(self):
        return self.facet_value.value

    @property
    def param_value(self):
        return process_value(self.value, safe=True)

    @property
    def count(self):
        return self.facet_value.count
    
    @property
    def count_plus(self):
        if not self.selected \
           and self.filter.select_multiple \
           and self.filter.selected_values \
           and self.filter.fq_connector == X.OR:
            return '+{}'.format(self.facet_value.count)
        return '{}'.format(self.facet_value.count)

    @property
    def instance(self):
        return self.facet_value.instance

    
class FacetFilterValue(FacetFilterValueMixin):
    def __init__(self, filter, facet_value, selected, title=None, **kwargs):
        self.filter = filter
        self.facet_value = facet_value
        self.selected = selected
        self._title = title
        self.opts = kwargs


class FacetFilter(Filter):
    filter_value_cls = FacetFilterValue
    available_operators = ['exact']
    
    def __init__(self, name, field=None, filter_value_cls=None, type=None,
                 local_params=None, instance_mapper=None,
                 select_multiple=True, **kwargs):
        super(FacetFilter, self).__init__(name, field, type=type,
                                          select_multiple=select_multiple)
        self.filter_value_cls = filter_value_cls or self.filter_value_cls
        self.local_params = LocalParams(
            kwargs.pop('_local_params', local_params))
        self._instance_mapper = kwargs.pop('_instance_mapper', instance_mapper)
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
        local_params = LocalParams(self.local_params)
        local_params['key'] = self.name
        local_params.merge({'ex': self.name})
        query = query.facet_field(
            self.field, _local_params=local_params,
            _instance_mapper=self.instance_mapper, _type=self.type,
            **self.kwargs)
        return query

    def process_results(self, results, params):
        self.values = []
        self.all_values = []
        self.selected_values = []
        selected_values = set(self.to_python(v) for v in params.get(self.name, []))
        facet = results.get_facet_field(self.name)
        if facet:
            for fv in facet.values:
                selected = fv.value in selected_values
                self.add_value(
                    self.filter_value_cls(
                        self, fv, selected,
                        select_multiple=self.select_multiple))


class FacetPivotFilterValueMixin(object):
    @property
    def all_values(self):
        return self.pivot.all_values

    @property
    def selected_values(self):
        return self.pivot.selected_values

    @property
    def values(self):
        return self.pivot.values


class FacetPivotFilterValue(FacetFilterValueMixin, FacetPivotFilterValueMixin):
    def __init__(self, filter, facet_values, selected, title=None,
                 **kwargs):
        self.filter = filter
        self.facet_values = facet_values
        self.facet_value = facet_values[-1]
        self.selected = selected
        self._title = title
        self.opts = kwargs
        self.pivot = None

    @property
    def param_value(self):
        return DEFAULT_VAL_SEP.join(process_value(fv.value, safe=True) for fv in self.facet_values)

    @property
    def filter_name(self):
        return self.filter._pivot_filter.name


class FacetPivotFilter(FacetFilter):
    filter_value_cls = FacetPivotFilterValue

    def __init__(self, name, field=None, filter_value_cls=None,
                 type=None, _pivot_filter=None, **kwargs):
        super(FacetPivotFilter, self).__init__(
            name, field=field, filter_value_cls=filter_value_cls,
            type=type, **kwargs)
        self._pivot_filter = _pivot_filter

    def bind(self, pivot_filter):
        return FacetPivotFilter(
            self.name, field=self.field, filter_value_cls=self.filter_value_cls,
            type=self.type, _pivot_filter=pivot_filter, **self.kwargs)
        
    def process_facet(self, facet, pivot_filters, selected_values, facet_values):
        cur_selected_values = set(self.to_python(v[0]) for v in selected_values)

        for fv in facet.values:
            selected = fv.value in cur_selected_values
            filter_value = self.filter_value_cls(
                self, facet_values + [fv], selected)
            self.add_value(filter_value)
            if fv.pivot:
                pivot_filter = pivot_filters[0].bind(self._pivot_filter)
                pivot_filter.process_facet(
                    fv.pivot,
                    pivot_filters[1:],
                    [v[1:] for v in selected_values if len(v) > 1 and fv.value == v[0]],
                    facet_values + [fv])
                filter_value.pivot = pivot_filter
                

class PivotFilter(BaseFilter, FacetPivotFilterValueMixin):
    available_operators = ['exact']
    fq_connector = X.OR

    def __init__(self, name, *pivots, **kwargs):
        super(PivotFilter, self).__init__(name)
        self._pivots = pivots
        self._pivot_fields = [p.field for p in pivots]
        self._pivot_kwargs = []
        for p in self._pivots:
            kw = p.kwargs.copy()
            kw.update(
                _instance_mapper=p._instance_mapper,
                _type=p.type,
            )
            self._pivot_kwargs.append(kw)
        self.local_params = LocalParams(_pop_from_kwargs(kwargs, 'local_params'))
        self.pivot = self._pivots[0].bind(self)

    def apply(self, query, params):
        local_params = LocalParams(self.local_params)
        local_params['key'] = self.name
        local_params.merge({'ex': self.name})
        query = query.facet_pivot(
            *zip(self._pivot_fields, self._pivot_kwargs),
            _local_params=local_params)
        fqs = []
        for op, value in self._filter_and_split_params(params):
            value = force_unicode(value)
            op_func = OPERATORS.get(op)
            if op_func:
                x = X()
                for field, v in zip(self._pivot_fields, value.split(DEFAULT_VAL_SEP)):
                    x = x & op_func(field, v)
                fqs.append(x)
        local_params = LocalParams()
        local_params.merge({'tag': self.name})
        if fqs:
            query = query.filter(X(*fqs, _op=self.fq_connector), _local_params=local_params)
        return query

    def process_results(self, results, params):
        selected_values = [force_unicode(v).split(DEFAULT_VAL_SEP)
                           for v in params.get(self.name, [])]
        self.pivot.process_facet(
            results.get_facet_pivot(self.name),
            self._pivots[1:],
            selected_values,
            [])

                
class FacetQueryFilterValue(object):
    def __init__(self, name, fq, local_params=None, title=None, **kwargs):
        self.filter_name = None
        self.value = name
        self.local_params = LocalParams(
            kwargs.pop('_local_params', local_params))
        self.fq = fq
        self.title = title
        self.opts = kwargs
        self.facet_query = None
        self.selected = False

    def __unicode__(self):
        return unicode(self.title)
                    
    @property
    def _key(self):
        return '{}__{}'.format(self.filter_name, self.value)

    @property
    def count(self):
        return self.facet_query.count


class FacetQueryFilter(Filter):
    def __init__(self, name, *args, **kwargs):
        super(FacetQueryFilter, self).__init__(name, **kwargs)
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
        return None, None
                           
    def apply(self, query, params):
        query = super(FacetQueryFilter, self).apply(query, params)
        for filter_value in self.filter_values:
            local_params = LocalParams(filter_value.local_params)
            local_params['key'] = filter_value._key
            local_params.merge({'ex': self.name})
            query = query.facet_query(
                filter_value.fq,
                _local_params=local_params)
        return query

    def process_results(self, results, params):
        was_selected = False
        for filter_value in self.filter_values:
            for facet_query in results.facet_queries:
                if facet_query.local_params.get('key') == filter_value._key:
                    filter_value.facet_query = facet_query
                    if filter_value.value in params.get(self.name, []):
                        filter_value.selected = True
                        was_selected = True
        if not was_selected and self.default:
            self.get_value(self.default).selected = True


class RangeFilter(Filter):
    fq_connector = X.AND
    available_operators = ['gte', 'lte']

    def __init__(self, name, field=None, type=None,
                 gather_stats=False, exclude_filter=True, **kwargs):
        super(RangeFilter, self).__init__(name, field, type=type, **kwargs)
        self.gather_stats = gather_stats
        self.exclude_filter = exclude_filter
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
        if self.gather_stats and not self.exclude_filter:
            query = query.stats(self.field)
        return query

    def process_results(self, results, params):
        if self.gather_stats:
            if self.exclude_filter:
                stats_query = results.searcher.search(results.query._q)
                stats_query._fq = deepcopy(results.query._fq)
                stats_query = stats_query.stats(self.field).limit(0)
                self.process_stats(
                    stats_query.results.get_stats_field(self.field))
            else:
                self.process_stats(results.get_stats_field(self.field))

    def process_stats(self, stats):
        self.stats = stats
        self.min = self.stats.min
        self.max = self.stats.max


class OrderingValue(object):
    ASC = 'asc'
    DESC = 'desc'
    
    def __init__(self, value, fields, title=None, **kwargs):
        self.value = value
        if self.value.startswith('-'):
            self.direction = self.DESC
        else:
            self.direction = self.ASC
        if not isinstance(fields, (list, tuple)):
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
        valuelist = params.get(self.name, [])
        if not isinstance(valuelist, (list, tuple)):
            valuelist = [valuelist]

        if valuelist:
            ordering_value = self.get_value(valuelist[0])

        if not ordering_value:
            ordering_value = self.default_value
        
        ordering_value.selected = True
        return ordering_value.apply(query, params)
