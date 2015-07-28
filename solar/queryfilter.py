from __future__ import unicode_literals

import math
import weakref
from copy import deepcopy
from itertools import starmap
from functools import partial
from collections import defaultdict

from .util import X, LocalParams, make_fq, process_value, wrap_list, _pop_from_kwargs
from .types import instantiate, Integer, Long, Float 
from .facets import FacetValue
from .compat import PY2, force_unicode, zip_longest


def exact_op(f, v):
    if v is None:
        return X(**{'{}__isnull'.format(f): True})
    else:
        return X(**{'{}__exact'.format(f): v})

def isnull_op(f, v):
    if v == '1':
        return X(**{'{}__isnull'.format(f): True})
    elif v == '0':
        return X(**{'{}__isnull'.format(f): False})
    return


OPERATORS = {
    'exact': exact_op,
    'gte': lambda f, v: X(**{'{}__gte'.format(f): v}),
    'gt': lambda f, v: X(**{'{}__gt'.format(f): v}),
    'lte': lambda f, v: X(**{'{}__lte'.format(f): v}),
    'lt': lambda f, v: X(**{'{}__lt'.format(f): v}),
    'isnull': isnull_op,
}


def to_float(value, type=None):
    type = type or Float()
    v = type.to_python(value)
    if math.isnan(v) or math.isinf(v):
        raise ValueError('NaN or Inf is not supported')
    return v


def to_int(value, type=None):
    type = type or Integer()
    v = type.to_python(value)
    if Integer.MIN_VALUE < v < Integer.MAX_VALUE:
        return v
    raise ValueError(
        'Integer value must be between %s and %s' % (
            Integer.MIN_VALUE, Integer.MAX_VALUE
        )
    )


def to_long(value, type=None):
    type = type or Long()
    v = type.to_python(value)
    if Long.MIN_VALUE < v < Long.MAX_VALUE:
        return v
    raise ValueError(
        'Long value must be between %s and %s' % (
            Long.MIN_VALUE, Long.MAX_VALUE
        )
    )
    

class BaseCodec(object):
    def decode_value(self, value, typelist=None):
        raise NotImplementedError()

    def decode(self, params, types=None):
        raise NotImplementedError()

    def encode_value(self, value, typelist=None):
        raise NotImplementedError()

    def encode(self, values, types=None):
        raise NotImplementedError()


class SimpleCodec(BaseCodec):
    DEFAULT_OPERATOR = 'exact'
    DEFAULT_OP_SEP = '__'
    DEFAULT_VAL_SEP = ':'

    PROCESSOR_FACTORIES = {
        Float: lambda type: partial(to_float, type=type),
        Integer: lambda type: partial(to_int, type=type),
        Long: lambda type: partial(to_long, type=type),
    }

    def _normalize_params(self, params):
        if hasattr(params, 'getall'):
            # Webob
            return params.dict_of_lists()
        if hasattr(params, 'getlist'):
            # Django
            return dict(params.lists())
        if isinstance(params, (list, tuple)):
            # list, tuple
            new_params = defaultdict(list)
            for p, v in params:
                new_params[p].extend(v)
            return new_params
        if isinstance(params, dict):
            # dict
            return params

        raise TypeError("'params' must be Webob MultiDict, "
                        "Django QueryDict, list, tuple or dict")

    def decode_value(self, value, typelist=None):
        typelist = [instantiate(t) for t in wrap_list(typelist or [])]
        raw_values = force_unicode(value).split(self.DEFAULT_VAL_SEP)
        decoded_values = []
        for v, type in zip_longest(raw_values, typelist):
            if type is None:
                to_python = force_unicode
            else:
                to_python_factory = self.PROCESSOR_FACTORIES.get(type.__class__)
                if to_python_factory:
                    to_python = to_python_factory(type)
                else:
                    to_python = type.to_python
            if v is None:
                continue
            try:
                # null is special value
                if v == 'null':
                    decoded_values.append(None)
                else:
                    decoded_values.append(to_python(v))
            except ValueError:
                break
        return decoded_values
        
    
    def decode(self, params, types=None):
        """Returns {name: [(operator1, [value1]), (operator2, [value21, value22])], ...}"""
        params = self._normalize_params(params)
        types = types or {}
        data = defaultdict(list)
        # sort is needed to pass tests
        for p, v in sorted(starmap(lambda p, v: (force_unicode(p), v), params.items())):
            ops = p.split(self.DEFAULT_OP_SEP)
            name = ops[0]
            if len(ops) == 1:
                op = self.DEFAULT_OPERATOR
            else:
                op = self.DEFAULT_OP_SEP.join(ops[1:])

            for w in wrap_list(v):
                decoded_values = self.decode_value(w, types.get(name))
                if decoded_values:
                    data[name].append((op, decoded_values))

        return dict(data)

    def _encode_value(self, value):
        if value is None:
            return 'null'
        if value is True:
            return 'true'
        if value is False:
            return 'false'
        return force_unicode(value)
        
    def encode_value(self, value, typelist=None):
        return self.DEFAULT_VAL_SEP.join(self._encode_value(v) for v in wrap_list(value))

    def encode(self, values, types=None):
        params = defaultdict(list)
        for name, value in values:
            params[name].append(self.encode_value(value))
        return dict(params)


class QueryFilter(object):
    def __init__(self, *filters, **kwargs):
        self.filters = []
        self.ordering_filter = None
        for f in filters:
            self.add_filter(f)

        self.codec = kwargs.get('codec') or SimpleCodec()
        self._params = []
        self._results = None

    def get_filter_types(self):
        types = {}
        for filter in self.filters:
            types[filter.name] = filter.get_types()
        return types
            
    def apply(self, query, params, exclude=None):
        types = self.get_filter_types()
        self._params = self.codec.decode(params, types)
        for filter in self.filters:
            if exclude is None or filter.name not in exclude:
                query = filter.apply(query, self._params.get(filter.name, []))

        if self.ordering_filter:
            query = self.ordering_filter.apply(
                query, self._params.get(self.ordering_filter.name, [])
            )

        return query

    def add_filter(self, filter):
        if not isinstance(filter, BaseFilter):
            filter = Filter(filter)
        filter._qf = weakref.proxy(self)
        self.filters.append(filter)

    def get_filter(self, name):
        for f in self.filters:
            if f.name == name:
                return f

    def add_ordering(self, ordering_filter):
        self.ordering_filter = ordering_filter

    def process_results(self, results):
        self._results = results
        for f in self.filters:
            f.process_results(self._results, self._params.get(f.name, []))


class BaseFilter(object):
    allowed_operators = None

    def __init__(self, name, type=None):
        self.name = name
        self.type = instantiate(type)
        self._qf = None

    def _filter_values(self, params):
        values = []
        for op, vals in params:
            if not self.allowed_operators or op in self.allowed_operators:
                values += vals
        return values

    def get_types(self):
        return [self.type]

    def process_results(self, results, params):
        raise NotImplementedError()


class Filter(BaseFilter):
    fq_connector = X.OR

    def __init__(self, name, field=None, type=None, local_params=None,
                 select_multiple=True, default=None, **kwargs):
        super(Filter, self).__init__(name, type=type)
        self.field = field or name
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
        for op, v in params:
            if not self.allowed_operators or op in self.allowed_operators:
                fqs.append(self._make_x(op, v[0]))
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
        if self.filter.get_title:
            return self.filter.get_title(self)
        if self.instance:
            return force_unicode(self.instance)
        return force_unicode(self.value)

    @property
    def filter_name(self):
        return self.filter.name

    @property
    def filter_value(self):
        return self.filter._qf.codec.encode_value(self.value)

    @property
    def select_multiple(self):
        return self.filter.select_multiple
        
    @property
    def value(self):
        return self.facet_value.value

    @property
    def count(self):
        return self.facet_value.count
    
    @property
    def count_text(self):
        if not self.count:
            return ''
        if not self.selected \
           and self.filter.select_multiple \
           and self.filter.selected_values \
           and self.filter.fq_connector == X.OR:
            return '+{}'.format(self.count)
        return '{}'.format(self.count)
    count_plus = count_text

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
    available_operators = ('exact',)
    instance_mapper = None
    get_title = None
    
    def __init__(self, name, field=None, filter_value_cls=None, type=None,
                 local_params=None, instance_mapper=None, get_title=None,
                 select_multiple=True, ensure_selected_values=False, **kwargs):
        super(FacetFilter, self).__init__(name, field, type=type,
                                          select_multiple=select_multiple, **kwargs)
        self.filter_value_cls = filter_value_cls or self.filter_value_cls
        self.local_params = LocalParams(kwargs.pop('_local_params', local_params))
        instance_mapper = kwargs.pop('_instance_mapper', instance_mapper)
        self.instance_mapper = instance_mapper or self.instance_mapper
        self.get_title = get_title or self.get_title
        self.ensure_selected_values = ensure_selected_values
        self.kwargs = kwargs
        self.values = []
        self.selected_values = []
        self.all_values = []

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
        param_values = self._filter_values(params)
        facet = results.get_facet_field(self.name)
        processed_facet_values = set()
        if facet:
            for fv in facet.values:
                selected = fv.value in param_values
                self.add_value(self.filter_value_cls(self, fv, selected))
                processed_facet_values.add(fv.value)
        if self.ensure_selected_values:
            for v in param_values:
                if v not in processed_facet_values:
                    fv = FacetValue(v, None, facet)
                    facet.values.append(fv)
                    self.add_value(self.filter_value_cls(self, fv, True))


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
    def __init__(self, filter, facet_value, selected, title=None, path_values=None, **kwargs):
        self.filter = weakref.proxy(filter)
        self.facet_value = facet_value
        self.selected = selected
        self._title = title
        self.path_values = path_values or ()
        self.opts = kwargs
        self.pivot = None

    @property
    def filter_name(self):
        return self.filter.root_pivot.name

    @property
    def filter_value(self):
        return self.filter.root_pivot._qf.codec.encode_value(self.path_values)


class FacetPivotFilter(FacetFilter):
    filter_value_cls = FacetPivotFilterValue
    
    def __init__(self, *args, **kwargs):
        super(FacetPivotFilter, self).__init__(*args, **kwargs)
        self.root_pivot = None

    def clone(self):
        return self.__class__(
            self.name, field=self.field, filter_value_cls=self.filter_value_cls,
            type=self.type, ensure_selected_values=self.ensure_selected_values,
            instance_mapper=self.instance_mapper, get_title=self.get_title,
            **self.kwargs
        )


class PivotFilter(BaseFilter, FacetPivotFilterValueMixin):
    allowed_operators = ('exact',)
    fq_connector = X.OR

    def __init__(self, name, *pivots, **kwargs):
        super(PivotFilter, self).__init__(name)
        self._pivots = pivots
        self._pivot_fields = [p.field for p in pivots]
        self._pivot_kwargs = []
        for p in self._pivots:
            kw = p.kwargs.copy()
            kw.update(
                _instance_mapper=p.instance_mapper,
                _type=p.type,
            )
            self._pivot_kwargs.append(kw)
        self.local_params = LocalParams(_pop_from_kwargs(kwargs, 'local_params'))
        self.pivot = self._pivots[0]
        self.pivot.root_pivot = self

    def get_types(self):
        return [p.type for p in self._pivots]

    def apply(self, query, params):
        local_params = LocalParams(self.local_params)
        local_params['key'] = self.name
        local_params.merge({'ex': self.name})
        query = query.facet_pivot(
            *zip(self._pivot_fields, self._pivot_kwargs),
            _local_params=local_params)
        fqs = []
        for op, values in params:
            if op not in self.allowed_operators:
                continue
            op_func = OPERATORS.get(op)
            if op_func:
                x = X()
                for field, v in zip(self._pivot_fields, values):
                    x = x & op_func(field, v)
                fqs.append(x)
        local_params = LocalParams()
        local_params.merge({'tag': self.name})
        if fqs:
            query = query.filter(X(*fqs, _op=self.fq_connector), _local_params=local_params)
        return query

    def process_results(self, results, params):
        selected_values = []
        for op, vals in params:
            if op in self.allowed_operators:
                selected_values.append(vals)
        facet = results.get_facet_pivot(self.name)
        self.process_facet(
            facet,
            self._pivots,
            selected_values,
        )

    def process_facet(self, facet, filters, selected_values, parent_values=None):
        filter = filters[0]
        next_filter = filters[1] if len(filters) >= 2 else None
        rest_filters = filters[2:]
        current_selected_values = [v[0] for v in selected_values if v]
        processed_facet_values = set()
        parent_values = parent_values or ()
        for fv in facet.values:
            path_values = parent_values + (fv.value,)
            selected = fv.value in current_selected_values
            filter_value = filter.filter_value_cls(filter, fv, selected,
                                                   path_values=path_values)
            filter.add_value(filter_value)
            processed_facet_values.add(fv.value)
            if fv.pivot and next_filter:
                filter_value.pivot = next_filter.clone()
                filter_value.pivot.root_pivot = self
                next_selected_values = [v[1:] for v in selected_values
                                        if len(v) >= 2 and fv.value == v[0]]
                self.process_facet(
                    fv.pivot,
                    (filter_value.pivot,) + rest_filters,
                    next_selected_values,
                    path_values,
                )

        if filter.ensure_selected_values:
            for v in current_selected_values:
                if v not in processed_facet_values:
                    fv = FacetValue(v, None, facet)
                    facet.values.append(fv)
                    path_values = parent_values + (fv.value,)
                    filter_value = filter.filter_value_cls(filter, fv, True,
                                                           path_values=path_values)
                    filter.add_value(filter_value)
                
                
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
    def filter_value(self):
        return self.value

    @property
    def count(self):
        return self.facet_query.count

    @property
    def count_text(self):
        if not self.count:
            return ''
        return '{}'.format(self.count)
    count_plus = count_text


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
        selected_values = set()
        for op, vals in params:
            if op == 'exact':
                selected_values.update(vals)
        was_selected = False
        for filter_value in self.filter_values:
            for facet_query in results.facet_queries:
                if facet_query.local_params.get('key') == filter_value._key:
                    filter_value.facet_query = facet_query
                    if filter_value.value in selected_values:
                        filter_value.selected = True
                        was_selected = True
        if not was_selected and self.default:
            self.get_value(self.default).selected = True


class RangeFilter(Filter):
    fq_connector = X.AND
    allowed_operators = ['gte', 'lte']

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
        for op, v in params:
            if op == 'gte':
                self.from_value = v[0]
            if op == 'lte':
                self.to_value = v[0]

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
    allowed_operators = ('exact',)

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
        valuelist = self._filter_values(params)

        if valuelist:
            ordering_value = self.get_value(valuelist[0])

        if not ordering_value:
            ordering_value = self.default_value

        ordering_value.selected = True
        return ordering_value.apply(query, params)
