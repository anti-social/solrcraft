from facets import OPERATORS


class QueryFilter(object):
    def __init__(self, *filters):
        self.filters = []
        self.filter_names = []
        self.ordering_filter = None
        for f in filters:
            self.add_filter(f)

    def apply(self, query, params, exclude=None):
        for filter in self.filters:
            if exclude is None or filter.name not in exclude:
                query = filter.apply(query, params)
        if self.ordering_filter:
            query = self.ordering_filter.apply(query, params)
        return query

    def add_filter(self, filter):
        if isinstance(filter, basestring):
            filter = Filter(filter)
        if filter.name in self.filter_names:
            return
        self.filters.append(filter)
        self.filter_names.append(filter.name)

    def add_ordering(self, order_param='sort', ordering_settings=None):
        ordering_filter = OrderingFilter(order_param, ordering_settings)
        if self.ordering_filter is None:
            self.ordering_filter = ordering_filter
        

class BaseFilter(object):
    def __init__(self, name):
        self.name = name

    def _parse_params(self, params):
        if hasattr(params, 'getall'):
            # Webob
            items = []
            for p, v in params.items():
                ops = p.split('__')
                name = ops[0]
                ops = ops[1:]
                if name == self.name:
                    items.append((ops, v))
            return items
        elif hasattr(params, 'getlist'):
            # Django
            return params.getlist(self.name)
        elif self.name in params:
            # dict
            value = params[self.name]
            if isinstance(value, (list, tuple)):
                return value
            return [value]
        return []

class Filter(BaseFilter):
    def apply(self, query, params):
        # print self.name
        x = []
        for ops, v in self._parse_params(params):
            op = OPERATORS.get(ops[0]) if ops else OPERATORS['exact']
            if op:
                _x = op(self.name, v)
                if _x:
                    x.append(_x)
        if x:
            return query.filter(*x, _op='OR')
        return query

class OrderingFilter(BaseFilter):
    def __init__(self, order_param='sort', ordering_settings=None, default_order=None):
        self.name = order_param
        self.ordering_settings = []
        self.order_value = default_order
        if ordering_settings:
            for i, (title, values) in enumerate(ordering_settings):
                if isinstance(values, basestring):
                    self.ordering_settings.append({'title': title, 'values': ((values,), None), 'aliases': (values, None)})
                    if i == 0:
                        self.order_value = self.order_value or values
                else:
                    if len(values) == 1:
                        self.ordering_settings.append({'title': title, 'values': (values[0], None), 'aliases': (values[0][0], None)})
                        if i == 0:
                            self.order_value = self.order_value or values[0][0]
                    elif len(values) == 2:
                        self.ordering_settings.append({'title': title, 'values': (values[0], values[1]), 'aliases': (values[0][0], values[1][0])})
                        if i == 0:
                            self.order_value = self.order_value or ordering_values[0][0]

    def apply(self, query, params):
        self.order_value = params.get(self.name, self.order_value)
        if self.ordering_settings:
            for order_setting in self.ordering_settings:
                if self.order_value in order_setting['aliases']:
                    return query.order_by(*order_setting['values'][order_setting['aliases'].index(self.order_value)])
            return query
        else:
            return query.order_by(self.order_value)
