# -*- encoding: utf-8 -*-
from __future__ import unicode_literals


from .util import process_value, maybe_wrap_literal
from .compat import int_types


# http://wiki.apache.org/solr/FunctionQuery/
SOLR_FUNCTIONS = [
    'constant', 'literal', 'field', 'ord', 'rord', 'sum', 'sub', 'product',
    'div', 'pow', 'abs', 'log', 'sqrt', 'map', 'scale', 'query', 'linear',
    'recip', 'max', 'min', 'ms',
    # Math
    'rad', 'deg', 'sqrt', 'cbrt', 'ln', 'exp', 'sin', 'cos', 'tan',
    'asin', 'acos', 'atan', 'sinh', 'cosh', 'tanh', 'ceil', 'floor', 'rint',
    'hypo', 'atan2', 'pi', 'e',
    # Relevance - Solr 4.0
    'docfreq', 'termfreq', 'totaltermfreq', 'sumtotaltermfreq', 'idf', 'tf',
    'norm', 'maxdoc', 'numdocs',
    # Boolean - Solr 4.0
    'true', 'false', 'exists', ('if', 'if_'), ('def', 'def_'), ('not', 'not_'),
    ('and', 'and_'), ('or', 'or_'),
    # Distance
    'dist', 'sqedist', 'geodist', 'hsin', 'ghhsin', 'geohash', 'strdist', 'top',
    ]

class Function(object):
    def __init__(self, name, *args, **kwargs):
        self.name = name
        self.args = args
        self.weight = kwargs.get('weight')

    def __mul__(self, weight):
        if not isinstance(weight, int_types + (float,)):
            raise TypeError("Weight must be 'int' or 'float', "
                            "not '{}'".format(type(weight)))
        return type(self)(self.name, *self.args, weight=weight)

    def __add__(self, other):
        if not isinstance(other, Function):
            raise TypeError("Only 'Function' object can be added, "
                            "not '{}'".format(type(other)))
        return FunctionList([self, other])
    
    def __str__(self):
        return '{}({}){}'.format(
            self.name,
            ','.join(maybe_wrap_literal(process_value(a)) for a in self.args),
            '^{}'.format(self.weight) if self.weight else '',
        )


class FunctionList(object):
    def __init__(self, functions):
        self.functions = list(functions)

    def __add__(self, func):
        if not isinstance(func, Function):
            raise TypeError("Only 'Function' object can be added, "
                            "not '{}'".format(type(func)))
        return type(self)(self.functions + [func])

    def __str__(self):
        return ' '.join(process_value(f) for f in self.functions)


def _implements_solr_functions(cls):
    def set_func(func_name, name):
        setattr(cls, func_name,
                property(lambda self: cls(name)))
    
    for func_name in SOLR_FUNCTIONS:
        if isinstance(func_name, tuple):
            name, func_name  = func_name
        else:
            name, func_name = func_name, func_name
        set_func(func_name, name)
    return cls
    

@_implements_solr_functions
class _FunctionGenerator(object):
    def __init__(self, name=None):
        self.__name = name

    def __getattr__(self, name):
        if name.endswith('_'):
            name = name[0:-1]
        return _FunctionGenerator(name)

    def __call__(self, *args, **kwargs):
        return Function(self.__name, *args, **kwargs)
