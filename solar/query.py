from __future__ import unicode_literals

import re
import sys
import logging
import warnings
from copy import copy, deepcopy
from itertools import chain, starmap

from .pysolr import SolrError

from .compat import PY2, force_unicode, implements_to_string, reraise
from .result import SolrResults
from .stats import Stats
from .facets import FacetField, FacetRange, FacetQuery, FacetPivot
from .grouped import GroupedField, GroupedQuery, GroupedFunc
from .util import SafeUnicode, safe_solr_input, X, LocalParams, make_fq, make_q


log = logging.getLogger(__name__)


class SolrParameterSetter(object):
    def __init__(self, solr_query, param_name):
        self.solr_query = solr_query
        self.param_name = param_name

    def __call__(self, *args):
        solr_query = self.solr_query._clone()
        if len(args) == 1:
            solr_query._params[self.param_name] = args[0]
        else:
            solr_query._params[self.param_name] = list(args)
        return solr_query


@implements_to_string
class SolrQuery(object):
    def __init__(self, searcher, q, *args, **kwargs):
        self.searcher = searcher

        self._q_local_params = LocalParams(kwargs.pop('_local_params', None))
        self._q = q
        self._q_args = args
        self._q_kwargs = kwargs
        self._fq = []
        self._groupeds = []
        self._facet_fields = []
        self._facet_queries = []
        self._facet_dates = []
        self._facet_ranges = []
        self._facet_pivots = []
        self._stats_fields = []
        self._params = {}

        self._instance_mapper = None
        self._db_query = None

        self._iter_instances = False

        self._result_cache = None

    def __str__(self):
        def simple_quote(s):
            return force_unicode(s) \
                .replace('%', '%25') \
                .replace('&', '%26') \
                .replace('+', '%2B')
        
        params = []
        params.append(('q', self._make_q()))
        params.extend(self._prepare_params().items())
        parts = []
        for p, v in params:
            if not isinstance(v, (list, tuple)):
                v = [v]
            for w in v:
                parts.append('{}={}'.format(simple_quote(p), simple_quote(w)))
        return '&'.join(parts)

    def __len__(self):
        results = self._fetch_results()
        return len(results)

    def __iter__(self):
        results = self._fetch_results()
        if self._iter_instances:
            return iter(doc.instance for doc in results if doc.instance)
        else:
            return iter(results)

    def __getitem__(self, k):
        if not isinstance(k, (slice, int)):
            raise TypeError

        if self._result_cache is not None:
            docs = self._result_cache.docs[k]
        else:
            if isinstance(k, slice):
                start, stop = k.start, k.stop
                clone = self._clone()
                if start is not None:
                    clone._params['start'] = start
                if stop is not None:
                    clone._params['rows'] = stop - start
                return clone
            else:
                docs = self._fetch_results().docs[k]
        if self._iter_instances:
            return [doc.instance for doc in docs if doc.instance]
        return docs

    def __getattr__(self, attr_name):
        # fix IPython
        if attr_name in ('trait_names', '_getAttributeNames'):
            raise AttributeError
        if attr_name.startswith('_'):
            raise AttributeError
        return SolrParameterSetter(self, attr_name.replace('_', '.'))

    def _fetch_results(self, only_count=False):
        if self._result_cache is None:
            self._result_cache = self._do_search(only_count)
        return self._result_cache

    def _prepare_params(self, only_count=False):
        params = deepcopy(self._params)
        self._modify_params(params, only_count=only_count)
        prepared_params = {}
        for key, val in params.items():
            if isinstance(val, tuple):
                prepared_params[key] = ','.join(val)
            elif isinstance(val, bool):
                prepared_params[key] = force_unicode(val).lower()
            elif val is None:
                pass
            else:
                prepared_params[key] = val
        return prepared_params

    def _modify_params(self, params, only_count=False):
        def merge_params(params, merged_params):
            for p, v in merged_params.items():
                if isinstance(v, (list, tuple)):
                    params.setdefault(p, []).extend(v)
                else:
                    params[p] = v
            return params
        
        if only_count:
            params['rows'] = 0
        if self._fq:
            params['fq'] = [make_fq(x, local_params)
                            for x, local_params in self._fq]
        if 'qf' in params:
            params['qf'] = ' '.join(
                starmap('{}^{}'.format,
                        filter(lambda fw: fw[1], params['qf'])))
        if 'fl' not in params:
            params['fl'] = ('*', 'score')

        if params.get('defType') == 'dismax' \
                and (self._q is None or isinstance(self._q, X) or self._q_args or self._q_kwargs):
            # should we turn off dismax query parser in this cases?
            # or maybe we should use q.alt parameter instead of q?
            # params.pop('defType', None)
            warnings.warn('DisMax query parser does not support q=*:* or q=field:text')

        for grouped in self._groupeds:
            params = merge_params(params, grouped.get_params())

        for facet in chain(self._facet_fields, self._facet_queries,
                           self._facet_dates, self._facet_ranges,
                           self._facet_pivots):
            params = merge_params(params, facet.get_params())

        for stats in self._stats_fields:
            params = merge_params(params, stats.get_params())

    def _make_q(self):
        return make_q(self._q, self._q_local_params, *self._q_args, **self._q_kwargs)

    def _do_search(self, only_count=False):
        params = self._prepare_params(only_count=only_count)
        raw_results = self.searcher.select(self._make_q(), **params)
        return SolrResults(self, raw_results)
            
    def _clone(self, cls=None):
        cls = cls or self.__class__
        clone = cls(self.searcher, self._q, *self._q_args, **self._q_kwargs)
        clone._q_local_params = deepcopy(self._q_local_params)
        clone._fq = deepcopy(self._fq)
        clone._groupeds = deepcopy(self._groupeds)
        clone._facet_fields = deepcopy(self._facet_fields)
        clone._facet_queries = deepcopy(self._facet_queries)
        clone._facet_ranges = deepcopy(self._facet_ranges)
        clone._facet_dates = deepcopy(self._facet_dates)
        clone._facet_pivots = deepcopy(self._facet_pivots)
        clone._stats_fields = deepcopy(self._stats_fields)
        clone._params = deepcopy(self._params)
        clone._instance_mapper = self._instance_mapper
        clone._db_query = self._db_query
        clone._iter_instances = self._iter_instances
        return clone
    clone = _clone

    def _add_component(self, name, **kwargs):
        self._params[name] = True
        for p, v in kwargs.items():
            if v is not None:
                self._params['{}.{}'.format(name, p)] = v
        
    def _remove_component(self, name):
        for key in list(self._params.keys()):
            if key == name or key.startswith('{}.'.format(name)):
                del self._params[key]

    # Public methods

    @property
    def results(self):
        try:
            return self._fetch_results()
        except AttributeError as e:
            # catch AttributeError cause else __getattr__ will be called
            reraise(RuntimeError, RuntimeError(e.__class__.__name__, *e.args), sys.exc_info()[2])

    def search(self, q):
        clone = self._clone()
        clone._q = q
        return clone

    def all(self):
        return list(self.results)

    def count(self):
        return self._clone()._fetch_results(only_count=True).ndocs

    def instances(self):
        clone = self._clone().only(self.searcher.unique_field)
        clone._iter_instances = True
        return clone

    def filter(self, *args, **kwargs):
        local_params = LocalParams(kwargs.pop('_local_params', None))
        clone = self._clone()
        clone._fq.append((X(*args, **kwargs), local_params))
        return clone

    def exclude(self, *args, **kwargs):
        local_params = LocalParams(kwargs.pop('_local_params', None))
        clone = self._clone()
        clone._fq.append((~X(*args, **kwargs), local_params))
        return clone

    def instance_mapper(self, instance_mapper):
        clone = self._clone()
        clone._instance_mapper = instance_mapper
        return clone

    def with_db_query(self, db_query):
        clone = self._clone()
        clone._db_query = db_query
        return clone

    def only(self, *fields):
        return self.fl(fields)

    def dismax(self):
        return self.defType('dismax')
    
    def edismax(self):
        return self.defType('edismax')

    def qf(self, fields):
        clone = self._clone()
        if isinstance(fields, dict):
            fields = fields.items()
        clone._params['qf'] = fields
        return clone

    def field_weight(self, field_name, weight=1):
        clone = self._clone()
        if 'qf' not in clone._params:
            clone._params['qf'] = []
        qf = clone._params['qf']
        for i, (f, w) in enumerate(qf):
            if f == field_name:
                qf[i] = (field_name, weight)
                break
        else:
            qf.append((field_name, weight))
        return clone

    def order_by(self, *args):
        clone = self._clone()
        if not args:
            return clone
        if len(args) == 1 and args[0] is None:
            clone._params['sort'] = None
            return clone
        fields = list(clone._params.get('sort', []))
        for field in args:
            if field is None:
                continue
            if field.startswith('-'):
                fields.append('{} desc'.format(field[1:]))
            else:
                fields.append('{} asc'.format(field))
        clone._params['sort'] = tuple(fields)
        return clone

    def limit(self, n):
        return self.rows(n)

    def offset(self, n):
        return self.start(n)

    def set_param(self, param_name, value):
        clone = self._clone()
        clone._params[param_name] = value
        return clone

    def facet(self, *fields, **facet_params):
        """Turns on/off facets.
        Also can set facets and global facet parameters.

        ``facet_params``,
        for more details see
        http://wiki.apache.org/solr/SimpleFacetParameters#Field_Value_Faceting_Parameters ::
            ``limit`` - the maximum number of facet values
            ``offset`` - an offset into the list of facet values
            ``mincount`` - the minimum counts for facet values
            ``sort`` - 'count'/'index', if 'index' orders facet values by
            ``prefix`` - filters facet values
            ``missing`` - True/False, use ``mincount`` instead
            ``method`` - 'enum'/'fc'/'fcs'
        
        Usage::

            # Turns on facets,
            # set facet by category field and 2 global facet parameters
            search_query = search_query.facet('category', mincount=1, limit=20)

            # Turns off facets
            search_query = search_query.facet(None)
        """
        clone = self._clone()
        if len(fields) == 1 and fields[0] is None:
            clone._remove_component('facet')
            clone._facet_fields = []
            clone._facet_queries = []
            clone._facet_dates = []
            clone._facet_ranges = []
            clone._facet_pivots = []
        else:
            clone._add_component('facet', **facet_params)
            for field in fields:
                clone = clone.facet_field(field)
        return clone

    def facet_field(self, field, _local_params=None, _instance_mapper=None,
                    _coerce=None, **kwargs):
        clone = self._clone()
        local_params = LocalParams(_local_params)
        clone._facet_fields.append(
            FacetField(field, local_params=local_params,
                       instance_mapper=_instance_mapper, coerce=_coerce,
                       **kwargs))
        return clone

    def facet_range(self, field, start, end, gap,
                    hardend=None, other=None, include=None,
                    _local_params=None, _coerce=None, **kwargs):
        clone = self._clone()
        local_params = LocalParams(_local_params)
        clone._facet_ranges.append(
            FacetRange(field, start, end, gap,
                       hardend=hardend, other=other, include=include,
                       local_params=local_params, coerce=_coerce, **kwargs))
        return clone

    def facet_query(self, *args, **kwargs):
        clone = self._clone()
        local_params = LocalParams(kwargs.pop('_local_params', None))
        clone._facet_queries.append(
            FacetQuery(X(*args, **kwargs), local_params=local_params))
        return clone
        
    def facet_pivot(self, *fields, **kwargs):
        clone = self._clone()
        local_params = LocalParams(kwargs.pop('_local_params', None))
        clone._facet_pivots.append(
            FacetPivot(*fields, local_params=local_params, **kwargs))
        return clone

    def group(self, *fields, **group_params):
        """Turns on/off result grouping.
        Also can set groups and global group parameters.

        ``**group_params``,
        see http://wiki.apache.org/solr/FieldCollapsing#Request_Parameters ::
            ``limit`` - number of documents per group
            ``offset`` - offset of the document list of each group
            ``sort`` - how to sort documents within a single group
            ``ngroups`` - True/False, if True includes the total number of groups,
                useful for pagination, default True
            ``format`` - 'grouped'/'simple', if 'single' represents grouped 
                result as single flat list
            ``main`` - True/False, if True turns on non grouped result,
                only last grouping command is used
            ``truncate`` - True/False, if True computes facets counts based on
                the most relavant document
            ``facet`` - True/False, if True computes facets based on groups
        
        Usage::

            # Set 2 grouped results by company and model fields
            # with 5 documents per group
            search_query = search_query.group('company', 'model', limit=5)

            # Truns off result grouping
            search_query = search_query.group(None)
        """
        clone = self._clone()
        group_params.setdefault('ngroups', True)
        if len(fields) == 1 and fields[0] is None:
            clone._remove_component('group')
            clone._groupeds = []
        else:
            clone._add_component('group', **group_params)
            for field in fields:
                clone = clone.group_field(field)
        return clone

    def group_field(self, field, _instance_mapper=None):
        clone = self._clone()
        clone._groupeds.append(
            GroupedField(field, self.searcher.group_cls, self.searcher.document_cls,
                         instance_mapper=_instance_mapper))
        return clone

    def group_query(self, *args, **kwargs):
        clone = self._clone()
        clone._groupeds.append(
            GroupedQuery(X(*args, **kwargs), self.searcher.group_cls, self.searcher.document_cls))
        return clone

    def group_func(self, func):
        clone = self._clone()
        clone._groupeds.append(
            GroupedFunc(func, self.searcher.group_cls, self.searcher.document_cls))
        return clone

    def stats(self, field, facet_fields=None):
        clone = self._clone()
        clone._stats_fields.append(
            Stats(field, facet_fields=facet_fields))
        clone._params['stats'] = True
        return clone

    def get(self, *args, **kwargs):
        clone = self.filter(*args, **kwargs).limit(1)
        if len(clone):
            return clone[0]
