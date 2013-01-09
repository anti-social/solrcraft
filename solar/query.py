import re
import sys
from copy import copy, deepcopy
import urllib
import logging
import warnings

from pysolr import SolrError

from .result import SolrResults, Document
from .facets import FacetField, FacetQuery, FacetValue
from .util import SafeUnicode, safe_solr_input, X, LocalParams, make_fq, make_q


log = logging.getLogger(__name__)

DEFAULT_ROWS = 10

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

class SolrQuery(object):
    def __init__(self, searcher, q, *args, **kwargs):
        self.searcher = searcher

        self._q_local_params = LocalParams(kwargs.pop('_local_params', None))
        self._q = q
        self._q_args = args
        self._q_kwargs = kwargs
        self._fq = []
        self._facet_fields = []
        self._facet_queries = []
        self._facet_dates = []
        self._facet_ranges = []
        self._params = {}

        self._db_query = None
        self._db_query_filters = []

        self._result_cache = None

    def __unicode__(self):
        params = self._prepare_params()
        p = []
        p.append(('q', self._make_q().encode('utf-8')))
        for k, v in params.items():
            if hasattr(v, '__iter__'):
                for w in v:
                    p.append((k, w))
            else:
                p.append((k, v))
        return urllib.urlencode(p, True)

    def __str__(self):
        return unicode(self)

    def __len__(self):
        results = self._fetch_results()
        return results.hits

    def __iter__(self):
        results = self._fetch_results()
        return iter(results.docs)

    def __getitem__(self, k):
        if not isinstance(k, (slice, int, long)):
            raise TypeError

        if self._result_cache is not None:
            return self._result_cache.docs[k]
        else:
            if isinstance(k, slice):
                start, stop = k.start, k.stop
                if start is None:
                    start = 0
                if stop is None:
                    rows = DEFAULT_ROWS
                else:
                    rows = stop - start
                clone = self._clone()
                clone._params['start'] = start
                clone._params['rows'] = rows
                return clone
            else:
                return self._fetch_results().docs[k]

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
                prepared_params[key] = unicode(val).lower()
            elif val is None:
                pass
            else:
                prepared_params[key] = val
        return prepared_params

    def _modify_params(self, params, only_count=False):
        def merge_params(params, merged_params):
            for p, v in merged_params.items():
                if hasattr(v, '__iter__'):
                    params.setdefault(p, []).extend(v)
                else:
                    params[p] = v
            return params
        
        if only_count:
            params['rows'] = 0
        elif 'rows' not in params:
            params['rows'] = DEFAULT_ROWS
        if self._fq:
            params['fq'] = [make_fq(x, local_params)
                            for x, local_params in self._fq]
        if 'qf' in params:
            params['qf'] = ' '.join('%s^%s' % (f, w) for f, w in params['qf'] if w)
        if 'fl' not in params:
            params['fl'] = ('*', 'score')

        if params.get('defType') == 'dismax' \
                and (self._q is None or isinstance(self._q, X) or self._q_args or self._q_kwargs):
            # should we turn off dismax query parser in this cases?
            # or maybe we should use q.alt parameter instead of q?
            # params.pop('defType', None)
            warnings.warn('DisMax query parser does not support q=*:* or q=field:text')

        for facet_field in self._facet_fields:
            params = merge_params(params, facet_field.get_params())
                    
        for facet_query in self._facet_queries:
            params = merge_params(params, facet_query.get_params())

    def _make_q(self):
        return make_q(self._q, self._q_local_params, *self._q_args, **self._q_kwargs)

    def _do_search(self, only_count=False):
        params = self._prepare_params(only_count=only_count)
        raw_results = self.searcher.select(self._make_q(), **params)

        results = SolrResults(self, raw_results.hits,
                               self._db_query, self._db_query_filters)
        self._process_facets(raw_results.facets)
        
        if raw_results.grouped:
            results.add_grouped_docs(raw_results.grouped)
        else:
            results.add_docs(raw_results.docs)
            
        results.add_facets(self._facet_fields, self._facet_queries,
                           self._facet_dates, self._facet_ranges)

        results.add_stats_fields(raw_results.stats.get('stats_fields'))
            
        return results

    def _process_facets(self, facets):
        # facet fields
        for facet_field in self._facet_fields:
            facet_field.process_data(facets['facet_fields'])
        
        # facet queries
        for facet_query in self._facet_queries:
            facet_query.process_data(facets['facet_queries'])
            
    def _clone(self, cls=None):
        cls = cls or self.__class__
        clone = cls(self.searcher, self._q, *self._q_args, **self._q_kwargs)
        clone._q_local_params = deepcopy(self._q_local_params)
        clone._fq = deepcopy(self._fq)
        clone._facet_fields = deepcopy(self._facet_fields)
        clone._facet_queries = deepcopy(self._facet_queries)
        clone._params = deepcopy(self._params)
        clone._db_query = self._db_query
        clone._db_query_filters = copy(self._db_query_filters)
        return clone
    clone = _clone

    # Public methods

    @property
    def results(self):
        try:
            return self._fetch_results()
        except AttributeError, e:
            # catch AttributeError cause else __getattr__ will be called
            log.exception(e)
            raise RuntimeError(*e.args)

    def search(self, q):
        clone = self._clone()
        clone._q = q
        return clone

    def all(self):
        return list(self.results)

    def count(self):
        return len(self._clone()._fetch_results(only_count=True))

    def instances(self):
        return self._clone(InstancesSolrQuery).only('id')

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

    def with_db_query(self, db_query):
        clone = self._clone()
        clone._db_query = db_query
        return clone

    def filter_db_query(self, *args, **kwargs):
        clone = self._clone()
        clone._db_query_filters += args
        clone._db_query_filters += kwargs.items()
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
        if not args:
            return self.sort(None)
        fields = []
        for field in args:
            if field is None:
                continue
            if field.startswith('-'):
                fields.append('%s desc' % field[1:])
            else:
                fields.append('%s asc' % field)
        return self.sort(tuple(fields))

    def limit(self, n):
        return self.rows(n)

    def offset(self, n):
        return self.start(n)

    def set_param(self, param_name, value):
        clone = self._clone()
        clone._params[param_name] = value
        return clone

    def facet(self, limit=-1, offset=0, mincount=1, sort=True,
              missing=False, method='fc'):
        clone = self._clone()
        clone._params['facet'] = True
        clone._params['facet.limit'] = limit
        clone._params['facet.offset'] = offset
        clone._params['facet.mincount'] = mincount
        clone._params['facet.sort'] = sort
        clone._params['facet.missing'] = missing
        clone._params['facet.method'] = method
        return clone

    def facet_field(self, field, _local_params=None, _instance_mapper=None, **kwargs):
        clone = self._clone()
        local_params = LocalParams(_local_params)
        clone._facet_fields.append(
            FacetField(field, local_params, instance_mapper=_instance_mapper, **kwargs))
        return clone

    def facet_query(self, *args, **kwargs):
        clone = self._clone()
        local_params = LocalParams(kwargs.pop('_local_params', None))
        clone._facet_queries.append(
            FacetQuery(X(*args, **kwargs), local_params))
        return clone
        
    
    def group(self, field, limit=1, offset=None, sort=None, main=None, format=None, truncate=None):
        clone = self._clone()
        clone._params['group'] = True
        clone._params['group.ngroups'] = True
        clone._params['group.field'] = field
        clone._params['group.limit'] = limit
        clone._params['group.offset'] = offset
        clone._params['group.sort'] = sort
        clone._params['group.main'] = main
        clone._params['group.format'] = format
        clone._params['group.truncate'] = truncate
        return clone

    def stats(self, field):
        clone = self._clone()
        clone._params['stats'] = True
        clone._params['stats.field'] = field
        return clone

    def get(self, *args, **kwargs):
        clone = self.filter(*args, **kwargs).limit(1)
        if len(clone):
            return clone[0]

class InstancesSolrQuery(SolrQuery):
    def __iter__(self):
        results = self._fetch_results()
        return iter([doc.instance for doc in results if doc.instance])

    def __getitem__(self, k):
        res = super(InstancesSolrQuery, self).__getitem__(k)
        if isinstance(res, SolrQuery):
            return res
        elif isinstance(res, list):
            return [doc.instance for doc in res if doc.instance]
        return res.instance

