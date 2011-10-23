
from copy import deepcopy
import urllib

from result import SearchResult, Document
from facets import Facet, FacetValue, OPERATORS, OPS_FOR_FACET
from queryfilter import QueryFilter
from util import safe_solr_query, X, make_fq, convert_fq_to_filters_map, \
    prepare_params, split_param, make_param, process_value, unpack_tuples


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
    def __init__(self, searcher, q):
        self.searcher = searcher
        self.model = searcher.model
        
        self._q = q
        self._fq = X()
        self._params = {}
        self._filter_instances = {'args': [], 'kwargs': {}}

        self._prefetch = {}
        self._result_cache = None
        # self._facet_settings = {}

    def __unicode__(self):
        params = prepare_params(self._modify_params(deepcopy(self._params)))
        p = []
        p.append(('q', safe_solr_query(self._q).encode('utf-8')))
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

    def _modify_params(self, params, only_count=False):
        if only_count:
            params['rows'] = 0
        elif 'rows' not in params:
            params['rows'] = DEFAULT_ROWS
        if self._fq:
            params['fq'] = make_fq(self._fq)
        if 'fl' not in params:
            params['fl'] = ('*', 'score')
        # elif 'score' not in params['fl']:
        #     params['fl'] = params['fl'] + ('score',)
        
        # TODO: exclude filters for facet queries too
        if 'facet.field' in params:
            for i, facet_field in enumerate(params['facet.field']):
                if hasattr(self.searcher, 'multi_valued_fields') and facet_field in self.searcher.multi_valued_fields:
                    params['facet.field'][i] = facet_field
                else:
                    params['facet.field'][i] = '{!ex=%s}%s' % (facet_field, facet_field)
        # set default params from searcher
        for p, v in self.searcher.default_params.items():
            if p not in params:
                params[p] = v
        return params
    
    def _do_search(self, only_count=False):
        params = self._modify_params(deepcopy(self._params))
        # if 'fq' in params:
        #     print 'fq:', params['fq']
        # print params
        _results = self.searcher.select(safe_solr_query(self._q), **prepare_params(params))
        
        results = SearchResult(self.searcher, _results.hits, self._prefetch, self._filter_instances)
        for _doc in _results.docs:
            results.add_doc(Document(_doc))
        results.add_facets(self._process_facets(_results.facets))
        results.add_collapse_counts(_results.collapse_counts)
        if hasattr(_results, 'total_hits'):
            results.total_hits = _results.total_hits
        
        return results

    def _process_facets(self, _facets):
        # print _facets['facet_queries']
        facets = []
        facet_class_map = {}
        filters_map = convert_fq_to_filters_map(self._fq)
        # print filters_map

        # facet fields
        for _facet in _facets['facet_fields']:
            facet_name = _facet[0]
            facet_cls = self.searcher.facet_settings.get(facet_name, Facet)
            facet = facet_cls(self, name=facet_name)
            facet_class_map[facet_cls] = facet
            for fv_name, fv_count in _facet[1]:
                selected = False
                if facet_name in filters_map and fv_name in filters_map[facet_name]:
                    selected = True
                facet.add_value(FacetValue(facet_name, fv_name, fv_count, selected))
            facets.append(facet)
        
        # facet queries
        for facet_query, facet_count in _facets['facet_queries']:
            facet_name, facet_cond = facet_query.split(':')
            facet_cls = self.searcher.facet_settings.get(facet_name, Facet)
            if facet_cls not in facet_class_map:
                facet = facet_cls(self, name=facet_name)
                facet_class_map[facet_cls] = facet
                for (fv_param, fv_val), fv_title, fv_help_text in unpack_tuples(facet.queries, 3):
                    fv_field, fv_op = split_param(fv_param)
                    if OPS_FOR_FACET[fv_op](fv_field, fv_val) == facet_query:
                        break
                selected = facet_name in filters_map and facet_cond in filters_map[facet_name]
                fv_param = make_param(facet_name, fv_op)
                fv_val = process_value(fv_val)
                facet.add_value(FacetValue(fv_param, fv_val, facet_count, selected, fv_title, fv_help_text))
                facets.append(facet)
            else:
                facet = facet_class_map[facet_cls]
                for (fv_param, fv_val), fv_title, fv_help_text in unpack_tuples(facet.queries, 3):
                    fv_field, fv_op = split_param(fv_param)
                    if OPS_FOR_FACET[fv_op](fv_field, fv_val) == facet_query:
                        break
                selected = facet_name in filters_map and facet_cond in filters_map[facet_name]
                fv_param = make_param(facet_name, fv_op)
                fv_val = process_value(fv_val)
                facet_class_map[facet_cls].add_value(FacetValue(fv_param, fv_val, facet_count, selected, fv_title, fv_help_text))
        return facets

    def _clone(self, cls=None):
        cls = cls or self.__class__
        clone = cls(self.searcher, self._q)
        clone._fq = deepcopy(self._fq)
        clone._params = deepcopy(self._params)
        clone._prefetch = deepcopy(self._prefetch)
        clone._filter_instances = deepcopy(self._filter_instances)
        return clone

    def _clean_params(self, param):
        params = []
        for p in self._params:
            if p.startswith(param):
                params.append(p)
        for p in params:
            del self._params[p]

    # Public methods

    @property
    def results(self):
        return self._fetch_results()

    def search(self, q):
        clone = self._clone()
        clone._q = q
        return clone

    def all(self):
        return self._clone()

    def count(self):
        return len(self._clone()._fetch_results(only_count=True))

    def filter_instances(self, *args, **kwargs):
        clone = self._clone()
        clone._filter_instances['args'].extend(args)
        clone._filter_instances['kwargs'].update(kwargs)
        return clone
    
    def instances(self, *args, **kwargs):
        return self._clone(InstancesSolrQuery).only('id').filter_instances(*args, **kwargs)

    def filter(self, *args, **kwargs):
        clone = self._clone()
        clone._fq = clone._fq & X(*args, **kwargs)
        return clone

    def exclude(self, *args, **kwargs):
        clone = self._clone()
        clone._fq = clone._fq & ~X(*args, **kwargs)
        return clone
    
    def prefetch(self, *prefetch_fields, **kwargs):
        """This method was left for compatibility with Djapian.
        Now instances are fetched when first access to instance attribute of document or facet.
        """
        clone = self._clone()
        clone._prefetch['prefetch_fields'] = prefetch_fields
        if 'undefer_groups' in kwargs:
            clone._prefetch['undefer_groups'] = kwargs['undefer_groups']
        return clone

    def only(self, *fields):
        return self.fl(fields)

    def dismax(self, dismax='dismax'):
        return self.defType(dismax)

    def qf(self, fields):
        clone = self._clone()
        if isinstance(fields, dict):
            clone._params['qf'] = ' '.join(['%s^%s' % (f, v) for f, v in fields.items()])
        else:
            clone._params['qf'] = fields
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
    
    def facet(self, fields, limit=-1, offset=0, mincount=1, sort=True, missing=False, method='fc', params=None):
        def add_field_or_query(clone, field_name):
            facet_cls = clone.searcher.facet_settings.get(field_name)
            if facet_cls and hasattr(facet_cls, 'queries'):
                for (fv_field_op, fv_val), title, help_text in unpack_tuples(facet_cls.queries, 3):
                    fv_field, fv_op = split_param(fv_field_op)
                    if 'facet.query' not in clone._params:
                        clone._params['facet.query'] = []
                    if fv_field == field_name:
                        clone._params['facet.query'].append(OPS_FOR_FACET.get(fv_op, 'exact')(field_name, fv_val))
            else:
                if 'facet.field' not in clone._params:
                    clone._params['facet.field'] = []
                clone._params['facet.field'].append(field_name)
            if hasattr(facet_cls, 'default_params'):
                for facet_param, val in facet_cls.default_params.items():
                    clone._params['f.%s.facet.%s' % (field_name, facet_param)] = val
            if params and field_name in params:
                for facet_param, val in params[field_name].items():
                    clone._params['f.%s.facet.%s' % (field_name, facet_param)] = val

        clone = self._clone()
        if fields:
            if isinstance(fields, basestring):
                fields = [fields]
            for field_data in fields:
                if isinstance(field_data, basestring):
                    add_field_or_query(clone, field_data)
                # elif isinstance(field_data, (list, tuple)) and len(field_data) > 0:
                #     settings = {}
                #     clone._params['facet.field'].append(field_data[0])
                #     if len(field_data) >= 2:
                #         settings['name'] = field_data[1]
                #     if len(field_data) >= 3:
                #         settings['model'] = field_data[2]
                #     self._facet_settings[field_data[0]] = settings
            
            clone._params['facet'] = True
            clone._params['facet.limit'] = limit
            clone._params['facet.offset'] = offset
            clone._params['facet.mincount'] = mincount
            clone._params['facet.sort'] = sort
            clone._params['facet.missing'] = missing
            clone._params['facet.method'] = method
        else:
            clone._clean_params('facet')
        return clone
    facets = facet

    def collapse(self, field, threshold=1, maxdocs=None):
        clone = self._clone()
        if field:
            clone._params['collapse'] = True
            clone._params['collapse.field'] = field
            clone._params['collapse.threshold'] = self.searcher.default_params.get('collapse.threshold', threshold)
            clone._params['collapse.maxdocs'] = self.searcher.default_params.get('collapse.maxdocs', maxdocs)
        else:
            clone._clean_params('collapse')
        return clone

    def get_query_filter(self, filters=None, exclude_facets=None):
        filters = filters or []
        exclude_facets = set(exclude_facets or [])
        qf = QueryFilter()
        for f in filters:
            qf.add_filter(f)
        if 'facet.field' in self._params:
            for field in self._params['facet.field']:
                if field not in exclude_facets:
                    qf.add_filter(field)
        if 'facet.query' in self._params:
            for facet_query in self._params['facet.query']:
                field, cond = facet_query.split(':')
                if field not in exclude_facets:
                    qf.add_filter(field)
        return qf

class InstancesSolrQuery(SolrQuery):
    def __iter__(self):
        results = self._fetch_results()
        return iter([doc.instance for doc in results if doc.instance])

    def __getitem__(self, k):
        res = super(InstancesSolrQuery, self).__getitem__(k)
        if isinstance(res, SolrQuery):
            return res
        else:
            return [doc.instance for doc in res if doc.instance]
