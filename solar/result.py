from itertools import chain

from .util import LocalParams, X, make_fq
from .compat import force_unicode


class SolrResults(object):
    def __init__(self, raw_results, query, document_cls, instance_mapper, db_query,
                 facet_fields, facet_queries, facet_dates, facet_ranges,
                 facet_pivots, stats_fields, groupeds):
        self.raw_results = raw_results
        self.ndocs = self.hits = self.raw_results.hits
        self.docs = []
        self.query = query
        self.searcher = query.searcher
        self.document_cls = document_cls
        self.instance_mapper = instance_mapper
        self.db_query = db_query
        self.facet_fields = facet_fields
        self.facet_queries = facet_queries
        self.facet_dates = facet_dates
        self.facet_ranges = facet_ranges
        self.facet_pivots = facet_pivots
        self.stats_fields = stats_fields
        self.groupeds = groupeds

        for facet in chain(self.facet_fields, self.facet_queries,
                           self.facet_dates, self.facet_ranges,
                           self.facet_pivots):
            facet.process_data(self)
        
        for grouped in self.groupeds:
            grouped.process_data(self)

        for stats in self.stats_fields:
            stats.process_data(self)
        
        for raw_doc in self.raw_results.docs:
            doc = self.document_cls(_results=self, **raw_doc)
            self.docs.append(doc)

        self.highlighted = self.raw_results.highlighting
        self.debug_info = self.raw_results.debug

    def __bool__(self):
        return True
    __nonzero__ = __bool__
        
    def __len__(self):
        return self.ndocs

    def __iter__(self):
        return iter(self.docs)
    
    def get_grouped(self, key):
        if isinstance(key, X):
            key = make_fq(key)
        key = force_unicode(key)
        for grouped in self.groupeds:
            if grouped.key == key:
                return grouped

    def get_stats_field(self, field):
        for st in self.stats_fields:
            if st.field == field:
                return st
        
    def get_facet_field(self, key):
        for facet in self.facet_fields:
            if facet.key == key:
                return facet

    def get_facet_range(self, key):
        for facet in self.facet_ranges:
            if facet.key == key:
                return facet

    def get_facet_query(self, key_or_x, local_params=None):
        if isinstance(key_or_x, X):
            key = make_fq(key_or_x, LocalParams(local_params))
        else:
            key = key_or_x
        for facet in self.facet_queries:
            if facet.key == key:
                return facet

    def get_facet_pivot(self, key):
        for facet in self.facet_pivots:
            if facet.key == key:
                return facet

    def _populate_instances(self):
        if not self.instance_mapper:
            return

        all_docs = []
        for doc in self.docs:
            all_docs.append(doc)
        for grouped in self.groupeds:
            for group in grouped.groups:
                for doc in group.docs:
                    all_docs.append(doc)
            for doc in grouped.docs:
                all_docs.append(doc)

        ids = [doc.id for doc in all_docs]
        instances = self.instance_mapper(ids, db_query=self.db_query)

        for doc in all_docs:
            doc._instance = instances.get(doc.id)

    @property
    def instances(self):
        return [doc.instance for doc in self if doc.instance]
