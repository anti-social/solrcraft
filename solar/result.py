from .stats import Stats

class SolrResults(object):
    def __init__(self, query, hits, db_query=None, db_query_filters=[]):
        self.query = query
        self.searcher = self.query.searcher
        self.hits = hits
        self.docs = []
        self._all_docs = []
        self.facet_fields = []
        self.facet_queries = []
        self.facet_dates = []
        self.facet_ranges = []
        self.stats_fields = []
        self._db_query = db_query
        self._db_query_filters = db_query_filters

    def __len__(self):
        return self.hits
    
    def __iter__(self):
        return iter(self.docs)

    def __getitem__(self, k):
        return self.docs[k]

    def add_docs(self, docs):
        self.docs = [Document(d, results=self) for d in docs]
        self._all_docs = self.docs[:]

    def add_grouped_docs(self, grouped):
        self.docs = []
        self._all_docs = []
        for group_field, group_data in grouped.items():
            # grouped format
            if 'groups' in group_data:
                groups = group_data['groups']
                for group in groups:
                    group_value = group['groupValue']
                    group_doclist = group['doclist']
                    group_docs = group_doclist['docs']
                    group_hits = group_doclist['numFound']
                    g_docs = [Document(d, results=self) for d in group_docs]
                    doc = g_docs[0]
                    self.docs.append(doc)
                    self._all_docs.extend(g_docs)
                    doc.grouped_docs = g_docs[1:]
                    doc.grouped_count = group_hits - 1
            # simple format
            else:
                for doc in group_data['doclist']['docs']:
                    doc = Document(doc, results=self)
                    self.docs.append(doc)
                    self._all_docs.append(doc)

    def add_stats_fields(self, stats_fields):
        if stats_fields:
            self.stats_fields = [Stats(field, st) for field, st in stats_fields.items()]

    def get_stats_field(self, field):
        for st in self.stats_fields:
            if st.field == field:
                return st
        raise ValueError("Not found stats for field: '%s'" % field)
        
    def add_facets(self, facet_fields, facet_queries,
                   facet_dates, facet_ranges):
        self.facet_fields = facet_fields
        self.facet_queries = facet_queries
        self.facet_dates = facet_dates
        self.facet_ranges = facet_ranges

    def get_facet_field(self, field):
        for facet in self.facet_fields:
            if facet.field == field:
                return facet
        raise ValueError("Not found facet for field: '%s'" % field)

    def _populate_instances(self):
        ids = []
        for doc in self._all_docs:
            ids.append(self.searcher.get_id(doc.id))
        
        instances = self.searcher.get_instances(
            ids, self._db_query, self._db_query_filters)

        for doc in self._all_docs:
            doc._instance = instances.get(self.searcher.get_id(doc.id))

    @property
    def instances(self):
        return [doc.instance for doc in self if doc.instance]

    
class Document(object):
    def __init__(self, doc, results=None):
        self.results = results
        self.grouped_docs = []
        self.grouped_count = 0
        for key in doc:
            setattr(self, key, doc[key])

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            if self.results:
                self.results._populate_instances()
            else:
                return None
        return self._instance
