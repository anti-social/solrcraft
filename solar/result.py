from .stats import Stats

class SolrResults(object):
    def __init__(self, query, hits, db_query=None, db_query_filters=[]):
        self.query = query
        self.searcher = self.query.searcher
        self.ndocs = self.hits = hits
        self.docs = []
        self._all_docs = []
        self.facet_fields = []
        self.facet_queries = []
        self.facet_dates = []
        self.facet_ranges = []
        self.stats_fields = []
        self._db_query = db_query
        self._db_query_filters = db_query_filters
        self.debug_info = {}

        self.groupeds = {}

    def __len__(self):
        return self.ndocs
    
    def __iter__(self):
        return iter(self.docs)

    def __getitem__(self, k):
        return self.docs[k]

    def add_docs(self, docs):
        self.docs = [Document(d, results=self) for d in docs]
        self._all_docs = self.docs[:]

    def add_grouped_docs(self, raw_grouped):
        self.docs = []
        self._all_docs = []
        for key, grouped_data in raw_grouped.items():
            grouped = Grouped(key,
                              grouped_data.get('ngroups'),
                              grouped_data.get('matches'))
            self.groupeds[key] = grouped
            # grouped format
            if 'groups' in grouped_data:
                groups = grouped_data['groups']
                for group_data in groups:
                    group = Group(group_data['groupValue'],
                                  group_data['doclist']['numFound'])
                    grouped.groups.append(group)
                    for doc_data in group_data['doclist']['docs']:
                        # TODO: make document cache
                        # documents with identical ids should map to one object
                        doc = Document(doc_data, results=self)
                        group.add_doc(doc)
                        self._all_docs.append(doc)
            # simple format
            else:
                for doc_data in grouped_data['doclist']['docs']:
                    doc = Document(doc_data, results=self)
                    grouped.add_doc(doc)
                    self._all_docs.append(doc)

    def get_grouped(self, key):
        return self.groupeds.get(key)

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

    def add_debuginfo(self, debug):
        self.debug_info = debug

    def get_facet_field(self, field):
        for facet in self.facet_fields:
            if facet.field == field:
                return facet
        raise ValueError("Not found facet for field: '%s'" % field)

    def _populate_instances(self):
        ids = []
        for doc in self._all_docs:
            ids.append(self.searcher.get_id(doc.id))
        instances = self.searcher.get_instances(ids, self._db_query,
                                                self._db_query_filters)

        for doc in self._all_docs:
            doc._instance = instances.get(self.searcher.get_id(doc.id))

    @property
    def instances(self):
        return [doc.instance for doc in self if doc.instance]

class Grouped(object):
    def __init__(self, key, ngroups, ndocs):
        self.key = key
        self.ngroups = ngroups # present if group.ngroups=true else None
        self.ndocs = ndocs
        self.groups = [] # grouped format
        self.docs = [] # simple format

    def add_group(self, group):
        self.groups.append(group)

    def add_doc(self, doc):
        self.docs.append(doc)

    def get_group(self, value):
        for group in self.groups:
            if group.value == value:
                return group
    
class Group(object):
    def __init__(self, value, ndocs):
        self.value = value
        self.ndocs = ndocs
        self.docs = []
    
    def add_doc(self, doc):
        self.docs.append(doc)
    
class Document(object):
    def __init__(self, doc, results=None):
        self.results = results
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
