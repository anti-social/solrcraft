
class SearchResult(object):
    def __init__(self, searcher, hits, prefetch=None, filter_instances=None):
        self.searcher = searcher
        self.hits = hits
        self.docs = []
        self.facets = []
        self._prefetch = prefetch
        self._filter_instances = filter_instances

    def __len__(self):
        return self.hits
    
    def __iter__(self):
        return iter(self.docs)

    def __getitem__(self, k):
        return self.docs[k]

    def add_doc(self, doc):
        doc.results = self
        self.docs.append(doc)

    def add_facets(self, facets):
        self.facets = facets

    def get_facet(self, name):
        for facet in self.facets:
            if facet.name == name:
                return facet
        raise ValueError("Not found facet with name: '%s'" % name)

    def pop_facet(self, name):
        for i, facet in enumerate(self.facets):
            if facet.name == name:
                return self.facets.pop(i)
        raise ValueError("Not found facet with name: '%s'" % name)

    def add_collapse_counts(self, collapsed_counts):
        for doc in self:
            doc.collapse_count = collapsed_counts.get('results', {}).get(doc.id, {}).get('collapseCount', 0)

    def _populate_instances(self):
        ids = [d.id for d in self]
        instances = self.searcher._get_instances(ids, self._prefetch.get('prefetch_fields', []), self._prefetch.get('undefer_groups', []),
                                                *self._filter_instances.get('args', []), **self._filter_instances.get('kwargs', {}))
        for doc in self:
            doc._instance = instances.get(doc.id)

    @property
    def instances(self):
        return [doc.instance for doc in self if doc.instance]

class Document(object):
    def __init__(self, doc=None):
        self.results = None
        if doc:
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
