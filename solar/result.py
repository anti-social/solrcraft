
class SearchResult(object):
    def __init__(self, searcher, hits, prefetch=None, filter_instances=None):
        self.searcher = searcher
        self.hits = hits
        self.docs = []
        self._all_docs = []
        self.facets = []
        self._prefetch = prefetch
        self._filter_instances = filter_instances

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

    def _populate_instances(self):
        ids = []
        for doc in self._all_docs:
            ids.append(doc.id)
            ids.extend([g_doc.id for g_doc in doc.grouped_docs])
        instances = self.searcher._get_instances(
            ids, self._prefetch.get('prefetch_fields', []), self._prefetch.get('undefer_groups', []),
            *self._filter_instances.get('args', []), **self._filter_instances.get('kwargs', {}))

        for doc in self:
            doc._instance = instances.get(self.searcher._get_id(doc.id))
            for g_doc in doc.grouped_docs:
                g_doc._instance = instances.get(self.searcher._get_id(g_doc.id))

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
