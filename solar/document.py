from __future__ import unicode_literals

    
class Document(object):
    def __init__(self, _results=None, **raw_doc):
        self._results = _results
        self._fields = raw_doc.keys()
        for key in raw_doc:
            setattr(self, key, raw_doc[key])

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            if self._results:
                self._results._populate_instances()
            else:
                self._instance = None
        return self._instance

    @property
    def highlighted(self):
        id = getattr(self, self._results.searcher.unique_field, None)
        if id:
            return self._results.highlighted.get(id)

    def to_solr(self):
        return dict((f, getattr(self, f)) for f in self._fields)
