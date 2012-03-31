#!/usr/bin/env python
import random

from pysolr import Solr

from query import SolrQuery
from util import SafeString, SafeUnicode, safe_solr_query, X, make_fq

class SolrSearcherMeta(type):
    def __new__(mcs, name, bases, dct):
        cls = type.__new__(mcs, name, bases, dct)

        # attach searcher to the model if defined
        if hasattr(cls, 'model') and cls.model is not None:
            attach_as = getattr(cls, 'attach_as', 'searcher')
            setattr(cls.model, attach_as, cls())

        # process facets
        if hasattr(cls, 'facets') and cls.facets is not None:
            cls.facet_settings = {}
            for facet_cls in cls.facets:
                if hasattr(facet_cls, 'facets'):
                    for _facet_cls in facet_cls.facets:
                        cls.facet_settings[_facet_cls.field] = facet_cls
                else:
                    cls.facet_settings[facet_cls.field] = facet_cls

        return cls

class SolrSearcher(object):
    __metaclass__ = SolrSearcherMeta

    solr_url = None
    solr_read_urls = None
    solr_write_urls = None
    model = None
    session = None
    db_field = 'id'
    query_cls = SolrQuery

    default_params = {}
    facet_settings = {}

    def __init__(self, solr_url=None, model=None, session=None, db_field=None, query_cls=None):
        self.solr_url = solr_url or self.solr_url
        self.solr_read_urls = self.solr_read_urls or []
        self.solr_write_urls = self.solr_write_urls or []
        if self.solr_url:
            self.solr_read_urls.append(self.solr_url)
            self.solr_write_urls.append(self.solr_url)
        self.solr_read_urls = list(set(self.solr_read_urls))
        self.solr_write_urls = list(set(self.solr_write_urls))

        self.solrs_read = [Solr(url) for url in self.solr_read_urls]
        self.solrs_write = [Solr(url) for url in self.solr_write_urls]

        self.model = model or self.model
        self.session = session or self.session
        self.db_field = db_field or self.db_field
        self.query_cls = query_cls or self.query_cls

    # public methods

    def search(self, q=None, *args, **kwargs):
        clean_dismax = False
        if q is None or args or kwargs:
            clean_dismax = True
        q = safe_solr_query(q)
        q = make_fq(X(q, *args, **kwargs), add_tags=False, as_string=True)
        if isinstance(q, unicode):
            q = SafeUnicode(q)
        else:
            q = SafeString(q)
        if clean_dismax:
            return self.query_cls(self, q).dismax(None)
        return self.query_cls(self, q)

    def get(self, *args, **kwargs):
        return self.search().get(*args, **kwargs)

    # proxy methods

    def select(self, q, **kwargs):
        solr = random.choice(self.solrs_read)
        return solr.search(q, **kwargs)

    def add(self, docs, commit=True):
        for solr in self.solrs_write:
            self._add(solr, docs, commit=commit)

    def commit(self):
        for solr in self.solrs_write:
            self._commit(solr)

    def delete(self, id=None, *args, **kwargs):
        for solr in self.solrs_write:
            self._delete(solr, id=id, *args, **kwargs)

    def optimize_index(self):
        for solr in self.solrs_write:
            self._optimize_index(solr)

    # private methods

    def _add(self, solr, docs, commit=True):
        cleaned_docs = []
        for doc in docs:
            cleaned_docs.append(dict([(k, v) for k, v in doc.items() if v is not None]))
        solr.add(cleaned_docs, commit)

    def _commit(self, solr):
        solr.commit()

    def _delete(self, solr, id=None, *args, **kwargs):
        commit = kwargs.pop('commit', True)
        q = None
        if args or kwargs:
            q = make_fq(X(*args, **kwargs), add_tags=False, as_string=True)
        solr.delete(id, q, commit=commit)

    def _optimize_index(self, solr):
        solr.optimize()

    # methods to override

    def get_id(self, id):
        return int(id)

    def get_db_query(self):
        return self.session.query(self.model)

    def get_filter_by_method(self, db_query):
        if hasattr(db_query, 'filter_by'):
            # SQLAlchemy query
            return db_query.filter_by
        # Django query
        return db_query.filter

    def get_instances(self, ids, db_query=None, db_query_filters=[]):
        if not ids:
            return {}

        if not db_query:
            db_query = self.get_db_query()

        for query_filter in db_query_filters:
            if callable(query_filter):
                db_query = query_filter(db_query)
            elif isinstance(query_filter, tuple):
                db_query = self.get_filter_by_method(db_query)(
                    **{query_filter[0]: query_filter[1]})
            else:
                db_query = db_query.filter(query_filter)

        instances = {}
        db_query = db_query.filter(getattr(self.model, self.db_field).in_(ids))
        for obj in db_query:
            instances[obj.id] = obj

        return instances

class CommonSearcher(SolrSearcher):
    unique_field = '_id'
    type_field = '_type'
    type_value = None
    sep = ':'
    
    def get_type_value(self):
        return self.type_value or self.model.__name__

    def get_unique_value(self, id):
        return '%s%s%s' % (
            self.get_type_value(), self.sep, id,
        )
    
    def search(self, q=None, *args, **kwargs):
        return (
            super(CommonSearcher, self)
            .search(q, *args, **kwargs)
            .filter(**{self.type_field: self.get_type_value()})
        )

    def add(self, docs, commit=True):
        patched_docs = []
        for _doc in docs:
            if _doc:
                doc = _doc.copy()
                doc[self.unique_field] = self.get_unique_value(
                    doc[self.db_field]
                )
                doc[self.type_field] = self.get_type_value()
                patched_docs.append(doc)
                
        return super(CommonSearcher, self).add(patched_docs, commit=commit)

    def delete(self, id=None, *args, **kwargs):
        return (
            super(CommonSearcher, self)
            .delete(id, **{self.type_field: self.get_type_value()})
        )
