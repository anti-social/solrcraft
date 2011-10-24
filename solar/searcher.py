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
    query_cls = SolrQuery

    default_params = {}
    facet_settings = {}

    def __init__(self, solr_url=None, model=None, query_cls=None):
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
        if id is None:
            q = make_fq(X(*args, **kwargs), add_tags=False, as_string=True)
        solr.delete(id, q, commit=commit)

    def _optimize_index(self, solr):
        solr.optimize()

    def _get_id(self, id):
        return int(id)

    def _get_instances(self, ids, prefetch_fields=[], undefer_groups=[], *args, **kwargs):
        from sqlalchemy.orm import undefer, undefer_group, eagerload, subqueryload, RelationshipProperty

        if not ids:
            return {}
        ids = [self._get_id(id) for id in ids]
        instances = {}
        dbquery = self.model.query.filter(self.model.id.in_(ids))
        for field in prefetch_fields:
            fields = field.split('__')
            mapper = self.model.mapper
            field_path = []
            for f in fields:
                field_path.append(f)
                prop = mapper.get_property(f)
                if isinstance(prop, RelationshipProperty):
                    if prop.direction.name == 'ONETOMANY':
                        dbquery = dbquery.options(subqueryload('.'.join(field_path)))
                    else:
                        dbquery = dbquery.options(eagerload('.'.join(field_path)))
                else:
                    dbquery = dbquery.options(undefer('.'.join(field_path)))
                if not hasattr(prop, 'mapper'):
                    break
                mapper = prop.mapper
        for group in undefer_groups:
            dbquery = dbquery.options(undefer_group(group))
        # apply filters
        for a in args:
            dbquery = dbquery.filter(a)
        if kwargs:
            dbquery = dbquery.filter_by(**kwargs)
        for instance in dbquery:
            instances[instance.id] = instance

        return instances
