from __future__ import unicode_literals

import random

from .compat import text_type, with_metaclass
from .pysolr import Solr
from .query import SolrQuery
from .util import SafeUnicode, X, make_q
from .grouped import Group
from .document import Document


class SolrSearcherMeta(type):
    def __new__(mcs, name, bases, dct):
        cls = type.__new__(mcs, name, bases, dct)

        # attach searcher to the model if defined
        if hasattr(cls, 'model') and cls.model is not None:
            attach_as = getattr(cls, 'attach_as', 'searcher')
            setattr(cls.model, attach_as, cls())

        return cls


class SolrSearcher(with_metaclass(SolrSearcherMeta, object)):
    unique_field = 'id'

    model = None
    session = None
    db_field = 'id'
    db_field_type = int

    query_cls = SolrQuery
    group_cls = Group
    document_cls = Document

    def __init__(self, solr_url=None, solr=None, model=None, session=None, db_field=None,
                 query_cls=None, group_cls=None, document_cls=None):
        if solr_url:
            self.solr = Solr(solr_url)
        else:
            self.solr = solr

        self.model = model or self.model
        self.session = session or self.session
        self.db_field = db_field or self.db_field

        self.query_cls = query_cls or self.query_cls
        self.group_cls = group_cls or self.group_cls
        self.document_cls = document_cls or self.document_cls

        self._field_name_to_facet_cls_cache = {}

    # public methods

    def search(self, q=None, *args, **kwargs):
        return self.query_cls(self, q, *args, **kwargs)

    def get(self, id=None, ids=None, **kwargs):
        if ids and hasattr(ids, '__iter__'):
            ids = ','.join(ids)
        raw_results = self.solr.get(id=id, ids=ids, **kwargs)
        return [self.document_cls(**raw_doc) for raw_doc in raw_results.docs]

    # proxy methods

    def select(self, q, **kwargs):
        return self.solr.search(q, **kwargs)

    def add(self, docs, commit=True):
        return self.solr.add(docs, commit=commit)

    def commit(self):
        self.solr.commit()

    def delete(self, *args, **kwargs):
        commit = kwargs.pop('commit', True)
        self.solr.delete(q=make_q(None, None, *args, **kwargs), commit=commit)

    def optimize_index(self):
        self.solr.optimize()

    # methods to override

    def get_db_query(self):
        return self.session.query(self.model)

    def instance_mapper(self, ids, db_query=None):
        if not ids:
            return {}

        converted_ids_to_ids = {self.db_field_type(id): id for id in ids}

        if not db_query:
            db_query = self.get_db_query()

        if self.model is None:
            model = db_query._mapper_zero().class_
        else:
            model = self.model
        db_query = db_query.filter(
            getattr(model, self.db_field).in_(converted_ids_to_ids.keys()))
        instances = {}
        for obj in db_query:
            orig_id = converted_ids_to_ids.get(obj.id)
            if orig_id is not None:
                instances[orig_id] = obj

        return instances


class CommonSearcher(SolrSearcher):
    unique_field = '_id'
    type_field = '_type'
    type_value = None
    sep = ':'
    
    def get_type_value(self):
        return self.type_value or self.model.__name__

    def get_unique_value(self, id):
        return '{}{}{}'.format(
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

    def get(self, id=None, ids=None, **kwargs):
        if id:
            id = self.get_unique_value(id)
        if ids:
            ids = map(self.get_unique_value, ids)
        return super(CommonSearcher, self).get(id=id, ids=ids, **kwargs)

    def delete(self, *args, **kwargs):
        kwargs = kwargs.copy()
        kwargs[self.type_field] = self.get_type_value()
        return super(CommonSearcher, self).delete(*args, **kwargs)
