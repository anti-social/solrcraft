from __future__ import unicode_literals

from copy import deepcopy

from .util import X, make_fq


class Grouped(object):
    def __init__(self, key, group_cls, document_cls, **kwargs):
        self.key = key
        self.group_cls = group_cls
        self.document_cls = document_cls
        self.grouped_params = kwargs
        if self.grouped_params.get('ngroups') is None:
            self.grouped_params['ngroups'] = True
        self.ngroups = None # present if group.ngroups=true else None
        self.matches = None
        self.ndocs = None
        self.start = None
        self.groups = [] # grouped format
        self.docs = [] # simple format

    def get_params(self):
        params = {}
        params['group'] = True
        params['group.{}'.format(self.param_name)] = [self.key]
        for p, v in self.grouped_params.items():
            params['group.{}'.format(p)] = v
        return params

    def process_data(self, results):
        self.groups = []
        self.docs = []
        raw_groupeds = results.raw_results.grouped
        grouped_data = raw_groupeds.get(self.key, {})
        self.ngroups = grouped_data.get('ngroups')
        self.matches = grouped_data.get('matches')
        # grouped format
        if 'groups' in grouped_data:
            groups = grouped_data['groups']
            for group_data in groups:
                doclist_data = group_data.get('doclist', {})
                group = self.group_cls(
                    group_data['groupValue'],
                    doclist_data.get('numFound'),
                    doclist_data.get('start'))
                for raw_doc in doclist_data.get('docs', []):
                    doc = self.document_cls(
                        _results=results, **raw_doc)
                    group.add_doc(doc)
                self.add_group(group)
        # simple format
        else:
            doclist_data = grouped_data.get('doclist', {})
            self.ndocs = doclist_data.get('numFound')
            self.start = doclist_data.get('start')
            for raw_doc in doclist_data.get('docs', []):
                doc = self.document_cls(_results=results, **raw_doc)
                self.add_doc(doc)
        
    def add_group(self, group):
        group.grouped = self
        self.groups.append(group)

    def add_doc(self, doc):
        self.docs.append(doc)

    def get_group(self, value):
        for group in self.groups:
            if group.value == value:
                return group


class GroupedField(Grouped):
    param_name = 'field'

    def __init__(self, field, group_cls, document_cls, instance_mapper=None, **kwargs):
        self.field = field
        self._instance_mapper = instance_mapper
        super(GroupedField, self).__init__(
            self.field, group_cls, document_cls, **kwargs)

    def instance_mapper(self, ids):
        if self._instance_mapper:
            return self._instance_mapper(ids)
        return {}
        
    def _populate_instances(self):
        values = [group.value for group in self.groups]
        instances_map = self.instance_mapper(values)
        for group in self.groups:
            group._instance = instances_map.get(group.value)


class GroupedQuery(Grouped):
    param_name = 'query'

    def __init__(self, fq, group_cls, document_cls, **kwargs):
        self.fq = fq
        super(GroupedQuery, self).__init__(
            make_fq(fq), group_cls, document_cls, **kwargs)


class GroupedFunc(Grouped):
    param_name = 'func'

    def __init__(self, func, group_cls, document_cls, **kwargs):
        self.func = func
        super(GroupedFunc, self).__init__(
            str(func), group_cls, document_cls, **kwargs)


class Group(object):
    def __init__(self, value, ndocs, start, grouped=None):
        self.value = value
        self.ndocs = ndocs
        self.start = start
        self.docs = []
        self.grouped = grouped
    
    def add_doc(self, doc):
        self.docs.append(doc)

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            if self.grouped and hasattr(self.grouped, '_populate_instances'):
                self.grouped._populate_instances()
            else:
                return None
        return self._instance
