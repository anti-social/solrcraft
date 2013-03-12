from copy import deepcopy

from .util import X, make_fq


class Grouped(object):
    def __init__(self, field, searcher=None, instance_mapper=None, **kwargs):
        self.field = field
        self.grouped_params = kwargs
        if self.grouped_params.get('ngroups') is None:
            self.grouped_params['ngroups'] = True
        self.ngroups = None # present if group.ngroups=true else None
        self.matches = self.ndocs = None
        self.groups = [] # grouped format
        self.docs = [] # simple format
        self.searcher = searcher
        self._instance_mapper = instance_mapper

    def __deepcopy__(self, memodict):
        obj = type(self)(self.field,
                         instance_mapper=self._instance_mapper,
                         **self.grouped_params)
        obj.searcher = self.searcher
        return obj

    def instance_mapper(self, ids):
        if self._instance_mapper:
            return self._instance_mapper(ids)
        return {}
        
    def get_params(self):
        params = {}
        params['group'] = True
        params['group.field'] = [make_fq(X(self.field))]
        for p, v in self.grouped_params.items():
            params['group.%s' % p] = v
        return params

    def process_data(self, results):
        self.groups = []
        self.docs = []
        raw_groupeds = results.raw_results.grouped
        grouped_data = raw_groupeds[self.field]
        self.ngroups = grouped_data.get('ngroups')
        self.matches = self.ndocs = grouped_data.get('matches')
        # grouped format
        if 'groups' in grouped_data:
            groups = grouped_data['groups']
            for group_data in groups:
                group = self.searcher.group_cls(
                    group_data['groupValue'], group_data['doclist']['numFound'])
                for raw_doc in group_data['doclist']['docs']:
                    doc = self.searcher.document_cls(
                        _results=results, **raw_doc)
                    group.add_doc(doc)
                self.add_group(group)
        # simple format
        else:
            for raw_doc in grouped_data['doclist']['docs']:
                doc = self.searcher.document_cls(_results=results, **raw_doc)
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

    def _populate_instances(self):
        values = [group.value for group in self.groups]
        instances_map = self.instance_mapper(values)
        for group in self.groups:
            group._instance = instances_map.get(group.value)


class Group(object):
    def __init__(self, value, ndocs, grouped=None):
        self.value = value
        self.ndocs = ndocs
        self.docs = []
        self.grouped = grouped
    
    def add_doc(self, doc):
        self.docs.append(doc)

    @property
    def instance(self):
        if not hasattr(self, '_instance'):
            if self.grouped:
                self.grouped._populate_instances()
            else:
                return None
        return self._instance
