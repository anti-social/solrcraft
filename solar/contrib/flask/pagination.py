# coding: utf-8
from math import ceil


class SolrQueryWrapper(object):
    """Solr returns total count with response.
    So we can get documents and count with one request.
    """
    def __init__(self, query, grouped_by=None):
        self.query = query
        self.grouped_by = grouped_by
        self.sliced_query = None
        self.items = None
        self.count = None

    def __getitem__(self, range):
        if not isinstance(range, slice):
            raise ValueError('__getitem__ without slicing not supported')
        self.sliced_query = self.query[range]
        if self.grouped_by:
            grouped = self.sliced_query.results.get_grouped(self.grouped_by)
            self.items = grouped.groups
            self.count = grouped.ngroups
        else:
            self.items = list(self.sliced_query)
            self.count = len(self.sliced_query)
        return self.items

    def __iter__(self):
        if self.items is None:
            raise ValueError('Slice first')
        return iter(self.items)
    
    def __len__(self):
        if self.count is None:
            raise ValueError('Slice first')
        return self.count

    @property
    def results(self):
        if self.sliced_query is None:
            raise ValueError('Slice first')
        return self.sliced_query.results


class Pagination(object):
    """Helper class to provide compatibility with Flask-SQLAlchemy paginator.
    """
    def __init__(self, query, page=1, per_page=20):
        self.original_query = query
        if isinstance(query, SolrQueryWrapper):
            self.query = query
        else:
            self.query = SolrQueryWrapper(query)
        self.page = page if page > 0 else 1
        self.per_page = per_page
        self.offset = (self.page - 1) * self.per_page

        self.items = self.query[self.offset:self.offset + self.per_page]
        self.total = len(self.query)
        
    @property
    def pages(self):
        return int(ceil(self.total / float(self.per_page)))

    def prev(self):
        return type(self)(
            self.original_query, page=self.prev_num, per_page=self.per_page)
    
    @property
    def has_prev(self):
        return self.page > 1

    @property
    def prev_num(self):
        return self.page - 1

    def next(self):
        return type(self)(
            self.original_query, page=self.next_num, per_page=self.per_page)

    @property
    def has_next(self):
        return self.page < self.pages

    @property
    def next_num(self):
        return self.page + 1

    def iter_pages(self, left_edge=2, left_current=2,
                   right_current=5, right_edge=2):
        """Iterates over the page numbers in the pagination. The four
        parameters control the thresholds how many numbers should be produced
        from the sides. Skipped page numbers are represented as `None`.
        This is how you could render such a pagination in the templates:

        .. sourcecode:: html+jinja

        {% macro render_pagination(pagination, endpoint) %}
          <div class=pagination>
            {%- for page in pagination.iter_pages() %}
              {% if page %}
                {% if page != pagination.page %}
                  <a href="{{ url_for(endpoint, page=page) }}">{{ page }}</a>
                {% else %}
                  <strong>{{ page }}</strong>
                {% endif %}
              {% else %}
                <span class=ellipsis>…</span>
              {% endif %}
            {%- endfor %}
          </div>
        {% endmacro %}
"""
        last = 0
        for num in range(1, self.pages + 1):
            if num <= left_edge or \
               (num > self.page - left_current - 1 and \
                num < self.page + right_current) or \
               num > self.pages - right_edge:
                if last + 1 != num:
                    yield None
                yield num
                last = num
