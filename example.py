import sys

from solar import SolrSearcher, Facet
from solar.facets import compound_facet_factory


class Movie(object):
    pass

# Example of possible SQLAlchemy model
#
# class Movie(Base):
#     name = Column(Unicode(255))
#     description = Column(Unicode(4096))
#     actors = relationship('Actor', secondary=movie_actor_table)
#     director = relationship('Director')
#     category = relationship('Category')
#     rank = Column(Float)
#     release_date = Column(DateTime)

#     def __unicode__(self):
#         return self.name
    
class MovieSearcher(SolrSearcher):
    solr_read_urls = ['http://localhost:8180/movies']
    solr_write_urls = ['http://localhost:8180/movies']
    model = Movie
    attach_as = 'searcher' # optionally

    default_params = {
        'defType': 'dismax',
        'qf': 'name^5 description^1',
        'bf': 'linear(rank,100,0)',
        }

    class CategoryFacet(Facet):
        field = 'category'
        title = u'Categories'
        # model = Category

        default_params = {
            'limit': 10,
        }

    class ActorFacet(Facet):
        field = 'actors'
        title = u'Actors'

        def get_instances(self, ids):
            ids = [int(id) for id in ids]
            return dict([(str(c.id), c) for c in
                         Actor.query.undefer('something').filter(Actor.id.in_(ids))])

    class NewMovieFacet(Facet):
        field = 'release_date'
        queries = [(('release_date__gte', 'now-30d'), u'Last released',)]

    class HighRankFacet(Facet):
        field = 'rank'
        queries = [(('rank__gte', 8), u'Highest rank',)]

    facets = [CategoryFacet, ActorFacet,
              compound_facet_factory('boolean_filters', u'Additional filters',
                                     [NewMovieFacet, HighRankFacet])]

    def search(self, q=None, *args, **kwargs):
        return (super(MovieSearcher, self).search(q, *args, **kwargs)
                .pf('name').ps(5).only('id')
                .prefetch('description', 'director', 'actors', 'category'))


# Search examples

def main():
    q = Movie.searcher.search(u'monty python and holy grail')

    q = q.filter(actors=1).limit(20).offset(40)
    q = q.filter(actors=1)[40:60] # equals previous

    q = q.exclude(category__in=[1, 2, 3])
    print q
    print

    q = q.group('director', limit=5)

    q = q.facet(['category', 'actors', 'boolean_filters'],
                mincount=1, params={'category': {'limit': 5}})
    print q

    # return cause to make requests we need running Solr instance
    return

    q.count() # send request with rows=0
    len(q) # send request

    for doc in q: # do not send request
        print doc.id, doc.instance, doc.grouped_count
        if doc.grouped_docs:
            print doc.id, doc.instance

    for facet in q.results.facets:
        print facet
        for fv in facet.selected_values:
            print fv.instance

        for fv in facet:
            print fv.instance, fv.count_sign


    q = Movie.searcher.search(u'planet k-pax').instances()

    for movie in q.order_by('-release_date')[:10]:
        print movie

if __name__ == '__main__':
    main()

# http://github.com/anti-social/solar
