=====
Solar
=====

Features
--------

1. Searching
 .search('test', category=1).ps(5).qf('name^5 description').bf('linear(rank,100,0)')

 q=test+AND+category:1&ps=5&bf=linear(rank,100,0)&qf=name^5+description

2. Filtering

 .filter(status=0).filter(category__in=[1, 2, 3]).exclude(rank__lte=5)

 fq={!tag=status}status:0&fq={!tag=category}(category:1+OR+category:2+OR+category:3)&fq={!tag=rank}-rank:[*+TO+5]


3. Grouping

 .group('director', limit=5)

 group=true&group.ngroups=true&group.field=director&group.limit=5

4. Facets

 .facet('status').facet(['category', 'type'], params={'category': {'mincount': 5}})

 facet.mincount=1&facet.sort=true&facet.field={!ex=status}status&facet.field={!ex=category}category&facet.field={!ex=type}type&facet.missing=false&facet.offset=0&facet.method=fc&facet=true&facet.limit=-1&f.category.facet.mincount=5

There are facet.field and facet.query support.
Also automatically adds tag for every fq and excludes corresponding fq from facets.
See http://wiki.apache.org/solr/SimpleFacetParameters#Multi-Select_Faceting_and_LocalParams

5. Mapping

Mapping docs and facets on any objects you want
and access via .instance attribute
  
6. Multiple solrs

Reading from and writing to multiple Solr instances.
For reading choses Solr instance randomly.
Writes into every Solr instance.
But you should syncronize Solr's manually (use rsync or something else).

7. Lazy evaluate

Only iterating, count and access to .results attribute make http requests to Solr.

TODO
----

* Dynamic fields support (need mapping for facets)
* Groups refactoring
* Lazy results and facets
* Django support
* Documentation
* Unit tests
