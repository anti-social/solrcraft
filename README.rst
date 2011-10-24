=====
Solar
=====

Features
--------

1. Filtering

 .filter(status=0).filter(category__in=[1, 2, 3]).exclude(rank__lte=5)


2. Grouping

 .group('director', limit=5)

3. Facets

 .facet('category').facet(['status', 'type'])

There are facet.field and facet.query support
also automatically adds tag for every fq and excludes corresponding fq from facets
see http://wiki.apache.org/solr/SimpleFacetParameters#Multi-Select_Faceting_and_LocalParams

4. Mapping

Mapping docs and facets on any objects you want
and access via .instance attribute
  
5. Multiple solrs

Reading from and writing to multiple Solr instances
for reading choses Solr instance randomly
writes into every Solr instance
but you should syncronize Solr's manually (use rsync or something else)

6. Lazy evaluate

Only iterating, count and access to .results attribute make http requests to Solr

TODO
----

* Dynamic fields support (need mapping for facets)
* Groups refactoring
* Lazy results and facets
* Django support
