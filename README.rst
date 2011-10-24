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

 .facet('status').facet(['category', 'type'], params={'category': {'mincount': 5}})

There are facet.field and facet.query support.
Also automatically adds tag for every fq and excludes corresponding fq from facets.
See http://wiki.apache.org/solr/SimpleFacetParameters#Multi-Select_Faceting_and_LocalParams

4. Mapping

Mapping docs and facets on any objects you want
and access via .instance attribute
  
5. Multiple solrs

Reading from and writing to multiple Solr instances.
For reading choses Solr instance randomly.
Writes into every Solr instance.
But you should syncronize Solr's manually (use rsync or something else).

6. Lazy evaluate

Only iterating, count and access to .results attribute make http requests to Solr.

TODO
----

* Dynamic fields support (need mapping for facets)
* Groups refactoring
* Lazy results and facets
* Django support
