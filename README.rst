
Solar
=====

Features
--------

1. Filtering
~~~~~~~~~ { python }
    .filter(status=0).filter(category__in=[1, 2, 3]).exclude()
~~~~~~~~~

2. Grouping
~~~~~~~~~ { python }
    .group('director', limit=5)
~~~~~~~~~

3. Facets
~~~~~~~~~ { python }
    .facet('category').facet(['status', 'type'])
~~~~~~~~~

   there are facet.field and facet.query support
   also automatically adds tag for every fq and excludes corresponding fq from facets
   see http://wiki.apache.org/solr/SimpleFacetParameters#Multi-Select_Faceting_and_LocalParams

4. Mapping
   mapping docs and facets on any objects you want
   and access via .instance attribute
  
5. Multiple solrs
   reading from and writing to multiple Solr instances
   for reading choses Solr instance randomly
   writes into every Solr instance
   but you should syncronize Solr's manually (use rsync or something else)

TODO
----

* Dynamic fields support (need mapping for facets)
* Groups refactoring
