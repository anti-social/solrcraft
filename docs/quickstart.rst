Быстрый старт
=============

.. code-block:: python

    from solar import SolrSearcher

    searcher = SolrSearcher('http://localhost:8983/solr')
    query = searcher.search(u'Чак Норрис')
    query = query.filter(year=1993)
    for doc in query:
        print doc.id, doc.name
