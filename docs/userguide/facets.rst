Фасеты
======

`Simple facet parameters <http://wiki.apache.org/solr/SimpleFacetParameters>`_

.. method:: facet(limit=-1, offset=0, mincount=1, sort=True, missing=False, method='fc')

    Общие параметры для всех фасетов.


Фасеты по полю
--------------

.. method:: facet_field(field, _local_params=None, _instance_mapper=None, **kwargs)

    .. code-block:: python

        search_query.facet_field('genre')
        # facet.field=genre

        search_query.facet_field('artist', limit=10)
        # facet.field=artist&f.artist.facet.limit=10
    

Отображение значений фасетов
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Даже для числовых полей значения в фасетах будут в виде строк,
поэтому если вы ожидаете целые числа вам нужно будет конвертировать их вручную.

.. code-block:: python

    def genre_mapper(ids):
        if ids:
            ids = [int(id) for id in ids]
            query = session.query(Genre).filter(Genre.id.in_(ids))
            return dict((str(g.id), g) for g in query)
        return {}

    search_query = search_query.facet_field('genre_id', _instance_mapper=genre_mapper)
    for fv in search_query.results.get_facet_field('genre_id').values:
        print fv.instance, fv.value, fv.count

`genre_mapper` будет вызвана при первом обращении к атрибуту `instance` любого из значений фасета.
Функция-маппер должна вернуть словарь, где ключами будут исходные значения фасета,
а значения - соответствующие им объекты.


Произвольные фасеты
-------------------

.. method:: facet_query(*args, **kwargs)

    Позволяет задать фасет с произвольным фильтром.
    Принимает те же аргументы, что и метод :func:`filter`.

    .. code-block:: python

        search_query = search_query.facet_query(price__gte=500)
        # facet.query=price:[500 TO *]

        search_query = search_query.facet_query(X(status=0) | X(status=1))
        # facet.query=(status:0 OR status:1)

        # TODO:
        # search_query.results.get_facet_query(price__gte=500)

Геофасеты
~~~~~~~~~

Локальные параметры для фасетов
-------------------------------

key
~~~

Предоставляет возможность изменить ключ

.. code-block:: python

    search_query.facet_query(price__lte=100, _local_params=LocalParams(key='lowcost'))
    # facet.query={!key=lowcost}price:[* TO 100]
    # part of solr response:
    # {
    #   "facet_counts": {
    #     "facet_queries": {
    #       "lowcost": 2,
    # ...

ex
~~

Позволяет при рассчете фасета исключить фильтры, помеченные соответствующим тегом.

.. code-block:: python

    (search_query.filter(category=13, _local_params=LocalParams(tag='cat'))
     .facet_field('category', _local_params=LocalParams(ex='cat')))
    # fq={!tag=cat}category:13&facet.field={!ex=cat}category


TODO: facet_pivot

TODO: facet_date & facet_range
