Фильтрация результатов поиска
=============================

Простая фильтрация
------------------

Для фильтрации результатов поиска можно использовать два метода объекта SolrQuery:
`filter` и обратный ему `exclude`.

.. method:: filter(*args, **kwargs)

    Метод `filter` создает новый объект **SolrQuery** содержащий документы соответствующие
    параметрам данного фильтра. Множественные параметры соединяются с помощью логического `И`
    (`AND` в соответствующем параметре `fq` Solr запроса).
    
    .. code-block:: python
    
        search_query.filter(id=15)
        # fq=id:15

        search_query.filter(genre='Comedy')
        # fq=genre:Comedy

        search_query.filter(is_active=True)
        # fq=is_active:true
        search_query.filter(is_active=False)
        # fq=is_active:false

        from datetime import date, datetime
        search_query.filter(date_created=date(2013, 1, 4))
        # fq=date_created:2013-01-04T00:00:00Z
        search_query.filter(date_created=datetime(2013, 1, 4, 13, 0, 42))
        # fq=date_created:2013-01-04T13:00:42Z

        search_query.filter(rating=None)
        # fq=(NOT rating:[* TO *])

        search_query.filter(is_active=True, genre='Fantasy')
        # fq=(is_active:true AND genre:Fantasy)

.. method:: exclude(*args, **kwargs)

    Метод `exclude` создает новый объект **SolrQuery** содержащий документы *НЕ* соответствующие
    параметрам данного фильтра. Множественные параметры соединяются с помощью логического `И`
    (`AND` в соответствующем параметре `fq` Solr запроса).

    .. code-block:: python

        search_query.exclude(id=42)
        # fq=(NOT id:42)
        
        search_query.filter(rating=None)
        # fq=(NOT (NOT rating:[* TO *]))

        search_query.exclude(is_active=True, genre='Fantasy')
        # fq=(NOT (is_active:true AND genre:Fantasy))

При необходимости можно соединить несколько параметров фильтра логическим `ИЛИ` (`OR`)
с помощью именованного аргумента `_op`:

    .. code-block:: python

        from solar import X
        search_query.filter(is_active=True, genre='Fantasy', _op=X.OR)
        # fq=(is_active:true OR genre:Fantasy)


Фильтрация с условиями
----------------------

exact
~~~~~

Точное соответствие содержимого поля запрашиваемому значению.
Полезно при наличии пробелов в строковом поле.

.. code-block:: python

    search_query.filter(genre__exact=u'Talk Shows')
    # fq=genre:"Talk Shows"

    search_query.filter(keywords__exact=u'Based On Novel')
    # fq=keywords:"Based On Novel"

in
~~

Содержимое поля должно соответствовать одному из значений::

    search_query.filter(id__in=[100, 101, 202, 404])
    # fq=(id:100 id:101 id:202 id:404)

    search_query.filter(id__in=[])
    # fq=id:[* TO *] AND (NOT id:[* TO *])

.. warning:: Ограничение на количество условий в фильтре

    Solr имеет ограничение на количество условий в фильтре равное 1024 условиям.
    Это ограничение можно изменить в файле *solrconfig.xml*, см. `maxBooleanClauses <http://wiki.apache.org/solr/SolrConfigXml#The_Query_Section>`_

gte
~~~

Содержимое поля должно быть больше либо равно запрашиваемому значению::

    search_query.filter(year__gte=2010)
    # fq=year:[2010 TO *]

lte
~~~

Меньше либо равно::

    search_query.filter(rating__lte=3.8)
    # fq=rating:[* TO 3.8]
    
gt
~~

Больше::

    search_query.filter(gross__gt=100000000)
    # fq=gross:{100000000 TO *}

lt
~~

Меньше::

    search_query.filter(year__lt=1950)
    # fq=rating:{* TO 1950}

between
~~~~~~~

Между::

    search_query.filter(year__between=[2000, 2010])
    # fq=year:[2000 TO 2010]

isnull
~~~~~~

Проверка, что поле не содержит никакого значения::

    search_query.filter(genre__isnull=True)
    # fq=(NOT genre:[* TO *])

Также можно проверить, что поле содержит любое значение::

    search_query.filter(raging__isnull=False)
    # fq=raging:[* TO *]


Объект X
--------

Для построения сложных составных условий можно воспользоваться объектом X.
Его использование аналогично использованию Q в Django.

.. code-block:: python

    from solar import X

    search_query.filter(~X(year__lt=2010))
    # fq=(NOT year:{* TO 2010})

    search_query.filter(X(status=0) | X(author_id=100))
    # fq=(status:0 OR author_id:100)

    search_query.filter(X(status=1) & (X(genre='Comedy') | X(genre='Action') & ~X(genre='Fantasy')))
    # fq=(status:1 AND (genre:Comedy OR genre:Action) AND (NOT genre:Fantasy))


Локальные параметры для фильтрации
----------------------------------

Локальные параметры передаются в методы :func:`filter` и :func:`exclude`
с помощью именованного аргумента `_local_params`.

Для всех примеров нужно добавить импорт::

    from solar import LocalParams

tag
~~~

Помечает фильтр тегом::

    search_query.filter(year__between=[1990, 2000], _local_params=LocalParams(tag='year'))
    # fq={!tag=year}year:[1990 TO 2000]

Помеченный таким образом фильтр затем можно будет исключить при рассчете фасета.
    
frange
~~~~~~

Используется для фильтрации по диапазону значений возвращаемых функцией.
Параметры `frange`, все являются опциональными:

- l - нижнее значение *от*
- u - верхнее значение *до*
- incl - включать ли нижнее значение в диапазон, по умолчанию `true`
- incu - включать ли верхнее значение в диапазон, по умолчанию `true`

.. code-block:: python

    from solar import func
    search_query.filter(func.log('popularity'), _local_params=LocalParams('frange', l=0.5, u=1))
    # fq={!frange l=0.5 u=1}log(popularity)

.. _geofilt:

geofilt
~~~~~~~

`Spatial search <http://wiki.apache.org/solr/SpatialSearch>`_

Используется для фильтрации по расстоянию на сфере,
см. `The distance filter <http://wiki.apache.org/solr/SpatialSearch#geofilt_-_The_distance_filter>`_.
Параметры:
- pt - точка, от которой считается расстояние
- sfield - поле, в котором хранятся координаты
- d - максимальное расстояние в километрах между точками `pt` и `sfield`

`geofilt` рассчитывает *честное* расстояние между двумя точками на поверхности земли.

Поле `sfield` должно быть определено в схеме следующим образом:

.. code-block:: xml

    <fields>
        <!-- Другие поля -->

        <fieldType name="location" class="solr.LatLonType" subFieldSuffix="_coordinate"/>

        <dynamicField name="*_coordinate"  type="tdouble" indexed="true"  stored="false"/>
        
    </fields>

.. code-block:: python

    search_query.filter(
        _local_params=LocalParams('geofilt', pt='45.15,-93,85', sfield='location', d=5))
    # fq={!geofilt pt=45.15,-93.85 sfield=location d=5}

    # эквивалентный запрос
    (
        search_query.filter(_local_params=LocalParams('geofilt'))
        # TODO: .pt(45.15, -93.85)
        .pt('45.15,-93,85')
        .sfield('location')
        .d(5)
    )
    # fq={!geofilt}&pt=45.15,-93.85&sfield=location&d=5

bbox
~~~~

Аналогичен :ref:`geofilt`, но использует менее точный рассчет расстояний.
Включает немного большую область, например при d=5,
точка отстоящая от данной на 5.1 км может быть включена в выборку.
Может быть полезен для сортировки.

cache
~~~~~

`cache=false` отключенает `filterCache` для данного фильтра `fq`,
см. `Caching of filters <http://wiki.apache.org/solr/CommonQueryParameters#Caching_of_filters>`_::

    search_query.filter(keywords='Sequel', _local_params=LocalParams(cache=False))
    # fq={!cache=false}keywords:Sequel

Отключение кеша для фильтров может быть полезно в случаях:
    - когда поле содержит очень много различных значений
    - для редко используемых фильтров
    - когда фильтр требует тяжелых вычислений (совместно с cost)

Для первых двух пунктов, чтобы предотвратить выталкивание из `filterCache` других закешированных фильтров.

cost
~~~~

Определяет порядок применения *не кешируемых* фильтров к поисковому запросу.
По умолчанию фильтры применяются параллельно, получившиеся множества затем пересекаются.
Полезно для уменьшения документов, к которым будет применен фильтр.

.. code-block:: python

    from solar import func
    (search_query
    .filter(keywords='Sequel', _local_params=LocalParams(cache=False, cost=5))
    .filter(func.mul(func.sqrt('popularity'), func.sqrt('rating')), _local_params=LocalParams(cache=False, cost=100)))
    # fq={!cache=false cost=5}keywords:Sequel&fq={!cache=false cost=100}mul(sqrt(popularity),sqrt(rating))


Примеры
-------

Фильтр для выбора магазинов из "FL, Jacksonville" или в радиусе 50 км.
Пример взят из `Combine with subquery <http://wiki.apache.org/solr/SpatialSearch#How_to_combine_with_a_sub-query_to_expand_results>`_:

.. code-block:: python

    from solar import X, LocalParams
    (search_query
     .filter(X(state='FL') & X(city='Jacksonville') | X(_query_=LocalParams('geofilt')))
     .sfield('store')
     .pt('45.15,-93.85')
     .d(50))
    # TODO: .order_by(func.geodist().asc())
    # fq=((state:FL AND city:Jacksonville) OR _query_:"{!geofilt}")&sfield=store&pt=45.15,-93.85&d=50
