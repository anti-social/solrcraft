Группировка результатов
=======================

Общая информация по `группировке результатов <http://wiki.apache.org/solr/FieldCollapsing>`_.

.. method:: group(field, limit=1, offset=None, sort=None, main=None, format=None, truncate=None)

    Включает группировку результатов по определенному полю.

    - limit - количество документов в группе
    - offset - смещение документов в группе
    - sort - сортировка документов внутри группы. Если не задана, то документы внутри группы сортируются по общему правилу
    - main - если ``True``, используется стандартный (не группированный) формат выдачи
    - format - ``'grouped'`` или ``'simple'``. При ``'simple'`` сгруппированные документы разворачиваются в линейный формат. По умолчанию используется группированный формат
    - truncate - считать ли фасеты используя только первый документ в группе

    TODO: Добавить аргументы: ngroups, field, cache_percent, \*\*kwargs

    .. code-block:: python

        search_query = search_query.group('genre', limit=10)
        # group=true&group.field=genre&group.limit=10

        grouped = search_query.results.get_grouped('genre')
        for group in grouped.groups:
            print group.value, group.ndocs
            for doc in group.docs:
                print doc.id

TODO: Добавить поддержку group.query и group.func
