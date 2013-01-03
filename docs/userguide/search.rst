Поиск
=====

Инициализация серчера:

.. code-block:: python

    from solar import SolrSearcher, X, LocalParams

    searcher = SolrSearcher('http://localhost:8983/solr')

Простой полнотекстовый запрос:

.. code-block:: python

    query = searcher.search(u'Хоббит')
    # q=Хоббит
    for doc in query:
        print doc.id, doc.title

Соответствие любому документу:

.. code-block:: python

    query = searcher.search()
    # q=*:*

Поиск по определенному полю:

.. code-block:: python

    query = searcher.search(actor_names=u'Чак Норрис')
    # FIXME: q=actor_names:Чак Норрис

    query = searcher.search(actor_ids=42)
    # q=actor_ids:42

    query = searcher.search(title=u'Страх и ненависть', description=u'Лас-Вегас')
    # FIXME: q=title:Страх и ненависть AND description=Лас\-Вегас

    query = searcher.search(title=u'Страх и ненависть', description=u'Лас-Вегас', _op=X.OR)
    # FIXME: q=title:Страх и ненависть OR description=Лас\-Вегас

По умолчанию кол-во возвращаемых документов равно 10.
Solr не позволяет вернуть все документы соответствующие поисковому запросу, см. `Solr FAQ <http://wiki.apache.org/solr/FAQ#How_can_I_get_ALL_the_matching_documents_back.3F_..._How_can_I_return_an_unlimited_number_of_rows.3F>`_.
Используйте метод `limit` для задания кол-ва документов:

.. code-block:: python

    query = query.limit(20)
    # rows=20

Включение `dismax <http://wiki.apache.org/solr/DisMaxQParserPlugin>`_
и `edismax <http://wiki.apache.org/solr/ExtendedDisMax>`_ парсеров:

.. code-block:: python

    query = query.dismax()
    query = query.edismax()
    # то же самое
    query = query.defType('dismax')
    query = query.defType('edismax')
    
    query = query.qt([('title', 5), ('description', 1)])
    # qt=title^5 description^1

Будьте осторожны при использовании `dismax`, следующий запрос ничего не найдет:

.. code-block:: python

    # пустой результат
    searcher.search().dismax()
    # q=*:*&defType=dismax

    # найдет все документы
    searcher.search()
    # q=*:*

    # также найдет все документы
    searcher.search().edismax()
    # q=*:*&defType=edismax

Определение своего серчера:

.. code-block:: python

    from solar import SolrSearcher

    class FilmSearcher(SolrSearcher):
        def search_active(self, q=None, *args, **kwargs):
            return (super(FilmSearcher, self).search(q, *args, **kwargs)
                    .edismax()
                    .qt([('title', 5), ('actor_names', 2), ('description', 0.5)])
                    .filter(status=Film.STATUS_ACTIVE))

Для хранения нескольких типов документов в одной коллекции испульзуйте CommonSearcher:

.. code-block:: python

    from solar import CommonSearcher

    class MovieSearcher(CommonSearcher):
        type_value = 'Movie'

    class ActorSearcher(CommonSearcher):
        type_value = 'Actor'

При этом в схеме обязаны присутствовать 2 дополнительных поля: `_id` и `_type`:

.. code-block:: xml

    <fields>
        <field name="_id" type="string" indexed="true" stored="true" required="true"/>
        <field name="_type" type="string" indexed="true" stored="true" required="true"/>
  
        <field name="id" type="string" indexed="true" stored="true" required="true"/>

        <!-- Другие поля -->
    </fields>

    <uniqueKey>_id</uniqueKey>
    
Ко всем запросам будет добавлен фильтр с типом документа.
При переопределении метода `search` обязательно нужно вызвать родительский метод `search`.


Отображение объектов на документы (пока доступна только SQLAlchemy):
TODO: Добавить поддержку Django

.. code-block:: python

    class MovieSearcher(CommonSearcher):
        # при задании модели `type_value` можно опустить
        model = Movie
        session = session

        def search(self, q=None, *args, **kwargs):
            return (super(MovieSearcher, self).search(q, *args, **kwargs)
                    .edismax()
                    .qt([('title', 10), ('description', 1)]))

    film_searcher = FilmSearcher('http://localhost:8983/solr')
    search_query = film_searcher.search(u'Плохой санта')
    for doc in search_query:
        print doc.id, doc.instance

Запрос в базу данных будет выполнен при первом обращении к атрибуту `instance` документа.
При этом будут получены объекты сразу для всех документов, то есть запрос будет иметь вид:

.. code-block:: python

    session.query(Movie).filter(Movie.id.in_([doc.id for doc in search_query]))
