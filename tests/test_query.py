from __future__ import unicode_literals

from datetime import datetime
from collections import namedtuple

from mock import patch, Mock

from solar.searcher import SolrSearcher
from solar.util import SafeUnicode, X, LocalParams, make_fq
from solar.types import Integer, Float, DateTime
from solar.compat import force_unicode
from solar import func

from .base import TestCase


Obj = namedtuple('Obj', ['id', 'name'])


def _obj_mapper(ids):
    return {id: Obj(id, '{0} {0}'.format(id)) for id in ids}


class QueryTest(TestCase):
    def test_query_params(self):
        q = SolrSearcher().search()

        self.assertNotIn('fl=', str(q))
        self.assertIn('fl=id', str(q.fields('id')))
        self.assertIn('fl=id,score', str(q.fields('id', 'score')))
        self.assertNotIn('fl=', str(q.add_fields('id')))
        self.assertIn(
            'fl=id,score,status,popularity',
            str(q.fields('id', 'score').add_fields('status', 'popularity'))
        )
        self.assertIn(
            'fl=id',
            str(q.fields('id', 'score').add_fields('status').fields(None).fields('id'))
        )

        q = SolrSearcher().search().dismax()
        raw_query = str(q)

        self.assertIn('q=*:*', raw_query)
        self.assertIn('defType=dismax', raw_query)

        q = SolrSearcher().search('test query').dismax()
        raw_query = str(q)

        self.assertIn('q=test query', raw_query)
        self.assertIn('defType=dismax', raw_query)

        q = SolrSearcher().search(name='test').dismax()
        raw_query = str(q)

        self.assertIn('q=name:test', raw_query)
        self.assertIn('defType=dismax', raw_query)

        q = SolrSearcher().search(name='test').edismax()
        raw_query = str(q)

        self.assertIn('q=name:test', raw_query)
        self.assertIn('defType=edismax', raw_query)

        q = SolrSearcher().search().order_by('-score')
        self.assertIn('sort=score desc', str(q))
        q = q.order_by('popularity')
        self.assertIn('sort=score desc,popularity asc', str(q))
        q = q.order_by()
        self.assertIn('sort=score desc,popularity asc', str(q))
        q = q.order_by(None)
        self.assertNotIn('sort', str(q))

        q = (
            SolrSearcher().search(X(name='test') | X(name__startswith='test'))
            .dismax()
            .qf([('name', 10), ('keywords', 2)])
            .bf(func.linear('rank', 1, 0) * 100 + func.recip(func.ms('NOW/HOUR', 'dt_created'), 3.16e-11, 1, 1))
            .field_weight('name', 5)
        )
        raw_query = str(q)
        
        self.assertIn('q=(name:test OR name:test*)', raw_query)
        self.assertIn('qf=name^5 keywords^2', raw_query)
        self.assertIn('bf=linear(rank,1,0)^100 recip(ms(NOW/HOUR,dt_created),3.16e-11,1,1)', raw_query)
        self.assertIn('defType=dismax', raw_query)


        q = (
            SolrSearcher().search()
            .bf(func.linear('rank', 1, 0) * 100)
            .bf(func.product('rank', 'popularity'))
        )
        raw_query = str(q)
        self.assertIn('bf=linear(rank,1,0)^100', raw_query)
        self.assertIn('bf=product(rank,popularity)', raw_query)
        q = q.bf(None, func.recip(func.ms('NOW/HOUR', 'dt_created'), 3.16e-11, 1, 1))
        raw_query = str(q)
        self.assertIn('bf=recip(ms(NOW/HOUR,dt_created),3.16e-11,1,1)', raw_query)
        self.assertNotIn('bf=linear(rank,1,0)^100', raw_query)
        self.assertNotIn('bf=product(rank,popularity)', raw_query)
        
        q = (
            SolrSearcher()
            .search(LocalParams('dismax', bf=func.linear('rank', 100, 0),
                                qf='name', v=X(SafeUnicode('"nokia lumia"')) | X(SafeUnicode('"nokia n900"'))))
        )
        raw_query = str(q)

        self.assertIn("q={!dismax "
                      "bf='linear(rank,100,0)' "
                      "qf=name "
                      """v='(\\"nokia lumia\\" OR \\"nokia n900\\")'}""",
                      raw_query)

        q = (
            SolrSearcher()
            .search(
                X(_query_=LocalParams('dismax', bf=func.linear('rank', 100, 0),
                                      qf='name^10', v='nokia'))
                & X(_query_=LocalParams('dismax', bf=func.linear('rank', 100, 0),
                                        qf='description', v='nokia lumia AND')))
        )
        raw_query = str(q)

        self.assertIn(
            'q=(_query_:"{!dismax bf=\'linear(rank,100,0)\' qf=\'name^10\' v=nokia}" '
            'AND _query_:"{!dismax bf=\'linear(rank,100,0)\' qf=description v=\'nokia lumia and\'}")',
            raw_query)

        # Facets
        q = (
            SolrSearcher().search()
            .facet('category', 'model', mincount=5, limit=100)
            .facet_field('manufacturer', limit=10)
            .facet_query(X(is_active=True))
            .facet_range('price_unit', start=0, end=1000, gap=100,
                         _local_params={'key': 'price'})
        )
        raw_query = str(q)
        self.assertIn('facet=true', raw_query)
        self.assertIn('facet.mincount=5', raw_query)
        self.assertIn('facet.limit=100', raw_query)
        self.assertIn('facet.field=category', raw_query)
        self.assertIn('facet.field=model', raw_query)
        self.assertIn('facet.field=manufacturer', raw_query)
        self.assertIn('f.manufacturer.facet.limit=10', raw_query)
        self.assertIn('facet.query=is_active:true', raw_query)
        self.assertIn('facet.range={!key=price}price_unit', raw_query)
        self.assertIn('f.price_unit.facet.range.start=0', raw_query)
        self.assertIn('f.price_unit.facet.range.end=1000', raw_query)
        self.assertIn('f.price_unit.facet.range.gap=100', raw_query)

        q = q.facet(mincount=None)
        raw_query = str(q)
        self.assertIn('facet=true', raw_query)
        self.assertIn('facet.limit=100', raw_query)
        self.assertNotIn('facet.mincount=5', raw_query)

        q = q.facet(None)
        raw_query = str(q)
        self.assertNotIn('facet=true', raw_query)
        self.assertNotIn('facet.mincount=5', raw_query)
        self.assertNotIn('facet.field=category', raw_query)
        self.assertNotIn('facet.field=model', raw_query)
        self.assertNotIn('facet.field=manufacturer', raw_query)
        self.assertNotIn('f.manufacturer.facet.limit=10', raw_query)
        self.assertNotIn('facet.query=is_active:true', raw_query)

        # Grouping
        q = (
            SolrSearcher().search()
            .group(limit=4, facet=True)
            .group_field('company')
            .group_query(X(status=0), X(visible=True))
            .group_func(func.termfreq('text', "'term'"))
        )
        raw_query = str(q)
        self.assertIn('group=true', raw_query)
        self.assertIn('group.ngroups=true', raw_query)
        self.assertIn('group.limit=4', raw_query)
        self.assertIn('group.facet=true', raw_query)
        self.assertIn('group.field=company', raw_query)
        self.assertIn('group.query=status:0 AND visible:true', raw_query)
        self.assertIn("group.func=termfreq(text,'term')", raw_query)
        q = q.group(None)
        raw_query = str(q)
        self.assertNotIn('group=true', raw_query)
        self.assertNotIn('group.ngroups=true', raw_query)
        self.assertNotIn('group.limit=4', raw_query)
        self.assertNotIn('group.facet=true', raw_query)
        self.assertNotIn('group.field=company', raw_query)
        self.assertNotIn('group.query=status:0 AND visible:true', raw_query)
        self.assertNotIn("group.func=termfreq(text,'term')", raw_query)

    def test_slice(self):
        q = self.searcher.search().offset(40).limit(20)
        raw_query = force_unicode(q)
        self.assertIn('start=40', raw_query)
        self.assertIn('rows=20', raw_query)

        q = self.searcher.search()
        q = q[40:60]
        raw_query = force_unicode(q)
        self.assertIn('start=40', raw_query)
        self.assertIn('rows=20', raw_query)

        q = self.searcher.search()
        q = q[:10]
        raw_query = force_unicode(q)
        self.assertNotIn('start', raw_query)
        self.assertIn('rows=10', raw_query)
        
    def test_filter(self):
        q = SolrSearcher().search()

        self.assertSequenceEqual(
            q.filter(status=0)._prepare_params()['fq'],
            ["status:0"])
        self.assertSequenceEqual(
            q.filter(status=0).filter(company_status__in=[0, 6])._prepare_params()['fq'],
            ["status:0", "(company_status:0 OR company_status:6)"])
        self.assertSequenceEqual(
            q.filter(X(status=0), X(company_status=0), _op='OR')._prepare_params()['fq'],
            ["(status:0 OR company_status:0)"])
        self.assertSequenceEqual(
            q.filter(with_photo=True)._prepare_params()['fq'],
            ["with_photo:true"])
        self.assertSequenceEqual(
            q.filter(date_created__gt=datetime(2012, 5, 17, 14, 35, 41, 794880))._prepare_params()['fq'],
            ["date_created:{2012-05-17T14:35:41Z TO *}"])
        self.assertSequenceEqual(
            q.filter(price__lt=1000)._prepare_params()['fq'],
            ["price:{* TO 1000}"])
        self.assertSequenceEqual(
            q.filter(X(price__lte=100), X(price__gte=1000))._prepare_params()['fq'],
            ["price:[* TO 100] AND price:[1000 TO *]"])
        self.assertSequenceEqual(
            q.filter(price__between=[500, 1000], _local_params=[('cache', False), ('cost', 50)]) \
                ._prepare_params()['fq'],
            ["{!cache=false cost=50}price:{500 TO 1000}"])
        self.assertSequenceEqual(
            q.filter(price=None)._prepare_params()['fq'],
            ["(*:* NOT price:[* TO *])"])
        self.assertSequenceEqual(
            q.exclude(price=None)._prepare_params()['fq'],
            ["NOT ((*:* NOT price:[* TO *]))"])
        self.assertSequenceEqual(
            q.filter(price__isnull=True)._prepare_params()['fq'],
            ["(*:* NOT price:[* TO *])"])
        self.assertSequenceEqual(
            q.filter(X(genre='Comedy') & ~X(genre='Drama'))._prepare_params()['fq'],
            ["(genre:Comedy AND NOT (genre:Drama))"])
        self.assertSequenceEqual(
            q.filter(price__isnull=False)._prepare_params()['fq'],
            ["price:[* TO *]"])
        self.assertSequenceEqual(
            q.filter(category__in=[])._prepare_params()['fq'],
            ["(category:[* TO *] AND NOT category:[* TO *])"])
        self.assertSequenceEqual(
            q.filter(X(category__in=[1, 2, 3, 4, 5]), _local_params={'tag': 'category'}) \
                .filter(X(status=0) | X(status=5) | X(status=1) \
                            & X(company_status=6))._prepare_params()['fq'],
            ["{!tag=category}(category:1 OR category:2 OR category:3 OR category:4 OR category:5)",
             "(status:0 OR status:5 OR (status:1 AND company_status:6))"])
        self.assertSequenceEqual(
            q.exclude(status=1)._prepare_params()['fq'],
            ["NOT (status:1)"])
        self.assertSequenceEqual(
            q.exclude(status__in=[1, 2, 3])._prepare_params()['fq'],
            ["NOT ((status:1 OR status:2 OR status:3))"])


    def test_count(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 181,
    "start": 0,
    "docs": []
  }
}
'''

            q = self.searcher.search()
            self.assertEqual(q.count(), 181)
            self.assertEqual(q.count(), 181)
            self.assertEqual(send_request.call_count, 2)


    def test_iter_docs(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 181,
    "start": 0,
    "docs": [
      {
        "id": "1",
        "name": "Test 1"
      },
      {
        "id": "2",
        "name": "Test 2"
      }
    ]
  }
}
'''

            canonical_docs = [
                {"id": "1", "name": "Test 1"},
                {"id": "2", "name": "Test 2"}
            ]

            def check_docs(docs, canonical_docs):
                for doc, canonical_doc in zip(docs, canonical_docs):
                    self.assertEqual(doc.id, canonical_doc['id'])
                    self.assertEqual(doc.name, canonical_doc['name'])

            q = self.searcher.search()
            check_docs(q.all(), canonical_docs)
            check_docs(list(q), canonical_docs)
            check_docs(iter(q), canonical_docs)
            self.assertEqual(send_request.call_count, 1)

    def test_query_cloning(self):
        q = self.searcher.search()
        q = q.qf([('name', 5)])
        self.assertIn('qf=name^5', str(q))

        q2 = q.field_weight('description')
        self.assertIn('qf=name^5 description^1', str(q2))

        q = q.qf([('name', 5)])
        self.assertNotIn('qf=name^5 description^1', str(q))

        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 20,
    "start": 0,
    "docs": []
  },
  "facet_counts": {
    "facet_fields": {
      "category": [
        "1",
        20
      ]
    }
  }
}
'''
            q = self.searcher.search()
            q = q.facet_field('category')
            q2 = q.clone()

            raw_query = str(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.field=category', raw_query)

            r = q.results

            cat_facet = r.get_facet_field('category')
            self.assertEqual(len(cat_facet.values), 1)

            with self.patch_send_request() as send_request:
                send_request.return_value = '''
{
  "response": {
    "numFound": 0,
    "start": 0,
    "docs": []
  },
  "facet_counts": {
    "facet_fields": {}
  }
}
'''
                r = q2.results

                cat_facet = r.get_facet_field('category')
                self.assertEqual(len(cat_facet.values), 0)

    def test_facet_field(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 19083,
    "start": 0,
    "docs": []
  },
  "facet_counts": {
    "facet_queries": {},
    "facet_fields": {
      "cat": [
        "3540208",
        19083,
        "1",
        0,
        "1001",
        0,
        "100101",
        0,
        "100102",
        0
      ],
      "cat_ex": [
        "29",
        147163,
        "64101",
        43375,
        "628",
        30043,
        "3540208",
        19083,
        "61910",
        18397
      ]
    }
  }
}
'''
            obj_mapper = Mock(wraps=_obj_mapper)
            
            q = self.searcher.search()
            q = q.filter(category=3540208, _local_params={'tag': 'cat'})
            q = q.facet(limit=5)
            q = q.facet_field('category', _instance_mapper=obj_mapper,
                              _local_params={'key': 'cat'}, _type=Integer)
            q = q.facet_field('category', _instance_mapper=obj_mapper,
                              _local_params={'ex': 'cat', 'key': 'cat_ex'})

            raw_query = str(q)

            self.assertIn('fq={!tag=cat}category:3540208', raw_query)
            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.limit=5', raw_query)
            self.assertIn('facet.field={!key=cat}category', raw_query)
            self.assertIn('facet.field={!ex=cat key=cat_ex}category', raw_query)

            r = q.results

            cat_facet = r.get_facet_field('cat')
            self.assertEqual(len(cat_facet.values), 5)
            self.assertEqual(cat_facet.values[0].value, 3540208)
            self.assertEqual(cat_facet.values[0].count, 19083)
            self.assertEqual(cat_facet.values[0].instance, (3540208, '3540208 3540208'))
            self.assertEqual(cat_facet.values[1].value, 1)
            self.assertEqual(cat_facet.values[1].count, 0)
            self.assertEqual(cat_facet.values[1].instance, (1, '1 1'))
            cat_ex_facet = r.get_facet_field('cat_ex')
            self.assertEqual(len(cat_ex_facet.values), 5)
            self.assertEqual(cat_ex_facet.values[0].value, '29')
            self.assertEqual(cat_ex_facet.values[0].count, 147163)
            self.assertEqual(cat_ex_facet.values[0].instance, ('29', '29 29'))
            self.assertEqual(cat_ex_facet.values[3].value, '3540208')
            self.assertEqual(cat_ex_facet.values[3].count, 19083)
            self.assertEqual(cat_ex_facet.values[3].instance, ('3540208', '3540208 3540208'))

            self.assertEqual(obj_mapper.call_count, 1)
            
    def test_facet_range(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 19083,
    "start": 0,
    "docs": []
  },
  "facet_counts": {
    "facet_queries": {},
    "facet_ranges": {
      "price": {
        "counts": [
          "0.0",
          1370,
          "30.0",
          404,
          "60.0",
          207,
          "90.0",
          132
        ],
        "gap": 30,
        "start": 0,
        "end": 120
      },
      "date_modified": {
        "counts": [
          "2013-05-29T00:00:00Z",
          143,
          "2013-05-30T00:00:00Z",
          29,
          "2013-05-31T00:00:00Z",
          573,
          "2013-06-01T00:00:00Z",
          502,
          "2013-06-02T00:00:00Z",
          0,
          "2013-06-03T00:00:00Z",
          480,
          "2013-06-04T00:00:00Z",
          400,
          "2013-06-05T00:00:00Z",
          0
        ],
        "gap": "+1DAY",
        "start": "2013-05-29T00:00:00Z",
        "end": "2013-06-06T00:00:00Z"
      }
    }
  }
}
'''
            q = self.searcher.search()
            q = q.facet_range('price_unit', start=0, end=100, gap=30,
                              _local_params={'ex': 'price', 'key': 'price'},
                              _type=Float,
            )
            q = q.facet_range('date_modified',
                              start='NOW/DAY-7DAYS', end='NOW/DAY+1DAY', gap='+1DAY',
                              _type=DateTime,
            )

            raw_query = str(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.range={!ex=price key=price}price_unit', raw_query)
            self.assertIn('f.price_unit.facet.range.start=0', raw_query)
            self.assertIn('f.price_unit.facet.range.end=100', raw_query)
            self.assertIn('f.price_unit.facet.range.gap=30', raw_query)
            self.assertIn('facet.range=date_modified', raw_query)
            self.assertIn('f.date_modified.facet.range.start=NOW/DAY-7DAYS', raw_query)
            self.assertIn('f.date_modified.facet.range.end=NOW/DAY%2B1DAY', raw_query)
            self.assertIn('f.date_modified.facet.range.gap=%2B1DAY', raw_query)

            r = q.results

            price_facet = r.get_facet_range('price')
            self.assertEqual(price_facet.start, 0)
            self.assertEqual(price_facet.end, 120)
            self.assertEqual(price_facet.gap, 30)
            self.assertEqual(len(price_facet.values), 4)
            self.assertAlmostEqual(price_facet.values[0].start, 0)
            self.assertAlmostEqual(price_facet.values[0].end, 30)
            self.assertEqual(price_facet.values[0].count, 1370)
            self.assertAlmostEqual(price_facet.values[1].start, 30)
            self.assertAlmostEqual(price_facet.values[1].end, 60)
            self.assertEqual(price_facet.values[1].count, 404)
            self.assertAlmostEqual(price_facet.values[2].start, 60)
            self.assertAlmostEqual(price_facet.values[2].end, 90)
            self.assertEqual(price_facet.values[2].count, 207)
            self.assertAlmostEqual(price_facet.values[3].start, 90)
            self.assertAlmostEqual(price_facet.values[3].end, 120)
            self.assertEqual(price_facet.values[3].count, 132)
            
            date_facet = r.get_facet_range('date_modified')
            self.assertEqual(date_facet.start, datetime(2013, 5, 29))
            self.assertEqual(date_facet.end, datetime(2013, 6, 6))
            self.assertEqual(date_facet.gap, '+1DAY')
            self.assertEqual(len(date_facet.values), 8)
            self.assertEqual(date_facet.values[0].start, datetime(2013, 5, 29))
            self.assertEqual(date_facet.values[0].end, datetime(2013, 5, 30))
            self.assertEqual(date_facet.values[0].count, 143)
            self.assertEqual(date_facet.values[1].start, datetime(2013, 5, 30))
            self.assertEqual(date_facet.values[1].end, datetime(2013, 5, 31))
            self.assertEqual(date_facet.values[1].count, 29)
            self.assertEqual(date_facet.values[2].start, datetime(2013, 5, 31))
            self.assertEqual(date_facet.values[2].end, datetime(2013, 6, 1))
            self.assertEqual(date_facet.values[2].count, 573)
            self.assertEqual(date_facet.values[3].start, datetime(2013, 6, 1))
            self.assertEqual(date_facet.values[3].end, datetime(2013, 6, 2))
            self.assertEqual(date_facet.values[3].count, 502)
            self.assertEqual(date_facet.values[4].start, datetime(2013, 6, 2))
            self.assertEqual(date_facet.values[4].end, datetime(2013, 6, 3))
            self.assertEqual(date_facet.values[4].count, 0)
            self.assertEqual(date_facet.values[5].start, datetime(2013, 6, 3))
            self.assertEqual(date_facet.values[5].end, datetime(2013, 6, 4))
            self.assertEqual(date_facet.values[5].count, 480)
            self.assertEqual(date_facet.values[6].start, datetime(2013, 6, 4))
            self.assertEqual(date_facet.values[6].end, datetime(2013, 6, 5))
            self.assertEqual(date_facet.values[6].count, 400)
            self.assertEqual(date_facet.values[7].start, datetime(2013, 6, 5))
            self.assertEqual(date_facet.values[7].end, datetime(2013, 6, 6))
            self.assertEqual(date_facet.values[7].count, 0)

    def test_facet_query(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 19083,
    "start": 0,
    "docs": []
  },
  "facet_counts": {
    "facet_queries": {
      "{!ex=dt}date_modified:[NOW-1MONTH TO *]": 16184,
      "dt_year": 16184,
      "dt_year_ex": 19083,
      "date_modified:[NOW/DAY TO *]": 135,
      "dt_week": 6631
    }
  }
}
'''

            q = self.searcher.search()
            q = q.filter(date_modified__gte='NOW-1MONTH', _local_params={'tag': 'dt'})
            q = q.facet_query(X(date_modified__gte='NOW/DAY'))
            q = q.facet_query(X(date_modified__gte='NOW-7DAYS'), _local_params={'key': 'dt_week'})
            q = q.facet_query(X(date_modified__gte='NOW-1MONTH'), _local_params={'ex': 'dt'})
            q = q.facet_query(X(date_modified__gte='NOW-1YEAR'), _local_params={'key': 'dt_year'})
            q = q.facet_query(X(date_modified__gte='NOW-1YEAR'), _local_params={'ex': 'dt', 'key': 'dt_year_ex'})

            raw_query = str(q)
            
            self.assertIn('fq={!tag=dt}date_modified:[NOW-1MONTH TO *]', raw_query)
            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.query=date_modified:[NOW/DAY TO *]', raw_query)
            self.assertIn('facet.query={!key=dt_week}date_modified:[NOW-7DAYS TO *]', raw_query)
            self.assertIn('facet.query={!ex=dt}date_modified:[NOW-1MONTH TO *]', raw_query)
            self.assertIn('facet.query={!key=dt_year}date_modified:[NOW-1YEAR TO *]', raw_query)
            self.assertIn('facet.query={!ex=dt key=dt_year_ex}date_modified:[NOW-1YEAR TO *]', raw_query)

            r = q.results

            today_facet = r.get_facet_query('date_modified:[NOW/DAY TO *]')
            self.assertEqual(today_facet.count, 135)
            today_facet = r.get_facet_query(X(date_modified__gte='NOW/DAY'))
            self.assertEqual(today_facet.count, 135)
            week_facet = r.get_facet_query('dt_week')
            self.assertEqual(week_facet.count, 6631)
            month_facet = r.get_facet_query(X(date_modified__gte='NOW-1MONTH'), {'ex': 'dt'})
            self.assertEqual(month_facet.count, 16184)
            year_facet = r.get_facet_query('dt_year')
            self.assertEqual(year_facet.count, 16184)
            year_ex_facet = r.get_facet_query('dt_year_ex')
            self.assertEqual(year_ex_facet.count, 19083)

    def test_facet_pivot(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 88318,
    "start": 0,
    "docs": []
  },
  "facet_counts": {
    "facet_queries": {},
    "facet_fields": {},
    "facet_dates": {},
    "facet_ranges": {},
    "facet_pivot": {
      "tcv": [
        {
          "field": "type",
          "value": "B",
          "count": 88203,
          "pivot": [
            {
              "field": "category",
              "value": "14210102",
              "count": 13801,
              "pivot": [
                {
                  "field": "visible",
                  "value": true,
                  "count": 11159
                },
                {
                  "field": "visible",
                  "value": false,
                  "count": 2642
                }
              ]
            },
            {
              "field": "category",
              "value": "14210101",
              "count": 2379,
              "pivot": [
                {
                  "field": "visible",
                  "value": true,
                  "count": 2366
                },
                {
                  "field": "visible",
                  "value": false,
                  "count": 13
                }
              ]
            },
            {
              "field": "category",
              "value": "607",
              "count": 1631,
              "pivot": [
                {
                  "field": "visible",
                  "value": true,
                  "count": 1462
                },
                {
                  "field": "visible",
                  "value": false,
                  "count": 169
                }
              ]
            }
          ]
        },
        {
          "field": "type",
          "value": "C",
          "count": 82421,
          "pivot": [
            {
              "field": "category",
              "value": "14210102",
              "count": 13801,
              "pivot": [
                {
                  "field": "visible",
                  "value": true,
                  "count": 11159
                },
                {
                  "field": "visible",
                  "value": false,
                  "count": 2642
                }
              ]
            },
            {
              "field": "category",
              "value": "14210101",
              "count": 2379,
              "pivot": [
                {
                  "field": "visible",
                  "value": true,
                  "count": 2366
                },
                {
                  "field": "visible",
                  "value": false,
                  "count": 13
                }
              ]
            },
            {
              "field": "category",
              "value": "607",
              "count": 1631,
              "pivot": [
                {
                  "field": "visible",
                  "value": true,
                  "count": 1462
                },
                {
                  "field": "visible",
                  "value": false,
                  "count": 169
                }
              ]
            }
          ]
        },
        {
          "field": "type",
          "value": "S",
          "count": 3,
          "pivot": []
        }
      ]
    }
  }
}
'''
            obj_mapper = Mock(wraps=_obj_mapper)

            q = self.searcher.search()
            q = q.facet_pivot(
                ('type', dict(_instance_mapper=obj_mapper)),
                ('category', dict(_instance_mapper=obj_mapper, _type=Integer, limit=3)),
                'visible',
                _local_params=LocalParams(ex='type,category', key='tcv'),
            )

            raw_query = str(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.pivot={!ex=type,category key=tcv}type,category,visible', raw_query)
            self.assertIn('f.category.facet.limit=3', raw_query)

            r = q.results
            facet = r.get_facet_pivot('tcv')
            self.assertListEqual(facet.field_names, ['type', 'category', 'visible'])
            self.assertEqual(facet.field, 'type')
            self.assertEqual(facet.values[0].value, 'B')
            self.assertEqual(facet.values[0].instance, ('B', 'B B'))
            self.assertEqual(facet.values[0].count, 88203)
            self.assertEqual(facet.values[0].pivot.field, 'category')
            self.assertEqual(facet.values[0].pivot.values[0].value, 14210102)
            self.assertEqual(facet.values[0].pivot.values[0].count, 13801)
            self.assertEqual(facet.values[0].pivot.values[0].instance, (14210102, '14210102 14210102'))
            self.assertEqual(facet.values[0].pivot.values[0].pivot.field, 'visible')
            self.assertEqual(facet.values[0].pivot.values[0].pivot.values[0].value, True)
            self.assertEqual(facet.values[0].pivot.values[0].pivot.values[0].count, 11159)
            self.assertEqual(facet.values[0].pivot.values[0].pivot.values[1].value, False)
            self.assertEqual(facet.values[0].pivot.values[0].pivot.values[1].count, 2642)
            self.assertEqual(facet.values[1].value, 'C')
            self.assertEqual(facet.values[1].count, 82421)
            self.assertEqual(id(facet.values[0].pivot.values[0].instance),
                             id(facet.values[1].pivot.values[0].instance))
            self.assertEqual(facet.values[1].pivot.get_value(607).value, 607)
            self.assertEqual(facet.values[1].pivot.get_value(607).count, 1631)
            self.assertEqual(facet.values[1].pivot.get_value(607).pivot.get_value(True).count, 1462)
            self.assertEqual(facet.values[1].pivot.get_value(607).pivot.get_value(False).count, 169)
            self.assertEqual(facet.values[2].value, 'S')
            self.assertEqual(facet.values[2].count, 3)
            self.assertRaises(IndexError, lambda: facet.values[3])

            self.assertEqual(obj_mapper.call_count, 1)

    def test_group_query(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "grouped": {
    "price:[1000 TO 2000]": {
      "matches": 55,
      "doclist": {
        "numFound": 7,
        "start": 0,
        "docs": [
          {
            "id": "12",
            "name": "Test name"
          },
          {
            "id": "13"
          }
        ]
      }
    },
    "status:666": {
      "matches": 55,
      "doclist": {
        "numFound": 0,
        "start": 0,
        "docs": []
      }
    },
    "(visible:true OR status:1)": {
      "matches": 55,
      "doclist": {
        "numFound": 1,
        "start": 0,
        "docs": [
          {
            "id": "4",
            "name": "Test 2"
          }
        ]
      }
    }
  }
}
'''
            q = (
                self.searcher.search()
                .group(limit=2)
                .group_query(price__range=[1000, 2000])
                .group_query(status=666)
                .group_query(X(visible=True) | X(status=1))
            )
            raw_query = str(q)

            self.assertIn('group=true', raw_query)
            self.assertIn('group.limit=2', raw_query)
            self.assertIn('group.query=price:[1000 TO 2000]', raw_query)
            self.assertIn('group.query=status:666', raw_query)
            self.assertIn('group.query=(visible:true OR status:1)', raw_query)

            r = q.results
            price_grouped = r.get_grouped(X(price__range=[1000, 2000]))
            self.assertEqual(price_grouped.matches, 55)
            self.assertEqual(price_grouped.ndocs, 7)
            self.assertEqual(price_grouped.start, 0)
            self.assertEqual(len(price_grouped.docs), 2)
            self.assertEqual(price_grouped.docs[0].id, '12')
            self.assertEqual(price_grouped.docs[0].name, 'Test name')
            self.assertEqual(price_grouped.docs[1].id, '13')
            status_grouped = r.get_grouped(X(status=666))
            self.assertEqual(status_grouped.matches, 55)
            self.assertEqual(status_grouped.ndocs, 0)
            self.assertEqual(status_grouped.start, 0)
            self.assertEqual(len(status_grouped.docs), 0)
            other_grouped = r.get_grouped(X(visible=True) | X(status=1))
            self.assertEqual(other_grouped.matches, 55)
            self.assertEqual(other_grouped.ndocs, 1)
            self.assertEqual(other_grouped.start, 0)
            self.assertEqual(len(other_grouped.docs), 1)
            self.assertEqual(other_grouped.docs[0].id, '4')
            self.assertEqual(other_grouped.docs[0].name, 'Test 2')

    def test_group_func(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "grouped": {
    "termfreq(name,'test')": {
      "matches": 63,
      "ngroups": 3,
      "groups": [
        {
          "groupValue": 0,
          "doclist": {
            "numFound": 418,
            "start": 0,
            "docs": [
              {
                "id": "4"
              },
              {
                "id": "6"
              }
            ]
          }
        },
        {
          "groupValue": 1,
          "doclist": {
            "numFound": 13,
            "start": 0,
            "docs": [
              {
                "id": "70"
              },
              {
                "id": "84"
              }
            ]
          }
        },
        {
          "groupValue": 2,
          "doclist": {
            "numFound": 1,
            "start": 0,
            "docs": [
              {
                "id": "57"
              }
            ]
          }
        }
      ]
    },
    "sum(product(company,1000),status)": {
      "matches": 63,
      "ngroups": 18,
      "groups": [
        {
          "groupValue": 6000,
          "doclist": {
            "numFound": 7,
            "start": 0,
            "docs": [
              {
                "id": "4"
              },
              {
                "id": "6"
              }
            ]
          }
        },
        {
          "groupValue": 10000,
          "doclist": {
            "numFound": 3,
            "start": 0,
            "docs": [
              {
                "id": "80"
              },
              {
                "id": "26"
              }
            ]
          }
        },
        {
          "groupValue": 8000,
          "doclist": {
            "numFound": 9,
            "start": 0,
            "docs": [
              {
                "id": "5"
              },
              {
                "id": "22"
              }
            ]
          }
        },
        {
          "groupValue": 7000,
          "doclist": {
            "numFound": 70,
            "start": 0,
            "docs": [
              {
                "id": "14323732"
              },
              {
                "id": "13737185"
              }
            ]
          }
        },
        {
          "groupValue": 1006,
          "doclist": {
            "numFound": 1,
            "start": 0,
            "docs": [
              {
                "id": "9"
              }
            ]
          }
        }
      ]
    }
  }
}
'''
            q = (
                self.searcher.search()
                .group(limit=2)
                .group_func(func.termfreq('name', "'test'"))
                .group_func(func.sum(func.product('company', 1000), 'status'))
                .limit(5)
            )
            raw_query = str(q)

            self.assertIn('group=true', raw_query)
            self.assertIn('group.limit=2', raw_query)
            self.assertIn('group.ngroups=true', raw_query)
            self.assertIn("group.func=termfreq(name,'test')", raw_query)
            self.assertIn('group.func=sum(product(company,1000),status)', raw_query)
            self.assertIn('rows=5', raw_query)

            r = q.results
            term_grouped = r.get_grouped(func.termfreq('name', "'test'"))
            self.assertEqual(term_grouped.matches, 63)
            self.assertEqual(term_grouped.ngroups, 3)
            self.assertEqual(term_grouped.ndocs, None)
            self.assertEqual(term_grouped.start, None)
            self.assertEqual(len(term_grouped.docs), 0)
            self.assertEqual(len(term_grouped.groups), 3)
            self.assertEqual(term_grouped.groups[0].value, 0)
            self.assertEqual(term_grouped.groups[0].ndocs, 418)
            self.assertEqual(term_grouped.groups[0].start, 0)
            self.assertEqual(len(term_grouped.groups[0].docs), 2)
            self.assertEqual(term_grouped.groups[0].docs[0].id, '4')
            self.assertEqual(term_grouped.groups[0].docs[1].id, '6')
            self.assertEqual(term_grouped.groups[1].value, 1)
            self.assertEqual(term_grouped.groups[1].ndocs, 13)
            self.assertEqual(term_grouped.groups[1].start, 0)
            self.assertEqual(len(term_grouped.groups[1].docs), 2)
            self.assertEqual(term_grouped.groups[1].docs[0].id, '70')
            self.assertEqual(term_grouped.groups[1].docs[1].id, '84')
            self.assertEqual(term_grouped.groups[2].value, 2)
            self.assertEqual(term_grouped.groups[2].ndocs, 1)
            self.assertEqual(term_grouped.groups[2].start, 0)
            self.assertEqual(len(term_grouped.groups[2].docs), 1)
            self.assertEqual(term_grouped.groups[2].docs[0].id, '57')
            company_status_grouped = r.get_grouped(func.sum(func.product('company', 1000), 'status'))
            self.assertEqual(company_status_grouped.matches, 63)
            self.assertEqual(company_status_grouped.ngroups, 18)
            self.assertEqual(company_status_grouped.ndocs, None)
            self.assertEqual(company_status_grouped.start, None)
            self.assertEqual(len(company_status_grouped.groups), 5)
            self.assertEqual(company_status_grouped.groups[0].value, 6000)
            self.assertEqual(company_status_grouped.groups[0].ndocs, 7)
            self.assertEqual(company_status_grouped.groups[0].start, 0)
            self.assertEqual(len(company_status_grouped.groups[0].docs), 2)
            self.assertEqual(company_status_grouped.groups[0].docs[0].id, '4')
            self.assertEqual(company_status_grouped.groups[0].docs[1].id, '6')
            self.assertEqual(company_status_grouped.groups[-1].value, 1006)
            self.assertEqual(company_status_grouped.groups[-1].ndocs, 1)
            self.assertEqual(company_status_grouped.groups[-1].start, 0)
            self.assertEqual(len(company_status_grouped.groups[-1].docs), 1)
            self.assertEqual(company_status_grouped.groups[-1].docs[0].id, '9')

    def test_search_grouped_main(self):
        class TestSearcher(SolrSearcher):
            def instance_mapper(self, ids, db_query=None):
                return dict((id, Obj(int(id), '{} {}'.format(id, id)))
                            for id in ids)

        searcher = TestSearcher('http://example.com:8180/solr')
        with self.patch_send_request(searcher) as send_request:
            send_request.return_value = '''{
  "grouped":{
    "company":{
      "matches":281,
      "ngroups":109,
      "groups":[{
          "groupValue":"1",
          "doclist":{
            "numFound":9,
            "start":0,
            "docs":[
              {
                "id":"111",
                "name":"Test 1",
                "company":"1"},
              {
                "id":"222",
                "name":"Test 2",
                "company":"1"},
              {
                "id":"333",
                "name":"Test 3",
                "company":"1"}]
          }},
        {
          "groupValue":"3",
          "doclist":{"numFound":1,"start":0,"docs":[
              {
                "id":"555",
                "name":"Test 5",
                "company":"3"}]
          }}]}},
  "facet_counts":{
    "facet_queries":{
      "{!ex=price cache=false}price:[* TO 100]":0},
    "facet_fields":{
      "category":[
        "1",5,
        "2",2],
      "tag":[
        "100",10,
        "200",20,
        "1000",30]},
    "facet_dates":{},
    "facet_ranges":{}},
  "stats":{
    "stats_fields":{
      "price":{
        "min":3.5,
        "max":892.0,
        "count":1882931,
        "missing":556686,
        "sum":5.677964302447648E13,
        "sumOfSquares":2.452218850256837E26,
        "mean":3.0154924967763808E7,
        "stddev":1.1411980204045008E10}}}}'''

            q = searcher.search()
            q = q.facet_field('category', mincount=5, limit=10,
                              type=Integer,
                              _local_params={'ex': 'category'},
                              _instance_mapper=_obj_mapper)
            q = q.facet_field('tag', _local_params={'ex': 'tag'})
            q = q.facet_query(price__lte=100,
                              _local_params=[('ex', 'price'), ('cache', False)])
            q = q.group(limit=3)
            q = q.group_field('company', _instance_mapper=_obj_mapper, _type=Integer)
            q = q.filter(category=13, _local_params={'tag': 'category'})
            q = q.stats('price')
            q = q.order_by('-date_created')
            q = q.offset(48).limit(24)
            raw_query = str(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.field={!ex=category}category', raw_query)
            self.assertIn('f.category.facet.mincount=5', raw_query)
            self.assertIn('f.category.facet.limit=10', raw_query)
            self.assertIn('facet.field={!ex=tag}tag', raw_query)
            self.assertIn('facet.query={!ex=price cache=false}price:[* TO 100]', raw_query)
            self.assertIn('group=true', raw_query)
            self.assertIn('group.ngroups=true', raw_query)
            self.assertIn('group.limit=3', raw_query)
            self.assertIn('group.field=company', raw_query)
            self.assertIn('fq={!tag=category}category:13', raw_query)
            self.assertIn('stats=true', raw_query)
            self.assertIn('stats.field=price', raw_query)
            self.assertIn('sort=date_created desc', raw_query)
            self.assertIn('start=48', raw_query)
            self.assertIn('rows=24', raw_query)

            r = q.results
            grouped = r.get_grouped('company')
            self.assertEqual(grouped.ngroups, 109)
            self.assertEqual(grouped.matches, 281)
            self.assertEqual(grouped.groups[0].ndocs, 9)
            self.assertEqual(grouped.groups[0].value, 1)
            self.assertEqual(grouped.groups[0].instance.name, '1 1')
            self.assertEqual(grouped.groups[0].docs[0].id, '111')
            self.assertEqual(grouped.groups[0].docs[0].name, 'Test 1')
            self.assertEqual(grouped.groups[0].docs[0].instance.id, 111)
            self.assertEqual(grouped.groups[0].docs[0].instance.name, '111 111')
            self.assertEqual(grouped.groups[0].docs[-1].id, '333')
            self.assertEqual(grouped.groups[0].docs[-1].name, 'Test 3')
            self.assertEqual(grouped.groups[0].docs[-1].instance.id, 333)
            self.assertEqual(grouped.groups[0].docs[-1].instance.name, '333 333')
            self.assertEqual(grouped.groups[1].ndocs, 1)
            self.assertEqual(grouped.groups[1].value, 3)
            self.assertEqual(grouped.groups[1].instance.name, '3 3')
            self.assertEqual(grouped.groups[1].docs[0].id, '555')
            self.assertEqual(grouped.groups[1].docs[0].name, 'Test 5')
            self.assertEqual(grouped.groups[1].docs[0].instance.id, 555)
            self.assertEqual(grouped.groups[1].docs[0].instance.name, '555 555')
            self.assertEqual(len(grouped.docs), 0)
            
            self.assertEqual(len(r.facet_fields), 2)

            category_facet = r.get_facet_field('category')
            self.assertEqual(len(category_facet.values), 2)
            self.assertEqual(category_facet.values[0].value, 1)
            self.assertEqual(category_facet.values[0].count, 5)
            self.assertEqual(category_facet.values[0].instance, (1, '1 1'))
            self.assertEqual(category_facet.values[1].value, 2)
            self.assertEqual(category_facet.values[1].count, 2)
            self.assertEqual(category_facet.values[1].instance, (2, '2 2'))

            tag_facet = r.get_facet_field('tag')
            self.assertEqual(len(tag_facet.values), 3)
            self.assertEqual(tag_facet.values[-1].value, '1000')
            self.assertEqual(tag_facet.values[-1].count, 30)
            self.assertEqual(len(r.facet_queries), 1)

            price_stats = r.get_stats_field('price')
            self.assertEqual(len(r.stats_fields), 1)
            self.assertEqual(price_stats.min, 3.5)
            self.assertEqual(price_stats.max, 892.0)
            self.assertEqual(price_stats.count, 1882931)
            self.assertEqual(price_stats.missing, 556686)

    def test_search_grouped_simple(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''{
  "grouped": {
    "company": {
        "matches": 3657093,
        "ngroups": 216036,
        "doclist": {
          "numFound": 3657093,
          "start": 0,
          "docs": [
            {
              "id":"111",
              "name":"Test 1",
              "company":"1"},
            {
              "id":"222",
              "name":"Test 2",
              "company":"1"},
            {
              "id":"333",
              "name":"Test 3",
              "company":"1"},
            {
              "id":"555",
              "name":"Test 5",
              "company":"3"}]}}}}'''

            q = self.searcher.search()
            q = q.group('company', limit=3, format='simple')
            raw_query = str(q)

            self.assertIn('group=true', raw_query)
            self.assertIn('group.limit=3', raw_query)
            self.assertIn('group.format=simple', raw_query)
            self.assertIn('group.field=company', raw_query)

            r = q.results
            grouped = r.get_grouped('company')
            self.assertEqual(grouped.ngroups, 216036)
            self.assertEqual(grouped.ndocs, 3657093)
            self.assertEqual(len(grouped.docs), 4)
            self.assertEqual(grouped.docs[0].id, '111')
            self.assertEqual(grouped.docs[0].name, 'Test 1')
            self.assertEqual(grouped.docs[2].id, '333')
            self.assertEqual(grouped.docs[2].name, 'Test 3')
            self.assertEqual(grouped.docs[3].id, '555')
            self.assertEqual(grouped.docs[3].name, 'Test 5')

    def test_stats(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 56,
    "start": 0,
    "docs": []
  },
  "stats": {
    "stats_fields": {
      "price": {
        "min": 1,
        "max": 5358,
        "count": 14,
        "missing": 5,
        "sum": 27999.20001220703,
        "sumOfSquares": 84656303.06075683,
        "mean": 1999.942858014788,
        "stddev": 1484.7818530839374,
        "facets": {
          "visible": {
            "true": {
              "min": 1,
              "max": 5358,
              "count": 14,
              "missing": 5,
              "sum": 27999.20001220703,
              "sumOfSquares": 84656303.06075683,
              "mean": 1999.942858014788,
              "stddev": 1484.7818530839374,
              "facets": {}
            }
          },
          "category": {
            "11": {
              "min": 1,
              "max": 1,
              "count": 1,
              "missing": 0,
              "sum": 1,
              "sumOfSquares": 1,
              "mean": 1,
              "stddev": 0,
              "facets": {}
            },
            "21": {
              "min": 99,
              "max": 5358,
              "count": 13,
              "missing": 5,
              "sum": 27998.20001220703,
              "sumOfSquares": 84656302.06075683,
              "mean": 2153.707693246695,
              "stddev": 1424.674328206475,
              "facets": {}
            },
            "66": {
              "min": "Infinity",
              "max": "-Infinity",
              "count": 0,
              "missing": 1,
              "sum": 0,
              "sumOfSquares": 0,
              "mean": "NaN",
              "stddev": 0,
              "facets": {}
            }
          }
        }
      }
    }
  }
}'''

            q = (
                self.searcher.search()
                .stats('price', facet_fields=['visible', ('category', _obj_mapper)])
            )

            raw_query = str(q)

            self.assertIn('stats=true', raw_query)
            self.assertIn('stats.field=price', raw_query)
            self.assertIn('f.price.stats.facet=visible', raw_query)
            self.assertIn('f.price.stats.facet=category', raw_query)

            r = q.results
            s = r.get_stats_field('price')
            self.assertEqual(s.count, 14)
            self.assertEqual(s.missing, 5)
            self.assertAlmostEqual(s.min, 1.)
            self.assertAlmostEqual(s.max, 5358.)
            self.assertAlmostEqual(s.sum, 27999.20001220703)
            self.assertAlmostEqual(s.sum_of_squares, 84656303.06075683)
            self.assertAlmostEqual(s.mean, 1999.942858014788)
            self.assertAlmostEqual(s.stddev, 1484.7818530839374)

            visible_facet = s.get_facet('visible')
            self.assertEqual(len(visible_facet.values), 1)
            self.assertEqual(visible_facet.get_value('true').count, 14)
            self.assertEqual(visible_facet.get_value('true').missing, 5)
            self.assertAlmostEqual(visible_facet.get_value('true').min, 1.)
            self.assertAlmostEqual(visible_facet.get_value('true').max, 5358.)
            self.assertEqual(visible_facet.get_value('true').instance, None)

            category_facet = s.get_facet('category')
            self.assertEqual(len(category_facet.values), 3)
            self.assertEqual(category_facet.get_value('11').count, 1)
            self.assertEqual(category_facet.get_value('11').missing, 0)
            self.assertEqual(category_facet.get_value('11').instance.id, '11')
            self.assertEqual(category_facet.get_value('11').instance.name, '11 11')
            self.assertEqual(category_facet.get_value('21').count, 13)
            self.assertEqual(category_facet.get_value('21').missing, 5)
            self.assertAlmostEqual(category_facet.get_value('21').min, 99.)
            self.assertAlmostEqual(category_facet.get_value('21').max, 5358.)
            self.assertAlmostEqual(category_facet.get_value('21').sum, 27998.20001220703)
            self.assertAlmostEqual(category_facet.get_value('21').sum_of_squares, 84656302.06075683)
            self.assertAlmostEqual(category_facet.get_value('21').mean, 2153.707693246695)
            self.assertAlmostEqual(category_facet.get_value('21').stddev, 1424.674328206475)
            self.assertEqual(category_facet.get_value('21').instance.id, '21')
            self.assertEqual(category_facet.get_value('21').instance.name, '21 21')
            self.assertEqual(category_facet.get_value('66').count, 0)
            self.assertEqual(category_facet.get_value('66').missing, 1)
            self.assertEqual(category_facet.get_value('66').min, float('Inf'))
            self.assertEqual(category_facet.get_value('66').max, float('-Inf'))
            self.assertAlmostEqual(category_facet.get_value('66').sum, 0.)
            self.assertAlmostEqual(category_facet.get_value('66').sum_of_squares, 0.)
            self.assertEqual(str(category_facet.get_value('66').mean), 'nan')
            self.assertAlmostEqual(category_facet.get_value('66').stddev, 0.)
            self.assertEqual(category_facet.get_value('66').instance.id, '66')
            self.assertEqual(category_facet.get_value('66').instance.name, '66 66')

        # empty stats
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 0,
    "start": 0,
    "docs": []
  },
  "stats": {
    "stats_fields": {
      "price": null
    }
  }
}'''

            q = self.searcher.search().stats('price')

            raw_query = str(q)

            self.assertIn('stats=true', raw_query)
            self.assertIn('stats.field=price', raw_query)

            r = q.results
            s = r.get_stats_field('price')
            self.assertEqual(s.count, None)
            self.assertEqual(s.missing, None)
            self.assertAlmostEqual(s.min, None)
            self.assertAlmostEqual(s.max, None)
            self.assertAlmostEqual(s.sum, None)
            self.assertAlmostEqual(s.sum_of_squares, None)
            self.assertAlmostEqual(s.mean, None)
            self.assertAlmostEqual(s.stddev, None)
            
        # empty stats facet
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 0,
    "start": 0,
    "docs": []
  },
  "stats": {
    "stats_fields": {
      "price": {
        "min": 1,
        "max": 5358,
        "count": 14,
        "missing": 5,
        "sum": 27999.20001220703,
        "sumOfSquares": 84656303.06075683,
        "mean": 1999.942858014788,
        "stddev": 1484.7818530839374,
        "facets": {}
      }
    }
  }
}'''

            q = self.searcher.search().stats('price', facet_fields=['model'])

            raw_query = str(q)

            self.assertIn('stats=true', raw_query)
            self.assertIn('stats.field=price', raw_query)
            self.assertIn('f.price.stats.facet=model', raw_query)

            r = q.results
            s = r.get_stats_field('price')

            self.assertEqual(s.count, 14)
            self.assertEqual(s.missing, 5)
            self.assertAlmostEqual(s.min, 1.)
            self.assertAlmostEqual(s.max, 5358.)
            self.assertAlmostEqual(s.sum, 27999.20001220703)
            self.assertAlmostEqual(s.sum_of_squares, 84656303.06075683)
            self.assertAlmostEqual(s.mean, 1999.942858014788)
            self.assertAlmostEqual(s.stddev, 1484.7818530839374)

            model_facet = s.get_facet('model')
            self.assertEqual(len(model_facet.values), 0)

    def test_results_exc(self):
        q = self.searcher.search().stats('price')
        with patch.object(q, '_fetch_results') as _fetch_results:
            _fetch_results.side_effect = AttributeError('no such attribute')

            with self.assertRaises(RuntimeError) as cm:
                q.results
            self.assertEqual(cm.exception.args, ('AttributeError', 'no such attribute'))

    def test_highlight(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 10,
    "start": 0,
    "docs": [
      {
        "id": "2919"
      },
      {
        "id": "14141"
      },
      {
        "id": "13125"
      }
    ]
  },
  "highlighting": {
    "2919": {
      "description": [
        "{em}Test's{/em}"
      ],
      "name": [
        "Highlight {em}test{/em}"
      ]
    },
    "13125": {
      "name": [
        "Simple {em}test{/em}",
        "More {em}tests{/em}"
      ]
    },
    "14141": {
      "name": [
        "Real highlighting {em}test{/em}"
      ]
    }
  }
}'''

            q = (
                self.searcher.search('test')
                .highlight('name', 'description',
                           snippets=2,
                           simple_pre='{em}', simple_post='{/em}')
            )

            raw_query = str(q)

            self.assertIn('hl=true', raw_query)
            self.assertIn('hl.fl=name,description', raw_query)
            self.assertIn('hl.snippets=2', raw_query)
            self.assertIn('hl.simple.pre={em}', raw_query)
            self.assertIn('hl.simple.post={/em}', raw_query)

            r = q.results

            self.assertEqual(r.docs[0].highlighted,
                             {'name': ['Highlight {em}test{/em}'],
                              'description': ['{em}Test\'s{/em}']})
            self.assertEqual(r.docs[1].highlighted,
                             {'name': ['Real highlighting {em}test{/em}']})
            self.assertEqual(r.docs[2].highlighted,
                             {'name': ['Simple {em}test{/em}',
                                       'More {em}tests{/em}']})

            q = q.highlight(None)

            raw_query = str(q)

            self.assertNotIn('hl=true', raw_query)
            self.assertNotIn('hl.fl=name,description', raw_query)
            self.assertNotIn('hl.snippets=2', raw_query)
            self.assertNotIn('hl.simple.pre={em}', raw_query)
            self.assertNotIn('hl.simple.post={/em}', raw_query)
