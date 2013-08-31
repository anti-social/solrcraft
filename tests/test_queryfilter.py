from __future__ import unicode_literals

from datetime import datetime
from collections import namedtuple

from solar import X, LocalParams
from solar.types import Integer, Float, Boolean
from solar.compat import force_unicode
from solar.searcher import SolrSearcher
from solar.queryfilter import (
    QueryFilter, Filter, FacetFilter, FacetFilterValue,
    PivotFilter, FacetPivotFilter,
    FacetQueryFilter, FacetQueryFilterValue, RangeFilter,
    OrderingFilter, OrderingValue)

from .base import TestCase


Obj = namedtuple('Obj', ['id', 'name'])

def _obj_mapper(ids):
    return dict((id, Obj(id, '{0} {0}'.format(id))) for id in ids)


class CategoryFilterValue(FacetFilterValue):
    pass


class CategoryFilter(FacetFilter):
    filter_value_cls = CategoryFilterValue
    
    def __init__(self, name, *args, **kwargs):
        super(CategoryFilter, self).__init__(name, *args, **kwargs)


class QueryFilterTest(TestCase):
    def test_apply_insane_params(self):
        # this test case shouldn't raise exception
        qf = QueryFilter()
        qf.add_filter(FacetFilter('test'))
        qf.add_filter(
            PivotFilter(
                'pivot_test',
                FacetPivotFilter('a'),
                FacetPivotFilter('b'),
            )
        )
        qf.add_ordering(
            OrderingFilter(
                'sort',
                OrderingValue('-score', '-score'),
                OrderingValue('price', 'price'),
                default='-score',
            )
        )

        q = self.searcher.search()

        params = {
            111: 222,
            '\ufffd': '',
            '\uffff'.encode('utf-8'): '',
            'test': ['\ufffd', '\uffff'.encode('utf-8')],
            'pivot_test': ['\ufffd', '\uffff'.encode('utf-8')],
            'sort': ['\ufffd', '\uffff'.encode('utf-8')],
        }
        q = qf.apply(q, params)
    
    def test_filter(self):
        q = self.searcher.search()

        qf = QueryFilter()
        qf.add_filter(Filter('country',
                             _local_params=LocalParams(tag='cc')))

        params = {
            'country': ['us', 'ru'],
        }

        q = qf.apply(q, params)
        raw_query = force_unicode(q)

        self.assertIn('fq={!tag=cc,country}(country:"us" OR country:"ru")', raw_query)

    def test_facet_filter(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "facet_counts": {
    "facet_queries": {},
    "facet_fields": {
      "cat": [
        "100", 500,
        "5", 10,
        "2", 5,
        "1", 2,
        "13", 1
      ],
      "region": [
        "kiev", 42,
        "bucha", 18
      ]
    },
    "facet_dates": {},
    "facet_ranges": {}
  }
}'''
        
            q = self.searcher.search()

            qf = QueryFilter()
            qf.add_filter(
                CategoryFilter(
                    'cat', 'category', mincount=1,
                    type=Integer,
                    _local_params={'cache': False, 'ex': ('test',)}))
            qf.add_filter(FacetFilter('region'))

            params = {
                'cat': ['5', '13'],
            }

            q = qf.apply(q, params)
            raw_query = force_unicode(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.field={!cache=false ex=test,cat key=cat}category', raw_query)
            self.assertIn('f.category.facet.mincount=1', raw_query)
            self.assertIn('facet.field={!key=region ex=region}region', raw_query)
            self.assertIn('fq={!cache=false ex=test tag=cat}(category:"5" OR category:"13")', raw_query)

            qf.process_results(q.results)

            category_filter = qf.get_filter('cat')
            self.assertIsInstance(category_filter, CategoryFilter)
            self.assertEqual(category_filter.name, 'cat')
            self.assertEqual(category_filter.field, 'category')
            self.assertIsInstance(category_filter.all_values[0], CategoryFilterValue)
            self.assertEqual(category_filter.all_values[0].value, 100)
            self.assertEqual(category_filter.all_values[0].count, 500)
            self.assertEqual(category_filter.all_values[0].count_plus, '+500')
            self.assertEqual(category_filter.all_values[0].selected, False)
            self.assertEqual(category_filter.all_values[0].title, '100')
            self.assertEqual(category_filter.all_values[1].value, 5)
            self.assertEqual(category_filter.all_values[1].count, 10)
            self.assertEqual(category_filter.all_values[1].count_plus, '10')
            self.assertEqual(category_filter.all_values[1].selected, True)
            self.assertEqual(category_filter.all_values[2].value, 2)
            self.assertEqual(category_filter.all_values[2].count, 5)
            self.assertEqual(category_filter.all_values[2].count_plus, '+5')
            self.assertEqual(category_filter.all_values[2].selected, False)
            self.assertEqual(category_filter.all_values[3].value, 1)
            self.assertEqual(category_filter.all_values[3].count, 2)
            self.assertEqual(category_filter.all_values[3].count_plus, '+2')
            self.assertEqual(category_filter.all_values[3].selected, False)
            self.assertEqual(category_filter.all_values[4].value, 13)
            self.assertEqual(category_filter.all_values[4].count, 1)
            self.assertEqual(category_filter.all_values[4].count_plus, '1')
            self.assertEqual(category_filter.all_values[4].selected, True)

            region_filter = qf.get_filter('region')
            self.assertEqual(region_filter.all_values[0].value, 'kiev')
            self.assertEqual(region_filter.all_values[0].count, 42)
            self.assertEqual(region_filter.all_values[0].count_plus, '42')
            self.assertEqual(region_filter.all_values[1].value, 'bucha')
            self.assertEqual(region_filter.all_values[1].count, 18)
            self.assertEqual(region_filter.all_values[1].count_plus, '18')

    def test_facet_query_filter(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "facet_counts": {
    "facet_queries": {
      "date_created__today": 28,
      "date_created__week_ago": 105,
      "dist__d5": 0,
      "dist__d10": 12,
      "dist__d20": 40
    },
    "facet_fields": {},
    "facet_dates":{},
    "facet_ranges":{}
  }
}'''
        
            q = self.searcher.search()

            qf = QueryFilter()
            qf.add_filter(
                FacetQueryFilter(
                    'date_created',
                    FacetQueryFilterValue(
                        'today',
                        X(date_created__gte='NOW/DAY-1DAY'),
                        _local_params=LocalParams(ex='test'),
                        title='Only new',
                        help_text='Documents one day later'),
                    FacetQueryFilterValue(
                        'week_ago',
                        X(date_created__gte='NOW/DAY-7DAY'),
                        title='Week ago')))
            qf.add_filter(
                FacetQueryFilter(
                    'dist',
                    FacetQueryFilterValue(
                        'd5',
                        None, _local_params=LocalParams('geofilt', d=5, tag='d5')),
                    FacetQueryFilterValue(
                        'd10',
                        None, _local_params=LocalParams('geofilt', d=10, tag='d10')),
                    FacetQueryFilterValue(
                        'd20',
                        None, _local_params=LocalParams('geofilt', d=20, tag='d20')),
                    select_multiple=False))
            qf.add_ordering(
                OrderingFilter(
                    'sort',
                    OrderingValue('-score', '-score'),
                    OrderingValue('price', 'price'),
                    OrderingValue('-price', '-price')))

            params = {
                'cat': ['5', '13'],
                'country': ['us', 'ru'],
                'date_created': 'today',
                'dist': 'd10',
                }

            q = qf.apply(q, params)
            raw_query = force_unicode(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.query={!ex=test,date_created key=date_created__today}date_created:[NOW/DAY-1DAY TO *]', raw_query)
            self.assertIn('facet.query={!key=date_created__week_ago ex=date_created}date_created:[NOW/DAY-7DAY TO *]', raw_query)
            self.assertIn('facet.query={!geofilt d=5 tag=d5 key=dist__d5 ex=dist}', raw_query)
            self.assertIn('facet.query={!geofilt d=10 tag=d10 key=dist__d10 ex=dist}', raw_query)
            self.assertIn('facet.query={!geofilt d=20 tag=d20 key=dist__d20 ex=dist}', raw_query)
            self.assertIn('fq={!tag=date_created}date_created:[NOW/DAY-1DAY TO *]', raw_query)
            self.assertIn('fq={!geofilt d=10 tag=d10,dist}', raw_query)

            qf.process_results(q.results)

            date_created_filter = qf.get_filter('date_created')
            self.assertEqual(date_created_filter.get_value('today').count, 28)
            self.assertEqual(date_created_filter.get_value('today').selected, True)
            self.assertEqual(date_created_filter.get_value('today').title, 'Only new')
            self.assertEqual(date_created_filter.get_value('today').opts['help_text'], 'Documents one day later')
            self.assertEqual(date_created_filter.get_value('week_ago').count, 105)

            dist_filter = qf.get_filter('dist')
            self.assertEqual(dist_filter.get_value('d5').count, 0)
            self.assertEqual(dist_filter.get_value('d5').selected, False)
            self.assertEqual(dist_filter.get_value('d10').count, 12)
            self.assertEqual(dist_filter.get_value('d10').selected, True)
            self.assertEqual(dist_filter.get_value('d20').count, 40)
            self.assertEqual(dist_filter.get_value('d20').selected, False)

    def test_ordering_filter(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "facet_counts": {
    "facet_queries": {
      "date_created__today": 28,
      "date_created__week_ago": 105,
      "dist__d5": 0,
      "dist__d10": 12,
      "dist__d20": 40
    },
    "facet_fields": {},
    "facet_dates":{},
    "facet_ranges":{}
  }
}'''
        
            q = self.searcher.search()

            qf = QueryFilter()
            qf.add_ordering(
                OrderingFilter(
                    'sort',
                    OrderingValue('-score', '-score'),
                    OrderingValue('price', 'price'),
                    OrderingValue('-price', '-price')))

            params = {
                'sort': '-price',
            }

            q = qf.apply(q, params)
            raw_query = force_unicode(q)

            self.assertIn('sort=price desc', raw_query)

            ordering_filter = qf.ordering_filter
            self.assertEqual(ordering_filter.get_value('-price').selected, True)
            self.assertEqual(ordering_filter.get_value('-price').direction, OrderingValue.DESC)

    def test_pivot_filter(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "facet_counts": {
    "facet_pivot": {
      "manu": [
        {
          "field": "manufacturer",
          "value": "samsung",
          "count": 100,
          "pivot": [
            {
              "field": "model",
              "value": "note",
              "count": 66,
              "pivot": [
                {
                  "field": "discount",
                  "value": true,
                  "count": 11
                },
                {
                  "field": "discount",
                  "value": false,
                  "count": 55
                }
              ]
            },
            {
              "field": "model",
              "value": "S4",
              "count": 44,
              "pivot": [
                {
                  "field": "discount",
                  "value": true,
                  "count": 11
                },
                {
                  "field": "discount",
                  "value": false,
                  "count": 33
                }
              ]
            }
          ]
        },
        {
          "field": "manufacturer",
          "value": "nokia",
          "count": 1,
          "pivot": [
            {
              "field": "model",
              "value": "n900",
              "count": 1,
              "pivot": [
                {
                  "field": "discount",
                  "value": false,
                  "count": 1
                }
              ]
            }
          ]
        },
        {
          "field": "manufacturer",
          "value": "lenovo",
          "count": 5,
          "pivot": [
            {
              "field": "model",
              "value": "p770",
              "count": 4
            }
          ]
        }
      ]
    },
    "facet_fields": {},
    "facet_queries": {},
    "facet_dates": {},
    "facet_ranges": {}
  }
}'''
        
            q = self.searcher.search()

            qf = QueryFilter()
            qf.add_filter(
                PivotFilter(
                    'manu',
                    FacetPivotFilter('manufacturer', mincount=1),
                    FacetPivotFilter('model', _instance_mapper=_obj_mapper, limit=5),
                    FacetPivotFilter('discount', type=Boolean)))

            params = {
                'manu': ['samsung:note', 'nokia:n900:false', 'nothing:', 10],
                'manu__gte': '100',
            }

            q = qf.apply(q, params)
            raw_query = force_unicode(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.pivot={!key=manu ex=manu}manufacturer,model,discount', raw_query)
            self.assertIn('f.manufacturer.facet.mincount=1', raw_query)
            self.assertIn('f.model.facet.limit=5', raw_query)
            self.assertIn('fq={!tag=manu}'
                          '((manufacturer:"samsung" AND model:"note") '
                          'OR (manufacturer:"nokia" AND model:"n900" AND discount:"false") '
                          'OR (manufacturer:"nothing" AND model:"") '
                          'OR manufacturer:"10")',
                          raw_query)

            qf.process_results(q.results)

            manu_filter = qf.get_filter('manu')
            self.assertEqual(len(manu_filter.all_values), 3)
            self.assertEqual(len(manu_filter.selected_values), 2)
            self.assertEqual(len(manu_filter.values), 1)
            self.assertEqual(manu_filter.all_values[0].value, 'samsung')
            self.assertEqual(manu_filter.all_values[0].param_value, 'samsung')
            self.assertEqual(manu_filter.all_values[0].count, 100)
            self.assertEqual(manu_filter.all_values[0].selected, True)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].filter_name, 'manu')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].value, 'note')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].param_value, 'samsung:note')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].instance, ('note', 'note note'))
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].count, 66)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].count_plus, '66')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].selected, True)
            self.assertEqual(len(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values), 2)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[0].value, True)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[0].param_value, 'samsung:note:true')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[0].count, 11)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[0].selected, False)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[0].pivot, None)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[1].value, False)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[1].param_value, 'samsung:note:false')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[1].count, 55)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[1].selected, False)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[0].pivot.all_values[1].pivot, None)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].filter_name, 'manu')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].value, 'S4')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].param_value, 'samsung:S4')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].instance, ('S4', 'S4 S4'))
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].count, 44)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].count_plus, '+44')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].selected, False)
            self.assertEqual(len(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values), 2)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[0].value, True)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[0].param_value, 'samsung:S4:true')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[0].count, 11)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[0].count_plus, '11')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[0].selected, False)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[0].pivot, None)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[1].value, False)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[1].param_value, 'samsung:S4:false')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[1].count_plus, '33')
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[1].selected, False)
            self.assertEqual(manu_filter.all_values[0].pivot.all_values[1].pivot.all_values[1].pivot, None)
            self.assertEqual(manu_filter.all_values[1].value, 'nokia')
            self.assertEqual(manu_filter.all_values[1].param_value, 'nokia')
            self.assertEqual(manu_filter.all_values[1].count, 1)
            self.assertEqual(manu_filter.all_values[1].selected, True)
            self.assertEqual(len(manu_filter.all_values[1].pivot.all_values), 1)
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].value, 'n900')
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].param_value, 'nokia:n900')
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].instance, ('n900', 'n900 n900'))
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].count, 1)
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].selected, True)
            self.assertEqual(len(manu_filter.all_values[1].pivot.all_values[0].pivot.all_values), 1)
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].pivot.all_values[0].value, False)
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].pivot.all_values[0].count, 1)
            self.assertEqual(manu_filter.all_values[1].pivot.all_values[0].pivot.all_values[0].selected, True)
            self.assertEqual(manu_filter.all_values[2].value, 'lenovo')
            self.assertEqual(manu_filter.all_values[2].param_value, 'lenovo')
            self.assertEqual(manu_filter.all_values[2].count, 5)
            self.assertEqual(manu_filter.all_values[2].selected, False)

    def test_range_filter(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '{}'
        
            q = self.searcher.search()

            qf = QueryFilter()
            qf.add_filter(RangeFilter('price', 'price_unit', gather_stats=True,
                                      _local_params=LocalParams(cache=False),
                                      type=Float))

            params = {
                'price__gte': '100',
                'price__lte': ['nan', '200'],
                'price': '66',
            }

            q = qf.apply(q, params)
            raw_query = force_unicode(q)

            self.assertIn('fq={!cache=false tag=price}'
                          'price_unit:[100.0 TO *]', raw_query)
            self.assertIn('fq={!cache=false tag=price}'
                          'price_unit:[* TO 200.0]', raw_query)

            results = q.results
            with self.patch_send_request() as send_request:
                send_request.return_value = '''
{
  "response": {
    "numFound": 800,
    "start":0,
    "docs":[]
  },
  "stats": {
    "stats_fields": {
      "price_unit": {
        "min": 3.5,
        "max": 892.0,
        "count": 1882931,
        "missing": 556686,
        "sum": 5.677964302447648E13,
        "sumOfSquares": 2.452218850256837E26,
        "mean": 3.0154924967763808E7,
        "stddev": 1.1411980204045008E10
      }
    }
  }
}'''
                
                qf.process_results(results)

                price_filter = qf.get_filter('price')
                self.assertEqual(price_filter.from_value, 100)
                self.assertEqual(price_filter.to_value, 200)
                self.assertEqual(price_filter.min, 3.5)
                self.assertEqual(price_filter.max, 892.0)
