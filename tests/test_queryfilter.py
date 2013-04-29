from __future__ import unicode_literals

from datetime import datetime
from unittest import TestCase
try:
    from urllib import quote_plus
except ImportError:
    from urllib.parse import quote_plus

from mock import patch

from solar import X, LocalParams
from solar.searcher import SolrSearcher
from solar.queryfilter import (
    QueryFilter, Filter, FacetFilter, FacetFilterValue,
    FacetQueryFilter, FacetQueryFilterValue, RangeFilter,
    OrderingFilter, OrderingValue)


class CategoryFilterValue(FacetFilterValue):
    pass

class CategoryFilter(FacetFilter):
    filter_value_cls = CategoryFilterValue
    
    def __init__(self, name, *args, **kwargs):
        super(CategoryFilter, self).__init__(name, *args, **kwargs)


class QueryTest(TestCase):
    def test_queryfilter(self):
        s = SolrSearcher('http://example.com:8180/solr')
        with patch.object(s.solrs_read[0], '_send_request'):
            s.solrs_read[0]._send_request.return_value = '''{
  "grouped":{
    "company":{
      "matches":0,
      "ngroups":0,
      "groups":[]}},
  "facet_counts":{
    "facet_queries":{
      "date_created__today":28,
      "date_created__week_ago":105,
      "dist__d5":0,
      "dist__d10":12,
      "dist__d20":40},
    "facet_fields":{
      "cat":[
        "100",500,
        "5",10,
        "2",5,
        "1",2,
        "13",1]},
    "facet_dates":{},
    "facet_ranges":{}}}'''
        
            q = s.search()

            qf = QueryFilter()
            qf.add_filter(
                CategoryFilter(
                    'cat', 'category', mincount=1,
                    _local_params={'cache': False}))
            qf.add_filter(Filter('country', select_multiple=False))
            qf.add_filter(
                FacetQueryFilter(
                    'date_created',
                    FacetQueryFilterValue(
                        'today',
                        X(date_created__gte='NOW/DAY-1DAY'),
                        title='Only new',
                        help_text='Documents one day later'),
                    FacetQueryFilterValue(
                        'week_ago',
                        X(date_created__gte='NOW/DAY-7DAY'),
                        title='Week ago')))
            qf.add_filter(RangeFilter('price', 'price_unit', gather_stats=True,
                                      _local_params=LocalParams(cache=False)))
            qf.add_filter(
                FacetQueryFilter(
                    'dist',
                    FacetQueryFilterValue(
                        'd5',
                        None, _local_params=LocalParams('geofilt', d=5)),
                    FacetQueryFilterValue(
                        'd10',
                        None, _local_params=LocalParams('geofilt', d=10)),
                    FacetQueryFilterValue(
                        'd20',
                        None, _local_params=LocalParams('geofilt', d=20)),
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
                'date_created': ['today'],
                'price__gte': ['100'],
                'price__lte': ['200', 'nan'],
                'dist': ['d10'],
                'sort': ['-price'],
                }

            q = qf.apply(q, params)
            raw_query = str(q)

            self.assertIn('facet=true', raw_query)
            self.assertIn('facet.field=%s' % quote_plus('{!cache=false ex=cat key=cat}category'), raw_query)
            self.assertIn('facet.query=%s' % quote_plus('{!key=date_created__today ex=date_created}date_created:[NOW/DAY-1DAY TO *]'), raw_query)
            self.assertIn('facet.query=%s' % quote_plus('{!key=date_created__week_ago ex=date_created}date_created:[NOW/DAY-7DAY TO *]'), raw_query)
            self.assertIn('facet.query=%s' % quote_plus('{!geofilt d=5 key=dist__d5 ex=dist}'), raw_query)
            self.assertIn('facet.query=%s' % quote_plus('{!geofilt d=10 key=dist__d10 ex=dist}'), raw_query)
            self.assertIn('facet.query=%s' % quote_plus('{!geofilt d=20 key=dist__d20 ex=dist}'), raw_query)
            # self.assertIn('stats=true', raw_query)
            # self.assertIn('stats.field=price_unit', raw_query)
            self.assertIn('fq=%s' % quote_plus('{!cache=false tag=cat}(category:"5" OR category:"13")'), raw_query)
            self.assertIn('fq=%s' % quote_plus('{!tag=country}country:"ru"'), raw_query)
            self.assertIn('fq=%s' % quote_plus('{!tag=date_created}date_created:[NOW/DAY-1DAY TO *]'), raw_query)
            self.assertIn('fq=%s' % quote_plus('{!cache=false tag=price}price_unit:[100 TO *] AND price_unit:[* TO 200]'), raw_query)
            self.assertIn('fq=%s' % quote_plus('{!geofilt d=10 tag=dist}'), raw_query)
            self.assertIn('sort=%s' % quote_plus('price desc'), raw_query)

            results = q.results
            with patch.object(s.solrs_read[0], '_send_request'):
                s.solrs_read[0]._send_request.return_value = '''{
  "response":{"numFound":800,"start":0,"docs":[]
  },
  "stats":{
    "stats_fields":{
      "price_unit":{
        "min":3.5,
        "max":892.0,
        "count":1882931,
        "missing":556686,
        "sum":5.677964302447648E13,
        "sumOfSquares":2.452218850256837E26,
        "mean":3.0154924967763808E7,
        "stddev":1.1411980204045008E10}}}}'''
                
                qf.process_results(results)

                category_filter = qf.get_filter('cat')
                self.assertIsInstance(category_filter, CategoryFilter)
                self.assertEqual(category_filter.name, 'cat')
                self.assertEqual(category_filter.field, 'category')
                self.assertEqual(category_filter.all_values[0].value, '100')
                self.assertEqual(category_filter.all_values[0].count, 500)
                self.assertEqual(category_filter.all_values[0].selected, False)
                self.assertEqual(category_filter.all_values[0].title, '100')
                self.assertEqual(category_filter.all_values[1].value, '5')
                self.assertEqual(category_filter.all_values[1].count, 10)
                self.assertEqual(category_filter.all_values[1].selected, True)
                self.assertEqual(category_filter.all_values[2].value, '2')
                self.assertEqual(category_filter.all_values[2].count, 5)
                self.assertEqual(category_filter.all_values[2].selected, False)
                self.assertEqual(category_filter.all_values[3].value, '1')
                self.assertEqual(category_filter.all_values[3].count, 2)
                self.assertEqual(category_filter.all_values[3].selected, False)
                self.assertEqual(category_filter.all_values[4].value, '13')
                self.assertEqual(category_filter.all_values[4].count, 1)
                self.assertEqual(category_filter.all_values[4].selected, True)

                price_filter = qf.get_filter('price')
                self.assertEqual(price_filter.from_value, 100)
                self.assertEqual(price_filter.to_value, 200)
                self.assertEqual(price_filter.min, 3.5)
                self.assertEqual(price_filter.max, 892.0)

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

                ordering_filter = qf.ordering_filter
                self.assertEqual(ordering_filter.get_value('-price').selected, True)
                self.assertEqual(ordering_filter.get_value('-price').direction, OrderingValue.DESC)
