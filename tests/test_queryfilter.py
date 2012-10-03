#!/usr/bin/env python
from urllib import quote_plus
from datetime import datetime
from unittest import TestCase

from mock import patch

from solar.searcher import SolrSearcher
from solar.queryfilter import (
    QueryFilter, FacetFilter, FacetFilterValue,
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
      "date_created__week_ago":105},
    "facet_fields":{
      "category":[
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
                    'category', mincount=1,
                    _local_params={'cache': 'false'}))
            qf.add_filter(
                FacetQueryFilter(
                    'date_created',
                    FacetQueryFilterValue(
                        'today',
                        date_created__gte='NOW/DAY-1DAY'),
                    FacetQueryFilterValue(
                        'week_ago',
                        date_created__gte='NOW/DAY-7DAY')))
            qf.add_filter(RangeFilter('price'))
            qf.add_ordering(
                OrderingFilter(
                    'sort',
                    OrderingValue('-score', '-score'),
                    OrderingValue('price', 'price'),
                    OrderingValue('-price', '-price')))

            params = {
                'category': ['5', '13'],
                'date_created': ['today'],
                'price__gte': ['100'],
                'price__lte': ['200'],
                'sort': ['-price'],
                }

            q = qf.apply(q, params)
            raw_query = str(q)

            self.assertTrue('facet=true' in raw_query)
            self.assertTrue('facet.field=%s' % quote_plus('{!ex=category cache=false}category') in raw_query)
            self.assertTrue('facet.query=%s' % quote_plus('{!ex=date_created key=date_created__today}date_created:[NOW/DAY-1DAY TO *]') in raw_query)
            self.assertTrue('facet.query=%s' % quote_plus('{!ex=date_created key=date_created__week_ago}date_created:[NOW/DAY-7DAY TO *]') in raw_query)
            # self.assertTrue('stats=true' in raw_query)
            # self.assertTrue('stats.field=price' in raw_query)
            self.assertTrue('fq=%s' % quote_plus('{!tag=category}(category:5 OR category:13)') in raw_query)
            self.assertTrue('fq=%s' % quote_plus('{!tag=date_created}date_created:[NOW/DAY-1DAY TO *]') in raw_query)
            self.assertTrue('fq=%s' % quote_plus('{!tag=price}price:[100 TO *] AND price:[* TO 200]') in raw_query)
            self.assertTrue('sort=%s' % quote_plus('price desc') in raw_query)

            results = q.results
            with patch.object(s.solrs_read[0], '_send_request'):
                s.solrs_read[0]._send_request.return_value = '''{
  "response":{"numFound":800,"start":0,"docs":[]
  },
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
                
                qf.process_results(results)

                category_filter = qf.get_filter('category')
                self.assertTrue(isinstance(category_filter, CategoryFilter))
                self.assertEqual(category_filter.name, 'category')
                self.assertEqual(category_filter.all_values[0].value, '100')
                self.assertEqual(category_filter.all_values[0].count, 500)
                self.assertEqual(category_filter.all_values[0].selected, False)
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
                self.assertEqual(price_filter.from_value, '100')
                self.assertEqual(price_filter.to_value, '200')
                self.assertEqual(price_filter.min, 3.5)
                self.assertEqual(price_filter.max, 892.0)

                date_created_filter = qf.get_filter('date_created')
                self.assertEqual(date_created_filter.get_value('today').count, 28)
                self.assertEqual(date_created_filter.get_value('today').selected, True)
                self.assertEqual(date_created_filter.get_value('week_ago').count, 105)

                ordering_filter = qf.ordering_filter
                self.assertEqual(ordering_filter.get_value('-price').selected, True)
                self.assertEqual(ordering_filter.get_value('-price').direction, OrderingValue.DESC)
            

if __name__ == '__main__':
    from unittest import main
    main()
