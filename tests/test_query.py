#!/usr/bin/env python
from datetime import datetime
from urllib import quote_plus
from unittest import TestCase

from mock import patch

from solar.searcher import SolrSearcher
from solar.util import SafeUnicode, X, LocalParams, make_fq
from solar import func


class QueryTest(TestCase):
    def test_query(self):
        q = SolrSearcher().search().dismax()
        raw_query = str(q)

        self.assertTrue('q=%s' % quote_plus('*:*') in raw_query)
        self.assertTrue('defType=dismax' in raw_query)

        q = SolrSearcher().search('test query').dismax()
        raw_query = str(q)

        self.assertTrue('q=%s' % quote_plus('test query') in raw_query)
        self.assertTrue('defType=dismax' in raw_query)

        q = SolrSearcher().search(name='test').dismax()
        raw_query = str(q)

        self.assertTrue('q=%s' % quote_plus('name:test') in raw_query)
        self.assertTrue('defType=dismax' in raw_query)

        q = SolrSearcher().search(name='test').edismax()
        raw_query = str(q)

        self.assertTrue('q=%s' % quote_plus('name:test') in raw_query)
        self.assertTrue('defType=edismax' in raw_query)

        q = (
            SolrSearcher().search(X(name='test') | X(name__startswith='test'))
            .dismax()
            .qf([('name', 10), ('keywords', 2)])
            .bf((func.linear('rank', 1, 0) ^ 100) + func.recip(func.ms('NOW/HOUR', 'dt_created'), 3.16e-11, 1, 1))
            .field_weight('name', 5)
        )
        raw_query = str(q)
        
        self.assertTrue('q=%s' % quote_plus('(name:test OR name:test*)') in raw_query)
        self.assertTrue('qf=%s' % quote_plus('name^5 keywords^2') in raw_query)
        self.assertTrue('bf=%s' % quote_plus('linear(rank,1,0)^100 recip(ms(NOW/HOUR,dt_created),3.16e-11,1,1)') in raw_query)
        self.assertTrue('defType=dismax' in raw_query)

        q = (
            SolrSearcher()
            .search(LocalParams('dismax', bf=func.linear('rank', 100, 0),
                                qf='name', v=X(SafeUnicode(u'"nokia lumia"')) | X(SafeUnicode(u'"nokia n900"'))))
        )
        raw_query = str(q)

        self.assertTrue('q=%s' % quote_plus(
                """{!dismax bf='linear(rank,100,0)' qf=name v='(\\"nokia lumia\\" OR \\"nokia n900\\")'}""") in raw_query)

        q = (
            SolrSearcher()
            .search(
                X(_query_=LocalParams('dismax', bf=func.linear('rank', 100, 0),
                                      qf='name^10', v=u'nokia'))
                & X(_query_=LocalParams('dismax', bf=func.linear('rank', 100, 0),
                                        qf='description', v=u'nokia lumia AND')))
        )
        raw_query = str(q)

        self.assertTrue('q=%s' % quote_plus(
                '(_query_:"{!dismax bf=\'linear(rank,100,0)\' qf=\'name^10\' v=nokia}" '
                'AND _query_:"{!dismax bf=\'linear(rank,100,0)\' qf=description v=\'nokia lumia and\'}")') in raw_query)
    
    def test_filter(self):
        q = SolrSearcher().search()

        self.assertSequenceEqual(
            q.filter(status=0)._prepare_params()['fq'],
            [u"status:0"])
        self.assertSequenceEqual(
            q.filter(status=0).filter(company_status__in=[0, 6])._prepare_params()['fq'],
            [u"status:0", u"(company_status:0 OR company_status:6)"])
        self.assertSequenceEqual(
            q.filter(X(status=0), X(company_status=0), _op='OR')._prepare_params()['fq'],
            [u"(status:0 OR company_status:0)"])
        self.assertSequenceEqual(
            q.filter(with_photo=True)._prepare_params()['fq'],
            [u"with_photo:true"])
        self.assertSequenceEqual(
            q.filter(date_created__gt=datetime(2012, 5, 17, 14, 35, 41, 794880))._prepare_params()['fq'],
            [u"date_created:{2012-05-17T14:35:41Z TO *}"])
        self.assertSequenceEqual(
            q.filter(price__lt=1000)._prepare_params()['fq'],
            [u"price:{* TO 1000}"])
        self.assertSequenceEqual(
            q.filter(X(price__lte=100), X(price__gte=1000))._prepare_params()['fq'],
            [u"price:[* TO 100] AND price:[1000 TO *]"])
        self.assertSequenceEqual(
            q.filter(price__between=[500, 1000], _local_params=[('cache', False), ('cost', 50)]) \
                ._prepare_params()['fq'],
            [u"{!cache=false cost=50}price:{500 TO 1000}"])
        self.assertSequenceEqual(
            q.filter(price=None)._prepare_params()['fq'],
            [u"(NOT price:[* TO *])"])
        self.assertSequenceEqual(
            q.exclude(price=None)._prepare_params()['fq'],
            [u"(NOT (NOT price:[* TO *]))"])
        self.assertSequenceEqual(
            q.filter(price__isnull=True)._prepare_params()['fq'],
            [u"(NOT price:[* TO *])"])
        self.assertSequenceEqual(
            q.filter(price__isnull=False)._prepare_params()['fq'],
            [u"price:[* TO *]"])
        self.assertSequenceEqual(
            q.filter(X(category__in=[1, 2, 3, 4, 5]), _local_params={'tag': 'category'}) \
                .filter(X(status=0) | X(status=5) | X(status=1) \
                            & X(company_status=6))._prepare_params()['fq'],
            [u"{!tag=category}(category:1 OR category:2 OR category:3 OR category:4 OR category:5)",
             u"(status:0 OR status:5 OR (status:1 AND company_status:6))"])
        self.assertSequenceEqual(
            q.exclude(status=1)._prepare_params()['fq'],
            [u"(NOT status:1)"])
        self.assertSequenceEqual(
            q.exclude(status__in=[1, 2, 3])._prepare_params()['fq'],
            [u"(NOT (status:1 OR status:2 OR status:3))"])

    def test_search_grouped_main(self):
        s = SolrSearcher('http://example.com:8180/solr')
        with patch.object(s.solrs_read[0], '_send_request'):
            s.solrs_read[0]._send_request.return_value = '''{
  "grouped":{
    "company":{
      "matches":281,
      "ngroups":109,
      "groups":[{
          "groupValue":"1",
          "doclist":{"numFound":9,"start":0,"docs":[
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

            def category_mapper(ids):
                return dict((id, {'id': int(id), 'name': id}) for id in ids)
            
            q = s.search()
            q = q.facet_field('category', mincount=5, limit=10,
                              _local_params={'ex': 'category'},
                              _instance_mapper=category_mapper)
            q = q.facet_field('tag', _local_params={'ex': 'tag'})
            q = q.facet_query(price__lte=100,
                              _local_params=[('ex', 'price'), ('cache', False)])
            q = q.group('company', limit=3)
            q = q.filter(category=13, _local_params={'tag': 'category'})
            q = q.stats('price')
            q = q.order_by('-date_created')
            q = q.offset(48).limit(24)
            raw_query = str(q)

            self.assertTrue('facet=true' in raw_query)
            self.assertTrue('facet.field=%s' % quote_plus('{!ex=category}category') in raw_query)
            self.assertTrue('f.category.facet.mincount=5' in raw_query)
            self.assertTrue('f.category.facet.limit=10' in raw_query)
            self.assertTrue('facet.field=%s' % quote_plus('{!ex=tag}tag') in raw_query)
            self.assertTrue('facet.query=%s' % quote_plus('{!ex=price cache=false}price:[* TO 100]') in raw_query)
            self.assertTrue('group=true' in raw_query)
            self.assertTrue('group.limit=3' in raw_query)
            self.assertTrue('group.field=company' in raw_query)
            self.assertTrue('fq=%s' % quote_plus('{!tag=category}category:13') in raw_query)
            self.assertTrue('stats=true' in raw_query)
            self.assertTrue('stats.field=price' in raw_query)
            self.assertTrue('sort=date_created+desc' in raw_query)
            self.assertTrue('start=48' in raw_query)
            self.assertTrue('rows=24' in raw_query)

            r = q.results
            grouped = r.get_grouped('company')
            self.assertEqual(grouped.ngroups, 109)
            self.assertEqual(grouped.ndocs, 281)
            self.assertEqual(grouped.groups[0].ndocs, 9)
            self.assertEqual(grouped.groups[0].docs[0].id, '111')
            self.assertEqual(grouped.groups[0].docs[0].name, 'Test 1')
            self.assertEqual(grouped.groups[0].docs[-1].id, '333')
            self.assertEqual(grouped.groups[0].docs[-1].name, 'Test 3')
            self.assertEqual(grouped.groups[1].ndocs, 1)
            self.assertEqual(grouped.groups[1].docs[0].id, '555')
            self.assertEqual(grouped.groups[1].docs[0].name, 'Test 5')
            self.assertEqual(len(grouped.docs), 0)
            
            self.assertEqual(len(r.facet_fields), 2)

            category_facet = r.get_facet_field('category')
            self.assertEqual(len(category_facet.values), 2)
            self.assertEqual(category_facet.values[0].value, '1')
            self.assertEqual(category_facet.values[0].count, 5)
            self.assertEqual(category_facet.values[0].instance, {'id': 1, 'name': '1'})
            self.assertEqual(category_facet.values[1].value, '2')
            self.assertEqual(category_facet.values[1].count, 2)
            self.assertEqual(category_facet.values[1].instance, {'id': 2, 'name': '2'})

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
        s = SolrSearcher('http://example.com:8180/solr')
        with patch.object(s.solrs_read[0], '_send_request'):
            s.solrs_read[0]._send_request.return_value = '''{
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

            q = s.search()
            q = q.group('company', limit=3, format='simple')
            raw_query = str(q)

            self.assertTrue('group=true' in raw_query)
            self.assertTrue('group.limit=3' in raw_query)
            self.assertTrue('group.format=simple' in raw_query)
            self.assertTrue('group.field=company' in raw_query)

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

    def test_instance_mapper(self):
        # TODO
        pass
            

if __name__ == '__main__':
    from unittest import main
    main()
