#!/usr/bin/env python
from __future__ import unicode_literals

from datetime import datetime
from unittest import TestCase

from solar import func
from solar.util import SafeUnicode, safe_solr_input, X, LocalParams, make_fq


class UtilTest(TestCase):
    def test_safe_solr_input(self):
        self.assertEqual(safe_solr_input('SEPARATOR'), 'SEPARATOR')
        self.assertEqual(safe_solr_input(' AND one OR two  OR'), ' and one or two  or')
        self.assertEqual(safe_solr_input('AND OR NOT TO'), 'and or not to')
        self.assertEqual(safe_solr_input('\\+-&|!(){}[]^"~*?:'),
                         '\\\\\\+\\-\\&\\|\\!\\(\\)\\{\\}\\[\\]\\^\\"\\~\\*\\?\\:')
    
    def test_X(self):
        self.assertEqual(str(X(status=0)),
                         "(AND: ('status', 0))")
        self.assertEqual(str(X(status=0) & X(company_status__in=[0,6])),
                         "(AND: ('status', 0), ('company_status__in', [0, 6]))")
        self.assertEqual(str(X(status=0) | X(company_status=0)),
                         "(OR: ('status', 0), ('company_status', 0))")
        self.assertEqual(str(X(with_photo=True)),
                         "(AND: ('with_photo', True))")
        self.assertEqual(str(X(date_created__gt=datetime(2012, 5, 17, 14, 35, 41, 794880))),
                         "(AND: ('date_created__gt', datetime.datetime(2012, 5, 17, 14, 35, 41, 794880)))")
        self.assertEqual(str(X(price__lt=1000)),
                         "(AND: ('price__lt', 1000))")
        self.assertEqual(str(X(price__gte=100) & X(price__lte=1000)),
                         "(AND: ('price__gte', 100), ('price__lte', 1000))")
        self.assertEqual(str(X(price__between=[500, 1000])),
                         "(AND: ('price__between', [500, 1000]))")
        self.assertEqual(str(X(price__range=[2, 10])),
                         "(AND: ('price__range', [2, 10]))")
        self.assertEqual(str(X(category__in=[1, 2, 3, 4, 5]) & (X(status=0) | X(status=5) | X(status=1) & X(company_status=6))),
                         "(AND: ('category__in', [1, 2, 3, 4, 5]), (OR: ('status', 0), ('status', 5), (AND: ('status', 1), ('company_status', 6))))")
        self.assertEqual(str(~X(status=1)),
                         "(AND: (NOT (AND: ('status', 1))))")
        self.assertEqual(str(~X(status__in=[1, 2, 3])),
                         "(AND: (NOT (AND: ('status__in', [1, 2, 3]))))")
    
    def test_make_fq(self):
        self.assertEqual(make_fq(X(status=0)),
                         "status:0")
        self.assertEqual(make_fq(X(status=0) & X(company_status__in=[0,6])),
                         "status:0 AND (company_status:0 OR company_status:6)")
        self.assertEqual(make_fq(X(status=0) | X(company_status=0)),
                         "(status:0 OR company_status:0)")
        self.assertEqual(make_fq(X(name='Chuck Norris')),
                         "name:(Chuck Norris)")
        self.assertEqual(make_fq(X(name__exact='Chuck Norris')),
                         'name:"Chuck Norris"')
        self.assertEqual(make_fq(X(name__startswith='Chuck Nor')),
                         'name:(Chuck Nor*)')
        self.assertEqual(make_fq(X(with_photo=True)),
                         "with_photo:true")
        self.assertEqual(make_fq(X(date_created__gt=datetime(2012, 5, 17, 14, 35, 41, 794880))),
                         "date_created:{2012-05-17T14:35:41Z TO *}")
        self.assertEqual(make_fq(X(price__lt=1000)),
                         "price:{* TO 1000}")
        self.assertEqual(make_fq(X(price__gte=100) & X(price__lte=1000)),
                         "price:[100 TO *] AND price:[* TO 1000]")
        self.assertEqual(make_fq(X(price__between=[500, 1000])),
                         "price:{500 TO 1000}")
        self.assertEqual(make_fq(X(price__range=[2, 10])),
                         "price:[2 TO 10]")
        self.assertEqual(make_fq(X(category__in=[1, 2, 3, 4, 5]) & (X(status=0) | X(status=5) | X(status=1) & X(company_status=6))),
                         "(category:1 OR category:2 OR category:3 OR category:4 OR category:5) AND (status:0 OR status:5 OR (status:1 AND company_status:6))")
        self.assertEqual(make_fq(~X(status=1)),
                         "NOT (status:1)")
        self.assertEqual(make_fq(~X(status__in=[1, 2, 3])),
                         "NOT ((status:1 OR status:2 OR status:3))")
        self.assertEqual(make_fq(X("status:0 OR status:1")),
                         "status\\:0 or status\\:1")
        self.assertEqual(make_fq(X(SafeUnicode("status:0 OR status:1"))),
                         "status:0 OR status:1")
        self.assertEqual(make_fq(X(status=SafeUnicode('"0"'))),
                         'status:"0"')
        self.assertEqual(make_fq(X(LocalParams('dismax', qf='name', v=X('nokia lumia')))),
                         "{!dismax qf=name v='nokia lumia'}")
        self.assertEqual(make_fq(X(_query_=LocalParams('dismax', qf='name', v=X('nokia lumia')))),
                         "_query_:\"{!dismax qf=name v='nokia lumia'}\"")

    def test_local_params(self):
        self.assertEqual(str(LocalParams({'cache': False})),
                         '{!cache=false}')
        self.assertEqual(str(LocalParams(LocalParams({'cache': False}))),
                         '{!cache=false}')
        self.assertEqual(str(LocalParams({'ex': 'tag'}, key='tag')),
                         '{!ex=tag key=tag}')
        self.assertEqual(str(LocalParams({'ex': 'tag', 'key': 'tag'}, key='category')),
                         '{!ex=tag key=category}')
        self.assertEqual(str(LocalParams('frange', l=0, u=5)),
                         '{!frange l=0 u=5}')
        self.assertEqual(str(LocalParams(['geofilt', ('d', 10), ('key', 'd10')])),
                         '{!geofilt d=10 key=d10}')
        self.assertEqual(str(LocalParams({'type': 'join', 'from': 'id', 'to': 'manu_id'})),
                         '{!join from=id to=manu_id}')
        self.assertEqual(str(LocalParams()), '')
        self.assertEqual(str(LocalParams(None)), '')

        self.assertEqual(str(LocalParams('dismax', v='OR test')),
                         """{!dismax v='or test'}""")
        self.assertEqual(str(LocalParams('dismax', v='"test"')),
                         """{!dismax v='\\"test\\"'}""")
        self.assertEqual(str(LocalParams('dismax', v='test\'')),
                         """{!dismax v='test\\\\\''}""")
        self.assertRaises(ValueError, LocalParams, '{dismax}', v='test')
        self.assertRaises(ValueError, LocalParams, ['dismax', ('!v', 'test')])
        
        self.assertEqual(
            str(LocalParams('dismax', qf='name',
                            v=X(SafeUnicode('"nokia lumia"')) | X(SafeUnicode('"nokia n900"')))),
            """{!dismax qf=name v='(\\"nokia lumia\\" OR \\"nokia n900\\")'}""")

        lp = LocalParams('dismax', bf=func.linear('rank', 100, 0), v='$q1')
        lp.update(LocalParams(qf='name^10 description'))
        lp.add('pf', 'name')
        lp.add('ps', 2)
        self.assertIn('type', lp)
        self.assertIn('v', lp)
        self.assertFalse('q' in lp)
        self.assertEqual(lp['type'], 'dismax')
        self.assertEqual(
            str(lp),
            "{!dismax bf='linear(rank,100,0)' v=$q1 qf='name^10 description' pf=name ps=2}")
