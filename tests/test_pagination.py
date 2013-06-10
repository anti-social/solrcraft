from __future__ import unicode_literals

from mock import patch

from solar import SolrSearcher
from solar.ext.pagination import SolrQueryWrapper
from solar.ext.pagination.flask import Pagination

from .base import TestCase


class FlaskPaginationTest(TestCase):
    def test_pagination(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response":{
    "numFound":28,
    "start":2,
    "docs":[
      {
        "id":"333"
      },
      {
        "id":"444"
      }
    ]
  }
}
'''
            p = Pagination(self.searcher.search(), page=2, per_page=2)

            self.assertEqual(p.total, 28)
            self.assertEqual(p.pages, 14)
            self.assertEqual(len(p.items), 2)
            self.assertEqual(p.items[0].id, '333')
            self.assertEqual(p.items[1].id, '444')
            self.assertEqual(p.has_next, True)
            self.assertEqual(p.next_num, 3)
            self.assertEqual(p.has_prev, True)
            self.assertEqual(p.prev_num, 1)
            for page, check_page in zip(p.iter_pages(), [1, 2, 3, 4, 5, 6, None, 13, 14]):
                self.assertEqual(page, check_page)

            self.assertEqual(send_request.call_count, 1)

        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response":{
    "numFound":18,
    "start":20,
    "docs":[]
  }
}
'''
            p = Pagination(self.searcher.search(), page=2)

            self.assertEqual(p.total, 18)
            self.assertEqual(p.pages, 1)
            self.assertEqual(len(p.items), 0)
            self.assertEqual(p.has_next, False)
            self.assertEqual(p.next_num, 3)
            self.assertEqual(p.has_prev, True)
            self.assertEqual(p.prev_num, 1)

            pp = p.prev()
            self.assertEqual(pp.total, 18)
            self.assertEqual(pp.pages, 1)

            self.assertEqual(send_request.call_count, 2)

    def test_grouped_pagination(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "grouped":{
    "company":{
      "matches":281,
      "ngroups":109,
      "groups":[
        {
          "groupValue":"1",
          "doclist":{"numFound":9,"start":0,"docs":[
              {
                "id":"111"
              },
              {
                "id":"222"
              },
              {
                "id":"333"
              }
            ]
          }
        },
        {
          "groupValue":"3",
          "doclist":{"numFound":1,"start":0,"docs":[
              {
                "id":"555"
              }
            ]
          }
        }
      ]
    }
  }
}
'''
            q = self.searcher.search().group('company', limit=3)
            p = Pagination(SolrQueryWrapper(q, 'company'), page=29, per_page=2)

            self.assertEqual(p.total, 109)
            self.assertEqual(p.pages, 55)
            self.assertEqual(len(p.items), 2)
            self.assertEqual(p.items[0].value, '1')
            self.assertEqual(p.items[0].ndocs, 9)
            self.assertEqual(p.items[0].docs[0].id, '111')
            self.assertEqual(p.items[0].docs[1].id, '222')
            self.assertEqual(p.items[0].docs[2].id, '333')
            self.assertEqual(p.items[1].value, '3')
            self.assertEqual(p.items[1].ndocs, 1)
            self.assertEqual(p.items[1].docs[0].id, '555')
            self.assertEqual(p.has_next, True)
            self.assertEqual(p.next_num, 30)
            self.assertEqual(p.has_prev, True)
            self.assertEqual(p.prev_num, 28)

            self.assertEqual(send_request.call_count, 1)
