 #!/usr/bin/env python
from collections import namedtuple

from solar.searcher import SolrSearcher

from .base import TestCase


class SearcherTestCase(TestCase):
    def test_attached_searcher(self):
        class Model:
            pass

        class ModelSearcher(SolrSearcher):
            model = Model

        self.assertEqual(Model.searcher.__class__, ModelSearcher)

    def test_get(self):
        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "doc": {
    "id": "111",
    "name": "Test realtime doc"
  }
}
'''

            doc = self.searcher.get('111')[0]
            self.assertEqual(doc.id, '111')
            self.assertEqual(doc.name, 'Test realtime doc')
            self.assertEqual(doc.instance, None)

        with self.patch_send_request() as send_request:
            send_request.return_value = '''
{
  "response": {
    "numFound": 2,
    "start": 0,
    "docs": [
      {
        "id": "111",
        "name": "Test realtime doc"
      },
      {
        "id": "222",
        "name": "Test realtime doc duplicate"
      }
    ]
  }
}
'''

            docs = self.searcher.get(ids=['111', '222'])
            self.assertEqual(len(docs), 2)
            self.assertEqual(docs[0].id, '111')
            self.assertEqual(docs[0].name, 'Test realtime doc')
            self.assertEqual(docs[1].id, '222')
            self.assertEqual(docs[1].name, 'Test realtime doc duplicate')
