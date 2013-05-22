 #!/usr/bin/env python
from unittest import TestCase
from collections import namedtuple

from mock import patch

from solar.searcher import SolrSearcher


class SearcherTestCase(TestCase):
    def test_attached_searcher(self):
        class Model:
            pass

        class ModelSearcher(SolrSearcher):
            model = Model

        self.assertEqual(Model.searcher.__class__, ModelSearcher)

    def test_get(self):
        s = SolrSearcher('http://example.com:8180/solr')
        with patch.object(s.solrs_write[0], '_send_request'):
            s.solrs_write[0]._send_request.return_value = '''
{
  "doc": {
    "id": "111",
    "name": "Test realtime doc"
  }
}
'''

            doc = s.get('111')[0]
            self.assertEqual(doc.id, '111')
            self.assertEqual(doc.name, 'Test realtime doc')
            self.assertEqual(doc.instance, None)

        with patch.object(s.solrs_write[0], '_send_request'):
            s.solrs_write[0]._send_request.return_value = '''
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

            docs = s.get(ids=['111', '222'])
            self.assertEqual(len(docs), 2)
            self.assertEqual(docs[0].id, '111')
            self.assertEqual(docs[0].name, 'Test realtime doc')
            self.assertEqual(docs[1].id, '222')
            self.assertEqual(docs[1].name, 'Test realtime doc duplicate')
