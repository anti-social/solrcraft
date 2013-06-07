from __future__ import unicode_literals

import unittest

from mock import patch

from solar import SolrSearcher


class TestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.searcher = SolrSearcher('http://example.com:8180/solr')
        super(TestCase, self).__init__(*args, **kwargs)

    def patch_send_request(self, searcher=None):
        searcher = searcher or self.searcher
        return patch.object(searcher.solr, '_send_request')
