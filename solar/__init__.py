from .searcher import SolrSearcher, CommonSearcher
from .query import SolrQuery, SolrError
from .util import X, LocalParams

from .functions import _FunctionGenerator
func = _FunctionGenerator()


__version__ = (0, 3, 1)
