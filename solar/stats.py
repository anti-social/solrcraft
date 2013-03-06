
class Stats(object):
    def __init__(self, field):
        self.field = field
        self.min = None
        self.max = None
        self.sum = None
        self.count = None
        self.missing = None
        self.sum_of_squares = None
        self.mean = None
        self.stddev = None

    def get_params(self):
        return {'stats.field': [self.field]}

    def process_data(self, results):
        raw_stats = results.raw_results.stats['stats_fields'][self.field]
        self.min = raw_stats['min']
        self.max = raw_stats['max']
        self.sum = raw_stats['sum']
        self.count = raw_stats['count']
        self.missing = raw_stats['missing']
        self.sum_of_squares = raw_stats['sumOfSquares']
        self.mean = raw_stats['mean']
        self.stddev = raw_stats['stddev']
