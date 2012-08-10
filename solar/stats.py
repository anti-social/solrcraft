
class Stats(object):
    def __init__(self, field, raw_stats):
        self.field = field
        self.min = raw_stats['min']
        self.max = raw_stats['max']
        self.sum = raw_stats['sum']
        self.count = raw_stats['count']
        self.missing = raw_stats['missing']
        self.sum_of_squares = raw_stats['sumOfSquares']
        self.mean = raw_stats['mean']
        self.stddev = raw_stats['stddev']
