
def maybe_float(v):
    if v is not None:
        return float(v)


def maybe_int(v):
    if v is not None:
        return int(v)


class Stats(object):
    def __init__(self, field, raw_stats):
        raw_stats = raw_stats or {}
        self.field = field
        self.min = maybe_float(raw_stats.get('min'))
        self.max = maybe_float(raw_stats.get('max'))
        self.sum = maybe_float(raw_stats.get('sum'))
        self.count = maybe_int(raw_stats.get('count'))
        self.missing = maybe_int(raw_stats.get('missing'))
        self.sum_of_squares = maybe_float(raw_stats.get('sumOfSquares'))
        self.mean = maybe_float(raw_stats.get('mean'))
        self.stddev = maybe_float(raw_stats.get('stddev'))
