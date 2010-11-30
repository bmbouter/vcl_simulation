import math

class MonitorStatistics(object):
    """Calculates statistics from a SimPy monitor"""

    def __init__(self, monitor):
        """Creates a MonitorStatistics object

        Parameters:
        monitor -- the monitor to have its statistics summarized

        """
        self.monitor = monitor

    def count(self, char):
	"""Returns the number of times char exists as a value of
        self.monitor"""
        return ''.join([str(a[1]) for a in self.monitor]).count(char)

    @property
    def batchmean(self):
        """Returns a dict containing keys 'average' and 'delta'

        batchmean computes the average using a batch means method and finds 
        the the 95% confidence interval.  The confidence interval as the 
        distance between the average and the edge of the confidence interval.

        """
        size = (len(self.monitor) / 31)
        lower = 0
        estimates = []
        for batch in range(0,31):
            upper = lower + size
            slice = self.monitor[lower:upper]
            lower = upper
            bucket_average = float(sum([x[1] for x in slice])) / len(slice)
            estimates.append(bucket_average)
        average = sum(estimates[1:]) / len(estimates[1:])
        xi_minus_xbar = [(x - average)*(x - average) for x in estimates[1:]]
        sum_xi_minus_xbar = sum(xi_minus_xbar)
        s_squared = sum_xi_minus_xbar / len(xi_minus_xbar)
        delta = (math.sqrt(s_squared) / math.sqrt(30)) * 1.96
        return (average, delta)
