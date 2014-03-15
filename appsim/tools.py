import math
import numpy as np

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

    def bp_by_time(self, time_interval):
	"""Groups blocking probability observations by time

	bp_by_time computes the blocking probability during a time interval,
	and returns a list of blocking probabilities for all simulated time.  A
	list of floats should be expected. Time intervals during which no
	arrivals occured are considered to have a blocking probability of 0 and
	are included in the result.  No confidence interval is computed.

        """
        max_time = self.monitor[-1][0]
        num_buckets = int(math.ceil(max_time / time_interval))
        bucket_observations = [[] for i in range(num_buckets)]
        for observation in self.monitor:
            observation_time = observation[0]
            index = int(math.floor(observation_time / time_interval))
            bucket_observations[index].append(observation)

        bp = []
        for bucket in bucket_observations:
            if bucket == []:
                bp.append(0)
            else:
                bp_sum = sum([observation[1] for observation in bucket])
                bp_bucket = bp_sum / float(len(bucket))
                bp.append(bp_bucket)

        return bp

    def statistics(self, bp):
	"""Returns a dict with the 50th, 95th, 99th percentiles and the mean.

        The dict has the following keys:
        'bp_50percentile' -- the 50th percentile of blocking probability
        'bp_95percentile' -- the 95th percentile of blocking probability
        'bp_99percentile' -- the 99th percentile of blocking probability
        'bp_mean' -- the mean of blocking probability

        """
        toReturn = {}
        toReturn['bp_50percentile'] = np.percentile(bp,50)
        toReturn['bp_95percentile'] = np.percentile(bp,95)
        toReturn['bp_99percentile'] = np.percentile(bp,99)
        toReturn['bp_mean'] = np.mean(bp)
        toReturn['bp_raw'] = bp
        return toReturn

    @property
    def bp_by_hour(self):
        """Returns blocking probability statistics by hour

	See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of statistics for expected keys in the dict returned.

        """
        seconds_in_an_hour = 3600
        bp = self.bp_by_time(seconds_in_an_hour)
        return self.statistics(bp)

    @property
    def bp_by_day(self):
        """Returns blocking probability statistics by hour

	See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of statistics for expected keys in the dict returned.

        """
        seconds_in_a_day = 86400
        bp = self.bp_by_time(seconds_in_a_day)
        return self.statistics(bp)

    @property
    def bp_by_week(self):
        """Returns blocking probability statistics by hour

	See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of statistics for expected keys in the dict returned.

        """
        seconds_in_a_week = 604800
        bp = self.bp_by_time(seconds_in_a_week)
        return self.statistics(bp)

    @property
    def bp_by_month(self):
        """Returns blocking probability statistics by hour

	See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of statistics for expected keys in the dict returned.

        """
        seconds_in_a_month = 2620800
        bp = self.bp_by_time(seconds_in_a_month)
        return self.statistics(bp)

    @property
    def bp_by_year(self):
        """Returns blocking probability statistics by hour

	See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of statistics for expected keys in the dict returned.

        """
        seconds_in_a_year = 31449600
        bp = self.bp_by_time(seconds_in_a_year)
        return self.statistics(bp)

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
