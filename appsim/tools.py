import math
import numpy as np

SECONDS_IN_AN_HOUR = 3600  # 60 s/min * 60 min/hour
SECONDS_IN_A_DAY = 86400  # 3600 s/hour * 24 hour/day
SECONDS_IN_A_WEEK = 606462  # (31536000 s/year / 52 weeks/year) rounded up
SECONDS_IN_A_MONTH = 2628000  # 31536000 s/year / 12 months/year
SECONDS_IN_A_YEAR = 31536000  # 86400 s/day * 365 s/year


class MonitorStatistics(object):
    """Calculates statistics from a SimPy monitor"""

    def __init__(self, monitor):
        """Creates a MonitorStatistics object

        Parameters:
        monitor -- the monitor to have its statistics summarized

        """
        self.monitor = monitor

    def bp_by_time(self, time_interval):
        """Groups blocking probability observations by time.

        bp_by_time computes the blocking probability during a time interval,
        and returns a list of blocking probabilities for all simulated time.
        A list of floats should be expected. Time intervals during which no
        arrivals occurred are considered to have a blocking probability of 0
        and are included in the result. No confidence interval is computed.

        """
        time_interval = float(time_interval)
        num_buckets = int(math.ceil(SECONDS_IN_A_YEAR / time_interval))
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

    def bp_statistics(self, bp):
        """Returns a dict with the 50th, 95th, 99th percentiles and the mean.

        The dict has the following keys:
        'bp_50percentile' -- the 50th percentile of blocking probability
        'bp_95percentile' -- the 95th percentile of blocking probability
        'bp_99percentile' -- the 99th percentile of blocking probability
        'bp_mean' -- the mean of blocking probability

        """
        toReturn = {}
        toReturn['bp_50percentile'] = np.percentile(bp, 50)
        toReturn['bp_95percentile'] = np.percentile(bp, 95)
        toReturn['bp_99percentile'] = np.percentile(bp, 99)
        toReturn['bp_mean'] = np.mean(bp)
        toReturn['bp_raw'] = bp
        return toReturn

    @property
    def bp_by_hour(self):
        """Returns blocking probability statistics by hour

        See DocBlock of bp_by_time for full time grouping behavior. See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_AN_HOUR)
        return self.bp_statistics(bp)

    @property
    def bp_by_day(self):
        """Returns blocking probability statistics by day

        See DocBlock of bp_by_time for full time grouping behavior. See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_DAY)
        return self.bp_statistics(bp)

    @property
    def bp_by_week(self):
        """Returns blocking probability statistics by week

        See DocBlock of bp_by_time for full time grouping behavior. See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_WEEK)
        return self.bp_statistics(bp)

    @property
    def bp_by_month(self):
        """Returns blocking probability statistics by month

        See DocBlock of bp_by_time for full time grouping behavior. See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_MONTH)
        return self.bp_statistics(bp)

    @property
    def bp_by_year(self):
        """Returns blocking probability statistics by year

        See DocBlock of bp_by_time for full time grouping behavior. See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_YEAR)
        return self.bp_statistics(bp)

    @property
    def batchmean(self):
        """Returns a dict containing keys 'average' and 'delta'

        batchmean computes the average using a batch means method and finds
        the the 95% confidence interval. The confidence interval as the
        distance between the average and the edge of the confidence interval.

        """
        size = (len(self.monitor) / 31)
        lower = 0
        estimates = []
        for batch in range(0, 31):
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

    @property
    def mean(self):
        """ Returns the mean of the statistic.

        :return: Returns a simple mean of the statistic
        """
        return np.mean([data[1] for data in self.monitor])


class WaitTimeStatistics(object):
    """Calculates statistics from a SimPy monitor"""

    def __init__(self, monitor):
        """Creates a WaitTimeStatistics object

        Parameters:
        monitor -- the monitor to have its statistics summarized

        """
        self.monitor = monitor

    def wait_times_by_time(self, time_interval):
        """Groups wait time observations by time.

        Averages the wait time statistics on a time interval,and returns a
        list of wait time averages per time period for all simulated time. A
        list of floats should be expected. Time intervals during which no
        arrivals occurred are considered to have a wait time of 0
        and are included in the result. No confidence interval is computed.

        """
        time_interval = float(time_interval)
        num_buckets = int(math.ceil((SECONDS_IN_A_YEAR / 2) / time_interval))
        bucket_observations = [[] for i in range(num_buckets)]
        for observation in self.monitor:
            observation_time = observation[0]
            index = int(math.floor(observation_time / time_interval))
            bucket_observations[index].append(observation)

        wait_times = []
        for bucket in bucket_observations:
            if bucket == []:
                wait_times.append(0)
            else:
                wait_times_sum = sum([observation[1] for observation in bucket])
                wait_times_bucket = wait_times_sum / float(len(bucket))
                wait_times.append(wait_times_bucket)

        return wait_times

    def wait_times_statistics(self, wait_times):
        """Returns a dict with the 50th, 95th, 99th percentiles and the mean.

        The dict has the following keys:
        'wait_times_50percentile' -- the 50th percentile of blocking probability
        'wait_times_95percentile' -- the 95th percentile of blocking probability
        'wait_times_99percentile' -- the 99th percentile of blocking probability
        'wait_times_mean' -- the mean of blocking probability

        """
        toReturn = {}
        toReturn['wait_times_50percentile'] = np.percentile(wait_times, 50)
        toReturn['wait_times_95percentile'] = np.percentile(wait_times, 95)
        toReturn['wait_times_99percentile'] = np.percentile(wait_times, 99)
        toReturn['wait_times_mean'] = np.mean(wait_times)
        toReturn['wait_times_raw'] = wait_times
        return toReturn

    @property
    def wait_times_by_hour(self):
        """Returns blocking probability statistics by hour

        See DocBlock of wait_times_by_time for full time grouping behavior.
        See DocBlock of wait_times_statistics for expected keys in the dict
        returned.

        """
        wait_times = self.wait_times_by_time(SECONDS_IN_AN_HOUR)
        return self.wait_times_statistics(wait_times)

    @property
    def wait_times_by_day(self):
        """Returns blocking probability statistics by day

        See DocBlock of wait_times_by_time for full time grouping behavior.
        See DocBlock of wait_times_statistics for expected keys in the dict
        returned.

        """
        wait_times = self.wait_times_by_time(SECONDS_IN_A_DAY)
        return self.wait_times_statistics(wait_times)

    @property
    def wait_times_by_week(self):
        """Returns blocking probability statistics by week

        See DocBlock of wait_times_by_time for full time grouping behavior.
        See DocBlock of wait_times_statistics for expected keys in the dict
        returned.

        """
        wait_times = self.wait_times_by_time(SECONDS_IN_A_WEEK)
        return self.wait_times_statistics(wait_times)

    @property
    def wait_times_by_month(self):
        """Returns blocking probability statistics by month

        See DocBlock of wait_times_by_time for full time grouping behavior.
        See DocBlock of wait_times_statistics for expected keys in the dict
        returned.

        """
        wait_times = self.wait_times_by_time(SECONDS_IN_A_MONTH)
        return self.wait_times_statistics(wait_times)

    @property
    def wait_times_by_year(self):
        """Returns blocking probability statistics by year

        See DocBlock of wait_times_by_time for full time grouping behavior.
        See DocBlock of wait_times_statistics for expected keys in the dict
        returned.

        """
        wait_times = self.wait_times_by_time(SECONDS_IN_A_YEAR)
        return self.wait_times_statistics(wait_times)

    @property
    def batchmean(self):
        """Returns a dict containing keys 'average' and 'delta'

        Computes the average using a batch means method and finds the the 95%
        confidence interval. The confidence interval as the distance between
        the average and the edge of the confidence interval.

        """
        size = (len(self.monitor) / 31)
        lower = 0
        estimates = []
        for batch in range(0, 31):
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

    @property
    def mean(self):
        """ Returns the mean of the statistic.

        :return: Returns a simple mean of the statistic
        """
        return np.mean([data[1] for data in self.monitor])
