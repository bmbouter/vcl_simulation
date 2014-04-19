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
        and returns a list of blocking probabilities for all simulated time.  A
        list of floats should be expected. Time intervals during which no
        arrivals occured are considered to have a blocking probability of 0 and
        are included in the result.  No confidence interval is computed.

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

        See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_AN_HOUR)
        return self.bp_statistics(bp)

    @property
    def bp_by_day(self):
        """Returns blocking probability statistics by day

        See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_DAY)
        return self.bp_statistics(bp)

    @property
    def bp_by_week(self):
        """Returns blocking probability statistics by week

        See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_WEEK)
        return self.bp_statistics(bp)

    @property
    def bp_by_month(self):
        """Returns blocking probability statistics by month

        See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_MONTH)
        return self.bp_statistics(bp)

    @property
    def bp_by_year(self):
        """Returns blocking probability statistics by year

        See DocBlock of bp_by_time for full time grouping behavior.  See
        DocBlock of bp_statistics for expected keys in the dict returned.

        """
        bp = self.bp_by_time(SECONDS_IN_A_YEAR)
        return self.bp_statistics(bp)

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

class UtilizationStatisticsMixin(object):
    """Adds methods to sim.MMCmodel that compute utilization.
    """

    def get_mean_utilization(self):
        """Compute the total average utilization.
        """
        total_seat_time = self.mServerProvisionLength.total() * self.cluster.density
        used_seat_time = self.mAcceptServiceTimes.total()
        return used_seat_time / total_seat_time

    def util_by_time(self, time_interval):
        """Computes utilization by time.

        Computes the utilization during the requested time interval, and
        returns a list of utilization for all simulated time.  A list of floats
        is expected.

        Generally utilization is computed for any time window as the cumulative
        amount of server time during the time window divided by the cumulative
        amount of user time during the time window.

        No confidence intervals are computed.

        """
        time_interval = float(time_interval)
        num_buckets = int(math.ceil(SECONDS_IN_A_YEAR / time_interval))
        bucket_observations = [{'seat': 0, 'user': 0} for i in range(num_buckets)]

        # Compute User Seat Time per Bucket
        for start_time, duration in self.mServerProvisionLength:
            skip = False
            while duration > 0 and not skip:
                bucket_index = int(math.floor(start_time / time_interval))
                if bucket_index >= num_buckets:
                    skip = True
                    continue
                bucket_end_time = (bucket_index + 1) * time_interval
                if (start_time + duration) >= bucket_end_time:
                    time_in_bucket = bucket_end_time - start_time
                else:
                    time_in_bucket = duration
                start_time = start_time + time_in_bucket
                duration = duration - time_in_bucket
                if duration < 0:
                    raise Exception('Duration should not go negative')
                bucket_seat_time = time_in_bucket * self.cluster.density
                bucket_observations[bucket_index]['seat'] += bucket_seat_time

        # Compute User Seat Time per Bucket
        for start_time, duration in self.mAcceptServiceTimes:
            skip = False
            while duration > 0 and not skip:
                bucket_index = int(math.floor(start_time / time_interval))
                if bucket_index >= num_buckets:
                    skip = True
                    continue
                bucket_end_time = (bucket_index + 1) * time_interval
                if (start_time + duration) >= bucket_end_time:
                    time_in_bucket = bucket_end_time - start_time
                else:
                    time_in_bucket = duration
                start_time = start_time + time_in_bucket
                duration = duration - time_in_bucket
                if duration < 0:
                    raise Exception('Duration should not go negative')
                bucket_observations[bucket_index]['user'] += time_in_bucket

        # Compute Proportional Utilization per Bucket
        util = []
        i = -1
        for bucket in bucket_observations:
            i = i + 1
            try:
                util_per_bucket = bucket['user'] / bucket['seat']
                util.append(util_per_bucket)
            except ZeroDivisionError:
                if bucket['user'] > bucket['seat']:
                    raise Exception('In calculating utilization, a user should not have more seat time than there is server time in a period.')
        return util

    def util_statistics(self, util):
        """Returns a dict with the 50th, 95th, 99th percentiles and the mean.

        The dict has the following keys:
        'util_50percentile' -- the 50th percentile of blocking probability
        'util_95percentile' -- the 95th percentile of blocking probability
        'util_99percentile' -- the 99th percentile of blocking probability
        'util_mean' -- the mean of blocking probability

        """
        toReturn = {}
        toReturn['util_50percentile'] = np.percentile(util, 50)
        toReturn['util_95percentile'] = np.percentile(util, 95)
        toReturn['util_99percentile'] = np.percentile(util, 99)
        toReturn['util_mean'] = np.mean(util)
        toReturn['util_raw'] = util
        return toReturn

    @property
    def util_by_hour(self):
        """Returns utilization statistics by hour.

        See DocBlock of util_by_time for full time grouping behavior.  See
        DocBlock of util_statistics for expected keys in the dict returned.

        """
        util = self.util_by_time(SECONDS_IN_AN_HOUR)
        return self.util_statistics(util)

    @property
    def util_by_day(self):
        """Returns utilization statistics by day.

        See DocBlock of util_by_time for full time grouping behavior.  See
        DocBlock of util_statistics for expected keys in the dict returned.

        """
        util = self.util_by_time(SECONDS_IN_A_DAY)
        return self.util_statistics(util)

    @property
    def util_by_week(self):
        """Returns utilization statistics by week.

        See DocBlock of util_by_time for full time grouping behavior.  See
        DocBlock of util_statistics for expected keys in the dict returned.

        """
        util = self.util_by_time(SECONDS_IN_A_WEEK)
        return self.util_statistics(util)

    @property
    def util_by_month(self):
        """Returns utilization statistics by month.

        See DocBlock of util_by_time for full time grouping behavior.  See
        DocBlock of util_statistics for expected keys in the dict returned.

        """
        util = self.util_by_time(SECONDS_IN_A_MONTH)
        return self.util_statistics(util)

    @property
    def util_by_year(self):
        """Returns utilization statistics by year.

        See DocBlock of util_by_time for full time grouping behavior.  See
        DocBlock of util_statistics for expected keys in the dict returned.

        """
        util = self.util_by_time(SECONDS_IN_A_YEAR)
        return self.util_statistics(util)

