import math
import collections

import numpy as np

from scaler import Scale

class ReservePolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler uses a reserve policy to request and release server
    resources from the cluster.

    """

    def __init__(self, sim, scale_rate, startup_delay_func,
            shutdown_delay, reserved):
        """Initializes a ReservePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        reserved -- The number of servers to have available at the end of a
            the scale operation

        """

        self.reserved = reserved
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay_func=startup_delay_func,
                             shutdown_delay=shutdown_delay)

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        This policy scales the cluster such that the blocking
        probability is managed. The scaler uses a heuristic based
        reserve capacity approach whereby a reserve capacity, R, is
        specified. Reserve capacity, R,  is specified in terms of
        application seats. The scaler powers on and off servers such
        that the adjusted cluster size, given the same number of active
        users as before will have R additional, unused application
        seats.

        """
        diff = self.sim.cluster.n - self.reserved
        if diff > 0:
            servers_to_stop = diff / self.sim.cluster.density
        else:
            servers_to_stop = 0
        if diff < 0:
            servers_to_start = int(math.ceil(abs(float(diff) / self.sim.cluster.density)))
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop


class TimeVaryReservePolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler uses a time varying reserve policy to request and release
    server resources from the cluster.

    """

    def __init__(self, sim, scale_rate, startup_delay_func,
            shutdown_delay, window_size, arrival_percentile,
            five_minute_counts_file):
        """Initializes a ReservePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        window_size -- If an integer, it is the number of previous five-minute
            counts to consider to build a percentile from.  If set to None, the
            all previous values observed are seen.
        arrival_percentile -- The percentile of observed five-minute counts, to
            use as the value of R.  R is used as the number of seats at the end
            of the scale operation.
        five_minute_counts_file -- The path to a file containing all five-minute
            arrival counts.

        """

        self.window_size = window_size
        self.arrival_percentile = arrival_percentile
        self.five_minute_counts_file = five_minute_counts_file
        self.counts_deque = collections.deque(maxlen=window_size)
        self.counts_file = open(self.five_minute_counts_file, 'r')
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay_func=startup_delay_func,
                             shutdown_delay=shutdown_delay)

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        This policy scales the cluster such that the blocking
        probability is managed. The scaler uses a heuristic based
        reserve capacity approach whereby a reserve capacity, R, is
        specified. Reserve capacity, R,  is specified in terms of
        application seats. The scaler powers on and off servers such
        that the adjusted cluster size, given the same number of active
        users as before will have R additional, unused application
        seats.

        This scaler varies R in a time-varying way by collecting the arrival
        counts observed over the review interval.  The data file used assumes a
        review interval of five-minutes.  Each time the scaler is called, it
        observers a new count from the arrivals in the previous five-minute
        window.  This policy saves up to window_size number of the most recent
        five-minute arrival counts.  Each call to scaler_logic, the value of R
        is set to the percentile of the set of previous five-minute arrival
        counts.  If window_size is None, then all previous arrival counts are
        used.

        """
        five_min_count = float(self.counts_file.next())
        self.counts_deque.append(five_min_count)
        the_list = list(self.counts_deque)
        R = np.percentile(the_list, self.arrival_percentile * 100.0)
        diff = self.sim.cluster.n - R
        if diff > 0:
            servers_to_stop = diff / self.sim.cluster.density
        else:
            servers_to_stop = 0
        if diff < 0:
            servers_to_start = int(math.ceil(abs(float(diff) / self.sim.cluster.density)))
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop
