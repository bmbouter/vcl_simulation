import collections
import copy
import csv
import math

import numpy as np

from appsim.user import User
from appsim.feature import FeatureFlipper, PredictorNotSetException
from appsim.tools import weighted_choice_sub
from appsim.scaler.scaler import Scale


class ReservePolicy(Scale):
    """
    Wake up periodically and Scale the cluster

    This scaler uses a reserve policy to request and release server
    resources from the cluster.
    """

    def __init__(self, sim, scale_rate, startup_delay_func,
            shutdown_delay, reserved):
        """
        Initializes a ReservePolicy object

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
        # self.called_before = False

    def scaler_logic(self):
        """
        Implements the scaler logic specific to this scaler

        This policy scales the cluster such that the blocking
        probability is managed. The scaler uses a heuristic based
        reserve capacity approach whereby a reserve capacity, R, is
        specified. Reserve capacity, R,  is specified in terms of
        application seats. The scaler powers on and off servers such
        that the adjusted cluster size, given the same number of active
        users as before will have R additional, unused application
        seats.
        """

        # Used to fix the number of VMs to a specific level
        # if not self.called_before:
        #     self.called_before = True
        #     return 20, 0
        # else:
        #     return 0, 0

        # Subtract the number of estimated departing customers
        departure_cls = FeatureFlipper.departure_estimation_cls()

        reserved = self.reserved - departure_cls.slotted_estimate(self.sim)

        if FeatureFlipper.add_capacity_for_waiting_users():
            reserved = reserved + User.waiting.qsize()

        diff = self.sim.cluster.n - reserved
        if diff > 0:
            servers_to_stop = int(math.floor(float(diff) / self.sim.cluster.density))
        else:
            servers_to_stop = 0
        if diff < 0:
            servers_to_start = int(math.ceil(abs(float(diff) / self.sim.cluster.density)))
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop


class ARHMMReservePolicy(Scale):

    """
    Wake up periodically and Scale the cluster with an ARHMM Reserve Policy

    This scaler uses an ARHMM reserve policy to request and release server
    resources from the cluster.
    """

    def __init__(self, sim, scale_rate, startup_delay_func,
            shutdown_delay, five_minute_counts_file):
        """
        Initializes a ARHMMReservePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        five_minute_counts_file -- The path to a file containing all five-minute
            arrival counts.
        """
        self.A = [
            [0.9583615, 0.0001809, 0.0414576],
            [0.0117296, 0.7786373, 0.2096332],
            [0.0421590, 0.2244591, 0.7333819]
        ]
        self.C = [
            [0.1217175, 0.1197142, 0.1388058, 0.1273634, 0.1863004],
            [0, 0, 0, 0, 0.4107128],
            [0.0702153, 0.0574256, 0.0518409, 0.0491597, -0.3574271]
        ]
        self.R = [
            {'mean': 1.2427012, 'std_dev': 2.2489144},
            {'mean': 0, 'std_dev': 0},
            {'mean': 0.9607062, 'std_dev': 0.6681520}
        ]
        self.pi = [0.3918647, 0.3063385, 0.3017968]
        self.current_state = 0
        self.five_minute_counts_file = five_minute_counts_file
        self.counts_file = open(self.five_minute_counts_file, 'r')
        self.prediction_output_file = open('arhmm/prediction_output.txt', 'w')
        self.arrivals_observed = []
        filt_prob_filename = 'arhmm/2008_five_minute_counts_filtered_probs.csv'
        self.filt_prob_reader = csv.reader(open(filt_prob_filename, 'rb'), delimiter=',')
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay_func=startup_delay_func,
                             shutdown_delay=shutdown_delay)

    def get_arhmm_prediction(self):
        """
        Build an ARHMM prediction of users arriving in the next time slot

        :return: An integer number of predicted users who will arrive
        :rtype: int
        """
        R = self.R[self.current_state]
        A = self.A[self.current_state]
        C = copy.copy(self.C[self.current_state])
        last_five_observed = self.arrivals_observed[-5:]
        # we need to reverse these both because if values are missing we want
        # the most recent values to be included, so they need to be leftmost
        last_five_observed.reverse()
        C.reverse()
        coeffs = [a * b for a, b in zip(C, last_five_observed)]
        prediction = sum(coeffs) + R['mean'] + (0 * R['std_dev'])
        self.current_state = weighted_choice_sub(A) # This is the random walk
        #if len(self.arrivals_observed) >= 5: # This is the filtered_probs
        #    filt_probs_as_strings = self.filt_prob_reader.next()
        #    filt_probs = [float(item) for item in filt_probs_as_strings]
        #    self.current_state = filt_probs.index(max(filt_probs))
        self.arrivals_observed.append(int(self.counts_file.next().strip()))
        self.prediction_output_file.write('%s\n' % prediction)
        return prediction

    def scaler_logic(self):
        """
        Implements the ARHMM Reserved policy logic

        This policy scales the cluster according to an ARHMM prediction to
        estimate the number of users arriving in the next time interval. This
        prediction is used as the R value in the same way ReservePolicy
        handles R. See ReservePolicy for more details.
        """
        reserved = self.get_arhmm_prediction()
        self.sim.predictionMonitor.observe(reserved)

        # Subtract the number of estimated departing customers
        departure_cls = FeatureFlipper.departure_estimation_cls()
        reserved = reserved - departure_cls.slotted_estimate(self.sim)

        if FeatureFlipper.add_capacity_for_waiting_users():
            reserved = reserved + User.waiting.qsize()

        diff = self.sim.cluster.n - reserved
        if diff > 0:
            servers_to_stop = int(math.floor(float(diff) / self.sim.cluster.density))
        else:
            servers_to_stop = 0
        if diff < 0:
            servers_to_start = int(math.ceil(abs(float(diff) / self.sim.cluster.density)))
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop


class DataDrivenReservePolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler uses a data file driven reserve policy to request and release
    server resources from the cluster.

    """

    def __init__(self, sim, scale_rate, startup_delay_func,
            shutdown_delay, reserve_capacity_data_file):
        """
        Initializes a DataDrivenReservePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        reserve_capacity_data_file -- The path to a file containing reserve
            capacity values per five-minute arrival periods.
        """
        self.reserve_capacity_data_file = reserve_capacity_data_file
        self.capacity_file = open(self.reserve_capacity_data_file, 'r')
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay_func=startup_delay_func,
                             shutdown_delay=shutdown_delay)

    def scaler_logic(self):
        """
        Implements the scaler logic specific to this scaler

        The scaler selects the value of R from a data file.
        """
        last_arrival_count = self.sim.user_generator.user_count_since_last_scale
        try:
            predictor = FeatureFlipper.get_n_step_predictor()
        except PredictorNotSetException:
            R = float(self.capacity_file.next())
        else:
            R = predictor.predict_n_steps(last_arrival_count, self.n)
        self.sim.predictionMonitor.observe(R)

        # Subtract the number of estimated departing customers
        departure_cls = FeatureFlipper.departure_estimation_cls()
        R = R - departure_cls.slotted_estimate(self.sim)

        if FeatureFlipper.add_capacity_for_waiting_users():
            R = R + User.waiting.qsize()

        diff = self.sim.cluster.n - R
        if diff > 0:
            servers_to_stop = int(math.floor(float(diff) / self.sim.cluster.density))
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
        """
        Initializes a TimeVaryReservePolicy object

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
        """
        Implements the scaler logic specific to this scaler

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

        # Subtract the number of estimated departing customers
        departure_cls = FeatureFlipper.departure_estimation_cls()
        R = R - departure_cls.slotted_estimate(self.sim)

        if FeatureFlipper.add_capacity_for_waiting_users():
            R = R + User.waiting.qsize()

        diff = self.sim.cluster.n - R
        if diff > 0:
            servers_to_stop = int(math.floor(float(diff) / self.sim.cluster.density))
        else:
            servers_to_stop = 0
        if diff < 0:
            servers_to_start = int(math.ceil(abs(float(diff) / self.sim.cluster.density)))
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop
