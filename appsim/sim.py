from collections import defaultdict
from random import seed

import numpy as np
from SimPy.Simulation import *

from cluster import Cluster
from appsim.scaler.reserve_policy import (ARHMMReservePolicy,
                                          DataDrivenReservePolicy,
                                          ReservePolicy,
                                          TimeVaryReservePolicy)
from appsim.scaler.fixed_size_policy import FixedSizePolicy
from appsim.scaler.data_file_policy import GenericDataFileScaler
from appsim.scaler.erlang_policy import ErlangBFormulaDataPolicy
from appsim.scaler.scaler import State
from appsim.user import User
from tools import MonitorStatistics, pairs, SECONDS_IN_A_YEAR, WaitTimeStatistics
from user_generators import PoissonGenerator, DataFileGenerator


MAX_Q = None  # [0, MAX_Q]
SECONDS_IN_A_YEAR = SECONDS_IN_A_YEAR * 1  # a hack to get more simulation time
BASE_PATH = '/home/bmbouter/Documents/Research/matlab/'


class MMCmodel(Simulation):
    """A highly customizable simpy M/M/C Erlang-loss system

    MMCmodel is a simulation which brings together different simulation
    components to simulate an M/M/C Erlang-loss system where customers
    are considered blocked and walk away if they are refused service
    upon arrival. After instantiation the following member variables
    must be assigned to instantiated classes of the following type:

    self.cluster:
        --Purpose: Cluster of virtual machines to server users with
        --Type: appsim.cluster.Cluster
    self.scaler:
        --Purpose: Scales the cluster periodically
        --Type: appsim.scaler.Scale
    self.user_generator:
        --Purpose: Causes user to arrive with some pattern
        --Type: appsim.user_generators.*

    The UtilizationStatisticsMixin adds methods which compute Utilization
    statistics on different timescales.

    """

    def __init__(self, rand_seed=333555777):
        """Initializer for an MMCmodel

        Parameters:
        rand_seed -- the number to seed the random number generator with

        """
        seed(rand_seed)
        self.initialize()
        ### Simulation Monitors
        ### Scale Monitors
        # scale monitors are recorded with each scale event
        self.mClusterActive = Monitor(sim=self)  # monitor cluster.active
        self.mClusterBooting = Monitor(sim=self)  # monitor cluster.booting
        self.mClusterShuttingDown = Monitor(sim=self)  # cluster.shutting_down
        self.mClusterOccupancy = Monitor(sim=self)  # utilized seats
        self.mClusterQueueDepth = Monitor(sim=self)  # Monitor the queue depths at scaling points
        self.mSystemState = Monitor(sim=self)  # system state records (n, s, q) at time t
        ### Wait Time Monitors
        self.mWaitTime = Monitor(sim=self) # wait time of each customer
        ### Customer Monitors
        self.mBlocked = Monitor(sim=self)  # ifcustomer is blocked or not
        self.mNumServers = Monitor(sim=self)  # cluster active+booting+shutting
        self.mServerProvisionLength = Monitor(sim=self)  # online time
        self.arrivalMonitor = Monitor(sim=self)  # user arrival monitor

        # service times of customers who were accepted
        self.mAcceptServiceTimes = Monitor(sim=self)

        # monitor for the service times for customers who are were blocked
        self.mLostServiceTimes = Monitor(sim=self)

    def finalize_simulation(self):
        if self.now() > SECONDS_IN_A_YEAR:
            raise Exception(
                'Finalizing simulation after %s seconds is incorrect!' % SECONDS_IN_A_YEAR)
        self._adjust_mAcceptServiceTimes()
        self._adjust_mLostServiceTimes()
        self._observe_running_servers()

    def _observe_running_servers(self):
        """
        Observe the provision length of servers that are still running assuming the stop now.

        Server provision start time and its deployed length is recorded when a server is deleted.
        It is recorded this way because policies may dynamically decide to delete a VM without known
        when those conditions will occur. Thus it is easier to do upon deletion.

        When a simulation completed, any servers that are in the active, booting, or shutting_down
        states need to have their start time and service times observed. This method makes those
        observations into the appropriate monitor, mServerProvisionLength. It records the start
        time and provisioned time, assuming the server is deleted at time self.now().
        """
        c = self.cluster
        for server in c.active + c.booting + c.shutting_down:
            server_deployed_time = self.now() - server.start_time

            # monitor the servers provision, deprovision time
            self.mServerProvisionLength.observe(server_deployed_time,
                                                t=server.start_time)
        c.active = c.booting = c.shutting_down = []

    def _adjust_mAcceptServiceTimes(self):
        """
        Update the service times of customers who were accepted from going into simulation time that
        never happened.

        The service time of accepted customers is recorded in the mAcceptServiceTimes monitor at the
        time a customer is accepted. These could potentially go further into the future than the
        simulation actually does. This method updates the duration values of any
        mAcceptServiceTimes observations where start_time + duration > self.now(). The start time
        requires no update, but the duration is set to self.now() - start_time.
        """
        current_sim_time = self.now()
        for i, item in enumerate(self.mAcceptServiceTimes):
            if sum(item) > current_sim_time:
                new_service_time = current_sim_time - item[0]
                self.mAcceptServiceTimes[i][1] = new_service_time

    def _adjust_mLostServiceTimes(self):
        """
        Update the service times of customers who were blocked from going into simulation time that
        never happened.

        The service time of blocked customers is recorded in the mLostServiceTimes monitor at the
        time a customer is blocked. These could potentially go further into the future than the
        simulation actually does. This method updates the duration values of any mLostServiceTimes
        observations where start_time + duration > self.now(). The start time requires no update,
        but the duration is set to self.now() - start_time.
        """
        current_sim_time = self.now()
        for i, item in enumerate(self.mLostServiceTimes):
            if sum(item) > current_sim_time:
                new_service_time = current_sim_time - item[0]
                self.mLostServiceTimes[i][1] = new_service_time

    def get_mean_utilization(self):
        """Compute the total average utilization."""
        total_mServerProvisionLength = sum([data[1] for data in self.mServerProvisionLength])
        total_seat_time = total_mServerProvisionLength * self.cluster.density
        used_seat_time = sum([data[1] for data in self.mAcceptServiceTimes])
        return used_seat_time / total_seat_time

    def generate_matrix_and_exit(self):
        # percent_service_time_regen = (self.user_generator.regenerate_service_time_count / float(User.user_count)) * 100
        # print '%0.2f percent service times were regenerated' % percent_service_time_regen

        max_customers = max(self.mSystemState, key=lambda s: s[1].n)[1].n
        print 'max number of customers in system = %s' % max_customers

        hist = []
        for cust_count in range(max_customers + 1):
            cust_per_state = len(filter(lambda s: s[1].n == cust_count, self.mSystemState))
            hist.append(cust_per_state)
        print 'customer counts in system (sorted) = %s' % hist
        print 'customer probabilities in system (sorted) = %s' % [count / float(sum(hist)) for count in hist]
        print 'total customers in system = %s' % sum(hist)


        max_servers = max(self.mSystemState, key=lambda s: s[1].s)[1].s
        print 'max number of servers in system = %s' % max_servers

        server_hist = []
        for server_count in range(max_servers + 1):
            servers_per_state = len(filter(lambda s: s[1].s == server_count, self.mSystemState))
            server_hist.append(servers_per_state)
        print 'server count in system (sorted) = %s' % server_hist
        print 'server probabilities in system (sorted) = %s' % [count / float(sum(server_hist)) for count in server_hist]


        max_q = max(self.mSystemState, key=lambda s: s[1].q)[1].q
        print 'max q observed in system = %s' % max_q
        q_hist = []
        for q_count in range(max_q + 1):
            q_per_state = len(filter(lambda s: s[1].q == q_count, self.mSystemState))
            q_hist.append(q_per_state)

        print 'q counts in system (sorted) = %s' % q_hist
        print 'q probabilities in system (sorted) = %s' % [count / float(sum(q_hist)) for count in q_hist]


        max_queue_depth = max(self.mClusterQueueDepth, key=lambda s: s[1])[1]
        print 'max_queue_depth observed in system = %s' % max_queue_depth
        queue_depth_hist = []
        for queue_depth_count in range(max_queue_depth + 1):
            queue_depth = len(filter(lambda s: s[1] == queue_depth_count, self.mClusterQueueDepth))
            queue_depth_hist.append(queue_depth)

        print 'queue_depth counts in system (sorted) = %s' % queue_depth_hist
        print 'queue_depth probabilities in system (sorted) = %s' % [count / float(sum(queue_depth_hist)) for count in queue_depth_hist]

        sys.exit(0)

        def all_states():
            global MAX_Q
            for n in range(max_customers + 1):
                for s in range(1, max_servers + 1):
                    if MAX_Q is None:
                        MAX_Q = max_servers
                    for q in range(MAX_Q + 1):
                        yield State(n, s, q)

        transitions = defaultdict(lambda : defaultdict(int))
        for prev_monitor, next_monitor in pairs(self.mSystemState):
            prev_state = prev_monitor[1]
            prev_key = '%s,%s,%s' % (prev_state.n, prev_state.s, prev_state.q)
            next_state = next_monitor[1]
            next_key = '%s,%s,%s' % (next_state.n, next_state.s, next_state.q)
            transitions[prev_key][next_key] += 1

        for prev_key in transitions.keys():
            sum_outbound_departures = float(sum(transitions[prev_key].values()))
            for next_key in transitions[prev_key].keys():
                transitions[prev_key][next_key] /= sum_outbound_departures

        with open(BASE_PATH + 'sim_transition_sparse.dat', 'w') as f_sparse_dat:
            for row, prev_state in enumerate(all_states()):
                prev_key = '%s,%s,%s' % (prev_state.n, prev_state.s, prev_state.q)
                if prev_key not in transitions:
                    continue
                for col, next_state in enumerate(all_states()):
                    next_key = '%s,%s,%s' % (next_state.n, next_state.s, next_state.q)
                    if next_key not in transitions[prev_key]:
                        continue
                    value = transitions[prev_key][next_key]
                    f_sparse_dat.write('%s    %s    %s\n' % (row + 1, col + 1, value))

            # matlab recommends to force the largest row, largest column to define the correct matrix size
            last_state = [ s for s in all_states() ][-1]
            state_count = len([ s for s in all_states() ]) + 1
            last_state_key = '%s,%s,%s' % (last_state.n, last_state.s, last_state.q)
            if transitions[last_state_key][last_state_key] == 0:
                f_sparse_dat.write('%s    %s    %s\n' % (state_count, state_count, 0))

        sys.exit()

    def run(self):
        """Runs an MMCmodel simulation"""
        self.activate(self.scaler, self.scaler.execute())
        self.activate(self.user_generator, self.user_generator.execute())
        self.simulate(until=SECONDS_IN_A_YEAR)
        self.finalize_simulation()

        # self.generate_matrix_and_exit()

        return_dict = {}
        # compute batch means bp and number of servers
        bp, bp_delta = MonitorStatistics(self.mBlocked).batchmean
        num_servers, ns_delta = MonitorStatistics(self.mNumServers).batchmean
        # compute bp timescales
        bp_by_hour = MonitorStatistics(self.mBlocked).bp_by_hour
        bp_by_day = MonitorStatistics(self.mBlocked).bp_by_day
        bp_by_week = MonitorStatistics(self.mBlocked).bp_by_week
        bp_by_month = MonitorStatistics(self.mBlocked).bp_by_month
        bp_by_year = MonitorStatistics(self.mBlocked).bp_by_year

        #self.mServerProvisionLength = [[0, 15768000]] # Uncomment for fixed capacity utilization calculation
        # filter out first half of year data
        self.mServerProvisionLength = filter(lambda data: data[0] > 15768000, self.mServerProvisionLength)
        # subtract out the times from the first half of the year
        self.mServerProvisionLength = [[data[0] - 15768000, data[1]] for data in self.mServerProvisionLength]

        # filter out first half of year data
        self.mAcceptServiceTimes = filter(lambda data: data[0] > 15768000, self.mAcceptServiceTimes)
        # subtract out the times from the first half of the year
        self.mAcceptServiceTimes = [[data[0] - 15768000, data[1]] for data in self.mAcceptServiceTimes]

        # compute utilization
        mean_utilization = self.get_mean_utilization()

        # filter out first half of year data
        self.mWaitTime = filter(lambda data: data[0] > 15768000, self.mWaitTime)
        # subtract out the times from the first half of the year
        self.mWaitTime = [[data[0] - 15768000, data[1]] for data in self.mWaitTime]

        # Add in waiting time info
        wts = WaitTimeStatistics(self.mWaitTime)
        wait_times_by_hour = wts.wait_times_by_hour
        wait_times_by_day = wts.wait_times_by_day
        wait_times_by_week = wts.wait_times_by_week
        wait_times_by_month = wts.wait_times_by_month
        wait_times_by_year = wts.wait_times_by_year
        mean_wait_time, mean_wait_time_delta = wts.batchmean
        wait_times_year_99_percentile = np.percentile([c[1] for c in self.mWaitTime], 99)

        total_mServerProvisionLength = sum([data[1] for data in self.mServerProvisionLength])

        # add in bp percent error and bp raw timescales
        bp_percent_error = 100 * (bp_delta / bp if bp else 0)
        bp_timescale_raw = {'hour': bp_by_hour['bp_raw'],
                            'day': bp_by_day['bp_raw'],
                            'week': bp_by_week['bp_raw'],
                            'month': bp_by_month['bp_raw'],
                            'year': bp_by_year['bp_raw']}
        return_dict.update({'bp_batch_mean': bp,
                            'bp_batch_mean_delta': bp_delta,
                            'bp_by_hour': bp_by_hour,
                            'bp_by_day': bp_by_day,
                            'bp_by_week': bp_by_week,
                            'bp_by_month': bp_by_month,
                            'bp_by_year': bp_by_year,
                            'wait_times_by_hour': wait_times_by_hour,
                            'wait_times_by_day': wait_times_by_day,
                            'wait_times_by_week': wait_times_by_week,
                            'wait_times_by_month': wait_times_by_month,
                            'wait_times_by_year': wait_times_by_year,
                            'wait_times_batch_mean': mean_wait_time,
                            'wait_times_batch_mean_delta': mean_wait_time_delta,
                            'wait_times_year_99_percentile': wait_times_year_99_percentile,
                            'total_provisioned_time': total_mServerProvisionLength,
                            'num_servers': num_servers,
                            'ns_delta': ns_delta,
                            'bp_batch_mean_percent_error': bp_percent_error,
                            'mean_utilization': mean_utilization,
                            'bp_timescale_raw': bp_timescale_raw})
        return return_dict


class FixedSizePolicyFixedPoissonSim(MMCmodel):
    """Designed to run MMCmodel with a Fixed Size Policy and homeogeneous
       Poisson arrivals and departures.

    """

    def run(self, num_vms_in_cluster, density, scale_rate, lamda, mu,
            startup_delay_func, shutdown_delay, num_customers):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        num_vms_in_cluster -- the integer number of virtual machines that
            should be started
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        lamda -- the parameter to a Poisson distribution (in seconds)
            which defines the arrival process
        mu -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        num_customers -- the number of users to simulate

        """
        self.scaler = FixedSizePolicy(self, scale_rate, startup_delay_func,
                                      shutdown_delay, num_vms_in_cluster)
        self.cluster = Cluster(self, density=density)
        self.user_generator = PoissonGenerator(self, num_customers, lamda, mu)
        return MMCmodel.run(self)


class ReservePolicyFixedPoissonSim(MMCmodel):
    """Designed to run MMCmodel with Reserve Policy and homogeneous Poisson
       arrivals and departures.

    """

    def run(self, reserved, density, scale_rate, lamda, mu,
            startup_delay_func, shutdown_delay):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        reserved -- scale the cluster such that this value is less than the
            greatest number of available application seats
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        lamda -- the parameter to a Poisson distribution (in seconds)
            which defines the arrival process
        mu -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        """
        self.scaler = ReservePolicy(self, scale_rate, startup_delay_func,
                                    shutdown_delay, reserved)
        self.cluster = Cluster(self, density=density)
        self.user_generator = PoissonGenerator(self, lamda, mu)
        return MMCmodel.run(self)


class ReservePolicyDataFileUserSim(MMCmodel):
    """Designed to run MMCmodel with Reserve Policy against a user arrival and
       departure scheduled from a data file containing interarrival and service
       times.

    """

    def run(self, reserved, users_data_file_path, density, scale_rate,
            startup_delay_func, shutdown_delay):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        reserved -- scale the cluster such that this value is less than the
            greatest number of available application seats
        users_data_file_path -- a file path to the user data file with two
            comma separated columns. interarrival time, service time
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.scaler = ReservePolicy(self, scale_rate, startup_delay_func,
                                    shutdown_delay, reserved)
        self.cluster = Cluster(self, density=density)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        return MMCmodel.run(self)


class DataDrivenReservePolicySim(MMCmodel):
    """
    Designed to run MMCmodel with Data driven Reserve Policy with user
    arrivals and departures scheduled from a data file containing
    interarrival and service times.
    """

    def run(self, reserve_capacity_data_file, users_data_file_path, density,
            scale_rate, startup_delay_func, shutdown_delay):
        """
        Runs the simulation with user driven arrivals.

        Parameters:
        reserve_capacity_data_file -- The path to a file containing reserve
            capacity values per five-minute arrival periods.
        users_data_file_path -- a file path to the user data file with two
            comma separated columns. interarrival time, service time
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.scaler = DataDrivenReservePolicy(self, scale_rate,
                                              startup_delay_func,
                                              shutdown_delay,
                                              reserve_capacity_data_file)
        self.cluster = Cluster(self, density=density)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        return MMCmodel.run(self)


class ARHMMReservePolicySim(MMCmodel):
    """
    Designed to run MMCmodel with ARHMM Reserve Policy with user arrivals and
    departures scheduled from a data file containing interarrival and service
    times.
    """

    def run(self, five_minute_counts_file, users_data_file_path, density,
            scale_rate, startup_delay_func, shutdown_delay):
        """
        Runs the simulation with user driven a

        Parameters:
        five_minute_counts_file -- The path to a file containing all five-minute
            arrival counts.
        users_data_file_path -- a file path to the user data file with two
            comma separated columns. interarrival time, service time
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.scaler = ARHMMReservePolicy(self, scale_rate, startup_delay_func,
                                    shutdown_delay, five_minute_counts_file)
        self.cluster = Cluster(self, density=density)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        return MMCmodel.run(self)


class TimeVaryReservePolicyDataFileUserSim(MMCmodel):
    """Designed to run MMCmodel with Time Varying Reserve Policy against a user
       arrival and departure scheduled from a data file containing interarrival
       and service times.

    """

    def run(self, window_size, arrival_percentile, five_minute_counts_file,
            users_data_file_path, density, scale_rate, startup_delay_func,
            shutdown_delay):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        window_size -- If an integer, it is the number of previous five-minute
            counts to consider to build a percentile from. If set to None, the
            all previous values observed are seen.
        arrival_percentile -- The percentile of observed five-minute counts, to
            use as the value of R. R is used as the number of seats at the end
            of the scale operation.
        five_minute_counts_file -- The path to a file containing all five-minute
            arrival counts.
        users_data_file_path -- a file path to the user data file with two
            comma separated columns. interarrival time, service time
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.scaler = TimeVaryReservePolicy(self, scale_rate,
                                            startup_delay_func, shutdown_delay,
                                            window_size,
                                            arrival_percentile,
                                            five_minute_counts_file)
        self.cluster = Cluster(self, density=density)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        return MMCmodel.run(self)


class DataFilePolicyDataFileUserSim(MMCmodel):
    """Designed to run MMCmodel with a provisioning schedule enumerated in a
       data file, and arrivals and departures scheduled from a data file
       containing interarrival and service times.

    """

    def run(self, prov_data_file_path, users_data_file_path, density,
            startup_delay_func, shutdown_delay):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        prov_data_file_path -- a file path to the provisioning data file with
            two comma separated columns. provision time, deprovision time
        users_data_file_path -- a file path to the user data file with two
            comma separated columns. interarrival time, service time
        density -- the number of application seats per virtual machine
        startup_delay_func -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.scaler = GenericDataFileScaler(self, startup_delay_func,
                                            shutdown_delay,
                                            prov_data_file_path)
        self.cluster = Cluster(self, density=density)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        return MMCmodel.run(self)


class ErlangDataPolicyDataFileUserSim(MMCmodel):
    """Designed to run MMCmodel with ErlangBFormulaDataPolicy against a user
       arrival and departure scheduled from a data file containing interarrival
       and service times.

    """

    def run(self, worst_bp, pred_user_count_file_path, mu, users_data_file_path,
            lag, density, scale_rate, startup_delay_func, shutdown_delay):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        worst_bp -- ensure the blocking probability does exceed this value as
            computed by the closed form Erlang B formula.
        pred_user_count_file_path -- a file path to the arrival predictions
            data file with one comma separated column containing user count
            values
        mu -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process. Used for prediction
            purposes.
        users_data_file_path -- a file path to the user data file with two
            comma separated columns. interarrival time, service time
        lag -- an integer number of periods to lag values in
            pred_user_count_file_path by. Effectively, this introduces lag
            number of zeros at the beginning of pred_user_count_file
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.cluster = Cluster(self, density=density)
        self.scaler = ErlangBFormulaDataPolicy(self, scale_rate,
                                               startup_delay_func,
                                               shutdown_delay, worst_bp,
                                               pred_user_count_file_path, mu,
                                               lag)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        return MMCmodel.run(self)
