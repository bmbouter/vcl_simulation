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
from tools import MonitorStatistics, SECONDS_IN_A_YEAR, WaitTimeStatistics
from user_generators import PoissonGenerator, DataFileGenerator


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

    def run(self):
        """Runs an MMCmodel simulation"""
        self.activate(self.scaler, self.scaler.execute())
        self.activate(self.user_generator, self.user_generator.execute())
        self.simulate(until=SECONDS_IN_A_YEAR)
        self.finalize_simulation()
        return_dict = {}
        # compute batch means bp and number of servers
        (bp, bp_delta) = MonitorStatistics(self.mBlocked).batchmean
        (num_servers, ns_delta) = MonitorStatistics(self.mNumServers).batchmean
        # compute bp timescales
        bp_by_hour = MonitorStatistics(self.mBlocked).bp_by_hour
        bp_by_day = MonitorStatistics(self.mBlocked).bp_by_day
        bp_by_week = MonitorStatistics(self.mBlocked).bp_by_week
        bp_by_month = MonitorStatistics(self.mBlocked).bp_by_month
        bp_by_year = MonitorStatistics(self.mBlocked).bp_by_year

        # time_interval = float(300)
        # num_buckets = int(math.ceil(SECONDS_IN_A_YEAR / time_interval))
        # bucket_observations = [[] for i in range(num_buckets)]
        # for observation in self.arrivalMonitor:
        #     observation_time = observation[0]
        #     index = int(math.floor(observation_time / time_interval))
        #     bucket_observations[index].append(observation)
        # arrival_counts = [len(observation) for observation in bucket_observations]
        # counts_file = open('data/2008_five_minute_counts.csv', 'r')
        # #counts_file = open('/tmp/observed_arrivals.csv', 'r')
        # diffs = []
        # o = open('/tmp/observed_arrivals.csv', 'w')
        # for i, line in enumerate(counts_file):
        #     o.write('%s\n' % arrival_counts[i])
        #     count = float(line)
        #     if count != arrival_counts[i]:
        #         diff = arrival_counts[i] - count
        #         print 'diff: %s at position %s' % (diff, i)
        #         diffs.append(diff)
        #
        # o.close()

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
        (mean_wait_time, mean_wait_time_delta) = wts.batchmean
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
            startup_delay_func, shutdown_delay, num_customers):
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
        num_customers -- the number of users to simulate

        """
        self.scaler = ReservePolicy(self, scale_rate, startup_delay_func,
                                    shutdown_delay, reserved)
        self.cluster = Cluster(self, density=density)
        self.user_generator = PoissonGenerator(self, num_customers, lamda, mu)
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
