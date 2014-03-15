from random import seed
import sqlite3

from SimPy.Simulation import *

from cluster import Cluster
from appsim.scaler.reserve_policy import ReservePolicy
#from appsim.scaler.ode_policy import OdePolicy
from appsim.scaler.fixed_size_policy import FixedSizePolicy
from appsim.scaler.data_file_policy import GenericDataFileScaler
from appsim.scaler.erlang_b_formula_policy import ErlangBFormulaDataPolicy
from tools import MonitorStatistics
from user_generators import PoissonGenerator, DataFileGenerator
from user_generators import NoMoreUsersException
from billable_time import HourMinimumBillablePolicy

from scaler.reserve_policy import ReservePolicy
#from scaler.ode_policy import OdePolicy


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
        self.mClusterActive = Monitor(sim=self)  # monitor cluster.active with each scale event
        self.mClusterBooting = Monitor(sim=self)  # monitor cluster.booting with each scale event
        self.mClusterShuttingDown = Monitor(sim=self)  # monitor cluster.shutting_down with each scale event
        self.mClusterOccupancy = Monitor(sim=self)  # monitor the number of utilized seats with each scale event
        ### Customer Monitors
        self.mBlocked = Monitor(sim=self)  # monitor observing if a customer is blocked or not
        self.mNumServers = Monitor(sim=self)  # monitor observing cluster num_servers active+booting+shutting
        ### Cost Monitors
        self.mServerProvisionLength = Monitor(sim=self)  # monitor observing the length a server was online when it is deprovisioned
        self.msT = Monitor(sim=self)  # monitor for the generated service times
        self.mLostServiceTimes = Monitor(sim=self)  # monitor for the generated service times for customers who are were blocked

    def finalize_simulation(self):
        self._observe_existing_customers()
        self._observe_running_servers_billable_time()
        self._adjust_mLostServiceTimes()

    def get_utilization(self):
        total_seat_time = self.mServerProvisionLength.total() * self.cluster.density
        used_seat_time = self.msT.total()
        return used_seat_time / total_seat_time

    def _observe_running_servers_billable_time(self):
        """Observes the billable time of servers that are still running assuming the stop now"""
        c = self.cluster
        for server in c.active + c.booting + c.shutting_down:
            self.mServerProvisionLength.observe(self.now() - server.start_time)  # monitor the servers provision, deprovision time
        c.active = c.booting = c.shutting_down = []

    def _observe_existing_customers(self):
        """Observes the billable time of customers that are still running assuming they leave now"""
        for active in self.cluster.active:
            for user in active.activeQ:
                self.msT.observe(self.now() - user.arrival_time)

    def _adjust_mLostServiceTimes(self):
        """Removes from mLostServiceTimes any service times that go further into the future than the simulation did"""
        current_sim_time = self.now()
        for i, item in enumerate(self.mLostServiceTimes):
            if sum(item) > current_sim_time:
                new_service_time = current_sim_time - item[0]
                self.mLostServiceTimes[i][1] = new_service_time

    def run(self):
        """Runs an MMCmodel simulation"""
        self.activate(self.scaler,self.scaler.execute())
        self.activate(self.user_generator,self.user_generator.execute())
        try:
            self.simulate(until=10**30)
        except NoMoreUsersException:
            self.stopSimulation()
        self.finalize_simulation()
        return_dict = self.cost_policy.run()
        #limit to exatly 1 year of data
        one_year_mBlocked = filter(lambda x: x[0] <= 31449600, self.mBlocked)
        (bp, bp_delta) = MonitorStatistics(one_year_mBlocked).batchmean
        (num_servers, ns_delta) = MonitorStatistics(self.mNumServers).batchmean
        bp_by_hour = MonitorStatistics(one_year_mBlocked).bp_by_hour
        bp_by_day = MonitorStatistics(one_year_mBlocked).bp_by_day
        bp_by_week = MonitorStatistics(one_year_mBlocked).bp_by_week
        bp_by_month = MonitorStatistics(one_year_mBlocked).bp_by_month
        bp_by_year = MonitorStatistics(one_year_mBlocked).bp_by_year
        utilization = self.get_utilization()
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
                            'num_servers': num_servers, 
                            'ns_delta': ns_delta, 
                            'bp_batch_mean_percent_error': bp_percent_error, 
                            'utilization': utilization,
                            'bp_timescale_raw': bp_timescale_raw})
        return return_dict


class FixedSizePolicyFixedPoissonSim(MMCmodel):
    """Designed to run MMCmodel with a Fixed Size Policy and homeogeneous
       Poisson arrivals and departures.

    """

    def run(self, num_vms_in_cluster, density, scale_rate, lamda, mu,
                startup_delay, shutdown_delay, num_customers):
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
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        num_customers -- the number of users to simulate

        """
        self.scaler = FixedSizePolicy(self, scale_rate, startup_delay, shutdown_delay, num_vms_in_cluster)
        self.cluster = Cluster(self, density=density)
        self.user_generator = PoissonGenerator(self, num_customers, lamda, mu)
        self.cost_policy = HourMinimumBillablePolicy(self)
        return MMCmodel.run(self)


class ReservePolicyFixedPoissonSim(MMCmodel):
    """Designed to run MMCmodel with Reserve Policy and homogeneous Poisson
       arrivals and departures.

    """

    def run(self, reserved, density, scale_rate, lamda, mu,
                startup_delay, shutdown_delay, num_customers):
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
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        num_customers -- the number of users to simulate

        """
        self.scaler = ReservePolicy(self, scale_rate, startup_delay, shutdown_delay, reserved)
        self.cluster = Cluster(self, density=density)
        self.user_generator = PoissonGenerator(self, num_customers, lamda, mu)
        self.cost_policy = HourMinimumBillablePolicy(self)
        return MMCmodel.run(self)


class ReservePolicyDataFileUserSim(MMCmodel):
    """Designed to run MMCmodel with Reserve Policy against a user arrival and
       departure scheduled from a data file containing interarrival and service
       times.

    """

    def run(self, reserved, users_data_file_path, density, scale_rate,
                startup_delay, shutdown_delay):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        reserved -- scale the cluster such that this value is less than the
            greatest number of available application seats
	users_data_file_path -- a file path to the user data file with two
            comma separated columns.  interarrival time, service time
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.scaler = ReservePolicy(self, scale_rate, startup_delay, shutdown_delay, reserved)
        self.cluster = Cluster(self, density=density)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        self.cost_policy = HourMinimumBillablePolicy(self)
        return MMCmodel.run(self)


class DataFilePolicyDataFileUserSim(MMCmodel):
    """Designed to run MMCmodel with a provisioning schedule enumerated in a
       data file, and arrivals and departures scheduled from a data file
       containing interarrival and service times.

    """

    def run(self, prov_data_file_path, users_data_file_path, density,
                startup_delay, shutdown_delay):
        """Runs the simulation with the following arguments and returns result

        Parameters:
	prov_data_file_path -- a file path to the provisioning data file with
	    two comma separated columns.  provision time, deprovision time
	users_data_file_path -- a file path to the user data file with two
            comma separated columns.  interarrival time, service time
        density -- the number of application seats per virtual machine
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.scaler = GenericDataFileScaler(self, startup_delay, shutdown_delay, prov_data_file_path)
        self.cluster = Cluster(self, density=density)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        self.cost_policy = HourMinimumBillablePolicy(self)
        return MMCmodel.run(self)


class ErlangDataPolicyDataFileUserSim(MMCmodel):
    """Designed to run MMCmodel with ErlangBFormulaDataPolicy against a user
       arrival and departure scheduled from a data file containing interarrival
       and service times.

    """

    def run(self, worst_bp, pred_user_count_file_path, mu, users_data_file_path,
		lag, density, scale_rate, startup_delay, shutdown_delay):
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
            comma separated columns.  interarrival time, service time
	lag -- an integer number of periods to lag values in
	    pred_user_count_file_path by. Effectively, this introduces lag
            number of zeros at the beginning of pred_user_count_file
        density -- the number of application seats per virtual machine
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """
        self.cluster = Cluster(self, density=density)
        self.scaler = ErlangBFormulaDataPolicy(self, scale_rate, startup_delay, shutdown_delay, worst_bp, pred_user_count_file_path, mu, lag)
        self.user_generator = DataFileGenerator(self, users_data_file_path)
        self.cost_policy = HourMinimumBillablePolicy(self)
        return MMCmodel.run(self)
