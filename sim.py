from random import seed
import sqlite3

from SimPy.Simulation import *

from cluster import Cluster
from appsim.scaler.reserve_policy import ReservePolicy
from appsim.scaler.ode_policy import OdePolicy
from appsim.scaler.fixed_size_policy import FixedSizePolicy
from appsim.scaler.erlang_b_formula_policy import ErlangBFormulaPolicy
from tools import MonitorStatistics
from user_generators import PoissonGenerator, IPPGenerator
from billable_time import HourMinimumBillablePolicy

from scaler.reserve_policy import ReservePolicy
from scaler.ode_policy import OdePolicy


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

    def run(self):
        """Runs an MMCmodel simulation"""
        self.activate(self.scaler,self.scaler.execute())
        self.activate(self.user_generator,self.user_generator.execute())
        simulationLength = (self.user_generator.maxCustomers + 2) * (1.0 / self.user_generator.lamda)
        self.simulate(until=simulationLength)
        return_dict = self.cost_policy.run()
        (bp, bp_delta) = MonitorStatistics(self.mBlocked).batchmean
        (num_servers, ns_delta) = MonitorStatistics(self.mNumServers).batchmean
        return_dict.update({'bp': bp, 'bp_delta': bp_delta, 'num_servers': num_servers, 'ns_delta': ns_delta})
        return return_dict

class FixedSizePolicySim(MMCmodel):
    """Designed to run MMCmodel with a Fixed Size Policy"""

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

class ErlangBFormulaPolicySim(MMCmodel):
    """Designed to run MMCmodel with Erlang B Closed Form Policy"""

    def run(self, worst_bp, density, scale_rate, lamda, mu, startup_delay,
                shutdown_delay, num_customers):
        """Runs the simulation with the following arguments and returns result

        Parameters:
	worst_bp -- the worst bp the ErlangBFormulaPolicy will try to enforce
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
        self.scaler = ErlangBFormulaPolicy(self, scale_rate, startup_delay, shutdown_delay, worst_bp, lamda, mu)
        self.cluster = Cluster(self, density=density)
        self.user_generator = PoissonGenerator(self, num_customers, lamda, mu)
        #self.user_generator = IPPGenerator(self, num_customers, lamda, 2.0, 0.99, mu)
        self.cost_policy = HourMinimumBillablePolicy(self)
        return MMCmodel.run(self)

class OdePolicySim(MMCmodel):
    """Designed to run MMCmodel with ODE Policy"""

    def run(self, worst_bp, delta, density, scale_rate, lamda, mu,
                startup_delay, shutdown_delay, num_customers):
        """Runs the simulation with the following arguments and returns result

        Parameters:
        worst_bp -- the worst bp the OdePolicy will try to enforce
        delta -- the numer of seconds the OdePolicy should maintain
            the predicted blocking probabilities less than worst_bp
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
        self.scaler = OdePolicy(self, scale_rate, startup_delay, shutdown_delay, delta, worst_bp, lamda, mu)
        self.cluster = Cluster(self, density=density)
        self.user_generator = PoissonGenerator(self, num_customers, lamda, mu)
        self.cost_policy = HourMinimumBillablePolicy(self)
        return MMCmodel.run(self)

class ReservePolicySim(MMCmodel):
    """Designed to run MMCmodel with Reserve Policy"""

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
