from random import seed
import sqlite3
#import pylab
from itertools import product

from SimPy.Simulation import *

from cluster import Cluster
from appsim.scaler.reserve_policy import ReservePolicy
from appsim.scaler.ode_policy import OdePolicy
from tools import MonitorStatistics
from user import Generator

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
        --Type: appsim.user.Generator

    """ 

    def __init__(self, rand_seed=333555777):
        """Initializer for an MMCmodel

        Parameters:
        rand_seed -- the number to seed the random number generator with

        """
        seed(rand_seed)
        self.initialize()
        ### Simulation Monitors
        self.msT = Monitor(sim=self)  # monitor for the generated service times
        ### Scale Monitors
        self.mClusterActive = Monitor(sim=self)  # monitor cluster.active with each scale event
        self.mClusterBooting = Monitor(sim=self)  # monitor cluster.booting with each scale event
        self.mClusterShuttingDown = Monitor(sim=self)  # monitor cluster.shutting_down with each scale event
        ### Customer Monitors
        self.mBlocked = Monitor(sim=self)  # monitor observing if a customer is blocked or not
        self.mNumServers = Monitor(sim=self)  # monitor observing cluster num_servers active+booting+shutting

    def run(self):
        """Runs an MMCmodel simulation"""
        self.activate(self.scaler,self.scaler.execute())
        self.activate(self.user_generator,self.user_generator.execute())
        simulationLength = (self.user_generator.maxCustomers + 2) * (1.0 / self.user_generator.lamda)
        self.simulate(until=simulationLength)

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
        self.user_generator = Generator(self, num_customers, lamda, mu)
        MMCmodel.run(self)
        (bp, bp_delta) = MonitorStatistics(self.mBlocked).batchmean
        (num_servers, ns_delta) = MonitorStatistics(self.mNumServers).batchmean
        return {'bp': bp, 'bp_delta': bp_delta, 'num_servers': num_servers, 'ns_delta': ns_delta}

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
        self.user_generator = Generator(self, num_customers, lamda, mu)
        MMCmodel.run(self)
        (bp, bp_delta) = MonitorStatistics(self.mBlocked).batchmean
        (num_servers, ns_delta) = MonitorStatistics(self.mNumServers).batchmean
        return {'bp': bp, 'bp_delta': bp_delta, 'num_servers': num_servers, 'ns_delta': ns_delta}

class AppSim(object):

    """AppSim is a generalized simulation runner for the MMCmodel simulation.

    AppSim iterates simulation parameters to run a variety of MMCModels.
    AppSim supports pluggable scaler policies, and records input and output
    values to a sqlite database.

    """

    def __init__(self, output_dir, file_name, num_customers):
        """Initializes an AppSim object

        Parameters:
        output_dir -- the output data (sqlite db) and graphs to be saved to
        file_name -- the filename all files should be be generated with
        num_customers -- the maximum number of customers to simulate

        """
        self.OUTPUT_DIR = output_dir
        self.SIM_NAME = file_name
        self.conn = sqlite3.connect(self.OUTPUT_DIR + self.SIM_NAME + '.db')
        self.c = self.conn.cursor()
        self.max_cust = num_customers

        # Create table
        """
            try:
                self.c.execute('''drop table ode_policy''')
            except sqlite3.OperationalError:
                pass
            self.c.execute('''create table ode_policy
                (ode_delta real, worst_cp real, density real, stime real, rate real, scale_rate real, bp real, bp_delta real, num_servers real, ns_delta real)''')"""

    def configurations(self):
        """A generator for all configurations of these simulation parameters"""
        prod_input = []
        for key in self.variables:
            if isinstance(self.variables[key], tuple):
                if len(self.variables[key]) == 2:
                    prod_input.append([{key: i} for i in range(self.variables[key][0], self.variables[key][1]+1)])
                elif len(self.variables[key]) == 3:
                    prod_input.append([{key: i} for i in range(self.variables[key][0], self.variables[key][1]+1, self.variables[key][2])])
            elif isinstance(self.variables[key], int) or isinstance(self.variables[key], float):
                prod_input.append([{key: self.variables[key]}])
        # Generate all combinations
        for single_run in product(*prod_input):
            new_dict = {}
            for item in single_run:
                for k in iter(item):
                    new_dict[k] = item[k]
            yield new_dict

    def runOdePolicy(self):
        """Runs all configurations for the OdePolicy

        This function manages a table named ode_policy and posts
        all output data into this table.  The table is created fresh each
        time this function is called

       """
        try:
            self.c.execute('''drop table ode_policy''')
        except sqlite3.OperationalError:
            pass
        self.c.execute('''create table ode_policy
            (delta real, worst_bp real, density real, lamda real, mu real, scale_period real, bp real, bp_delta real, num_servers real, ns_delta real)''')

        import pprint
        for params in self.configurations():
            pprint.pprint(params)
        for params in self.configurations():
            the_sim = MMCmodel()
            the_sim.scaler = OdePolicy(the_sim, params['scale_period'], params['startup_delay'], params['shutdown_delay'], params['delta'], params['worst_bp'], params['lamda'], params['mu'])
            the_sim.cluster = Cluster(the_sim, density=params['density'])
            the_sim.user_generator = Generator(the_sim, self.max_cust, params['lamda'], params['mu'])
            the_sim.run()
            (bp, bp_delta) = MonitorStatistics(the_sim.mBlocked).batchmean
            (num_servers, ns_delta) = MonitorStatistics(the_sim.mNumServers).batchmean
            # Insert a row of data
            self.c.execute("""insert into ode_policy
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (params['delta'], params['worst_bp'], params['density'], params['lamda'], params['mu'], params['scale_period'], bp, bp_delta, num_servers, ns_delta))
            self.conn.commit()
            print '(%s, %s, %s, %s) = save (delta=%s, worst_bp=%s, density=%d, lamda=%s, mu=%s, maxCustomers=%d, scale_period=%s)' % (bp, bp_delta, num_servers, ns_delta, params['delta'], params['worst_bp'], params['density'], params['lamda'], params['mu'], self.max_cust, params['scale_period'])

    def runReservePolicy(self):
        """Runs all configurations for the ReservePolicy

        This function manages a table named reserve_policy and posts
        all output data into this table.  The table is created fresh each
        time this function is called

       """
        try:
            self.c.execute('''drop table reserve_policy''')
        except sqlite3.OperationalError:
            pass
        self.c.execute('''create table reserve_policy
            (reserved real, density real, lamda real, mu real, scale_period real, bp real, bp_delta real, num_servers real, ns_delta real)''')

        for params in self.configurations():
            the_sim = MMCmodel()
            the_sim.scaler = ReservePolicy(the_sim, params['scale_period'], params['startup_delay'], params['shutdown_delay'], params['reserved'])
            the_sim.cluster = Cluster(the_sim, density=params['density'])
            the_sim.user_generator = Generator(the_sim, self.max_cust, params['lamda'], params['mu'])
            the_sim.run()
            (bp, bp_delta) = MonitorStatistics(the_sim.mBlocked).batchmean
            (num_servers, ns_delta) = MonitorStatistics(the_sim.mNumServers).batchmean
            # Insert a row of data
            self.c.execute("""insert into reserve_policy
                values (?, ?, ?, ?, ?, ?, ?, ?, ?)""", (params['reserved'], params['density'], params['lamda'], params['mu'], params['scale_period'], bp, bp_delta, num_servers, ns_delta))
            self.conn.commit()
            print '(%s, %s, %s, %s) = save (reserved=%d, density=%d, lamda=%s, mu=%s, maxCustomers=%d, scale_period=%s)' % (bp, bp_delta, num_servers, ns_delta, params['reserved'], params['density'], params['lamda'], params['mu'], self.max_cust, params['scale_period'])

    def createGraph(self, table_name, xlab, ylab, stack_lab):
        """This function produces a graph from the saved sqlite data

        parameters:
        table_name -- the name of the sqlite table to pull the data from
        xlab -- the column name of the x axis label
        ylab -- the column name of the y axis label
        stack_lab -- the column name of the value to be stacked

        """
        ###
        styles = ['b-' ,'g-', 'r-', 'm-', 'k-', 'b--' ,'g--', 'r--', 'm--', 'k--', 'b-.' ,'g-.', 'r-.', 'm-.', 'k-.', 'b:' ,'g:', 'r:', 'm:', 'k:']
        import pylab
        f1 = pylab.figure()
        #for i, v in enumerate(X.T):
        #    p.plot(t, v, styles[i], label='p%s'%i)
        ###
        stack_query = 'SELECT DISTINCT %s FROM %s' % (stack_lab, table_name)
        stack_vars = [item[0] for item in self.c.execute(stack_query)]
        for item in stack_vars:
            xvalues = []
            yvalues = []
            line_query = 'SELECT %s, %s from %s WHERE %s=%s ORDER BY %s ASC' % (xlab, ylab, table_name, stack_lab, item, xlab)
            line_query_results = self.c.execute(line_query)
            for point in line_query_results:
                xvalues.append(point[0])
                yvalues.append(point[1])
            pylab.plot(xvalues, yvalues, label='%s=%s' % (stack_lab,item))
        v = pylab.axis()
        xdist = (v[1] - v[0]) * 0.1
        ydist = (v[3] - v[2]) * 0.1
        print 'xdist = %s' % xdist
        print 'ydist = %s' % ydist
        print 'v = \n'
        print v
        v = (v[0] - xdist, v[1] + xdist, v[2] - ydist, v[2] + ydist)
        print 'v = \n'
        print v
        pylab.axis(v)
        pylab.grid()
        pylab.legend(loc='best')
        pylab.xlabel(xlab)
        pylab.ylabel(ylab)
        if ylab == 'bp':
            pylab.title('Blocking Probability vs %s and %s' % (xlab, stack_lab))
        elif ylab == 'bp_delta':
            pylab.title('Blocking Probability Confidence Value vs %s and %s' % (xlab, stack_lab))
        elif ylab == 'num_servers':
            pylab.title('Cluster size in VMs vs %s and %s' % (xlab, stack_lab))
        elif ylab == 'ns_delta':
            pylab.title('Cluster Size Confidence Value vs %s and %s' % (xlab, stack_lab))
        f1.savefig(self.OUTPUT_DIR + self.SIM_NAME + '.png')
        #rows = self.c.execute('select bp, reserved, density from reserve_policy WHERE density=? ORDER BY reserved ASC', (1,))
        #self.c.execute('select ? from reserve_policy', xlab)
        #for row in rows:
        #    print row
        #c.execute('select ? from reserve_policy where symbol=?', t)
