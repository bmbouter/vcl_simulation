from random import expovariate
import csv

from SimPy.SimulationTrace import * ## Only change in program!

from user import User


class PoissonGenerator(Process):
    """Generates Users at a Poisson rate of lamda"""

    def __init__(self, sim, maxCustomers, lamda, mu):
        """Creates the Poisson user generator

        sim -- the sim users should arrive at for service
        maxCustomers -- the number of users to simulate
        lamda -- the parameter to a Poisson distribution
            which defines the arrival process
        mu -- the parameter to a Poisson distribution
            which defines the service time process

        """
        self.maxCustomers = maxCustomers
        self.lamda = lamda
        self.mu = mu
        self.sim = sim
        Process.__init__(self, name='User Arrival Generator', sim=self.sim)

    def execute(self):
        """Begins user generations to the application cluster"""

        for i in range(self.maxCustomers):
            yield hold,self,expovariate(self.lamda)
            L = User("User %s" % i, sim=self.sim)
            self.sim.activate(L, L.execute(expovariate(self.mu), self.sim.cluster), delay=0)


class DataFileGenerator(Process):
    """Generates Users according a Data file"""

    def __init__(self, sim, data_path):
        """Creates the Data driven  user generator

        sim -- the sim users should arrive at for service
        data_path -- the full path to the data file

        """
        self.data_path = data_path
        self.sim = sim
        self.user_count_since_last_scale = None
        Process.__init__(self, name='User Arrival Generator', sim=self.sim)

    def reset_user_count_since_scale(self):
        """
        Resets the user count to 0 so that the counter can increment it as new users arrive.

        :return: None
        """
        self.user_count_since_last_scale = 0

    def get_user_count_since_scale(self):
        """
        Return the user count since the last time reset_user_count_since_scale() was called

        :return: The integer count of users that have arrived since the last call to reset_user_count_since_scale()
        :rtype: int
        """
        return self.user_count_since_last_scale

    def execute(self):
        """Begins user generations to the application cluster

        csv files should be comma separated in the form interarrival,service
        """
        dataReader = csv.reader(open(self.data_path, 'rb'), delimiter=',')
        user_num = 1
        for row in dataReader:
            interarrival_time = float(row[0])
            service_time = float(row[1])
            yield hold, self, interarrival_time
            self.sim.arrivalMonitor.observe(1)
            if service_time < 0:
                raise Exception('Service time should not be less than 0')
            L = User("User %s" % user_num, sim=self.sim)
            user_num = user_num + 1
            self.user_count_since_last_scale = self.user_count_since_last_scale + 1
            self.sim.activate(L, L.execute(service_time, self.sim.cluster), delay=0)
