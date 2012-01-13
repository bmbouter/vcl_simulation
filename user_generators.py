from random import expovariate
import csv

from SimPy.SimulationTrace import * ## Only change in program!

from user import User

class NoMoreUsersException(Exception):
    pass

class IPPGenerator(Process):
    """Generates Users at a IPP rate of lamda, r, c-squared"""

    def __init__(self, sim, maxCustomers, lamda_avg, c_squared, r, mu):
        """Creates the IPP user generator

        sim -- the sim users should arrive at for service
        maxCustomers -- the number of users to simulate
        lamda_avg -- the average lamda arrival Poisson rate
        c_squared -- the squared coefficient of variation
        r -- the coefficient of burstiness
        mu -- the parameter to a Poisson distribution
            which defines the service time process

        """
        self.maxCustomers = maxCustomers
        self.lamda_avg = lamda_avg
        self.c_squared = c_squared
        self.r = r
        self.mu = mu
        self.sim = sim
        Process.__init__(self, name='User Arrival Generator', sim=self.sim)

    @property
    def lamda(self):
        """A legacy attribute used to calculate total simulation length"""
        return self.lamda_avg

    @property
    def phi(self):
        """Returns the value of phi"""
        numerator = 2 * self.lamda_avg * (1 - self.r) * (1- self.r)
        denominator = self.r * (self.c_squared - 1)
        return numerator / denominator

    @property
    def psi(self):
        """Returns the value of psi"""
        numerator = 2 * self.lamda_avg * (1 - self.r)
        denominator = self.c_squared - 1
        return numerator / denominator

    @property
    def lamda_on(self):
        """Returns the value of lamda_on"""
        return self.lamda_avg / self.r

    def execute(self):
        """Begins user generations to the application cluster"""

        num_cust = 0
        while True:
            on_to_off_time = expovariate(self.phi) + self.sim.now()
            interarrival_time = expovariate(self.lamda_on)
            while self.sim.now() + interarrival_time <= on_to_off_time:
                yield hold,self,interarrival_time
                L = User("User %s" % num_cust, sim=self.sim)
                self.sim.activate(L, L.execute(self.mu, self.sim.cluster), delay=0)
                num_cust = num_cust + 1
                if num_cust >= self.maxCustomers:
                    raise NoMoreUsersException()
                interarrival_time = expovariate(self.lamda_on)
            yield hold,self,expovariate(self.psi) + on_to_off_time - self.sim.now()

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
        raise NoMoreUsersException()

class DataFileGenerator(Process):
    """Generates Users according a Data file"""

    def __init__(self, sim, data_path):
        """Creates the Data driven  user generator

        sim -- the sim users should arrive at for service
        data_path -- the full path to the data file

        """
        self.data_path = data_path
        self.sim = sim
        Process.__init__(self, name='User Arrival Generator', sim=self.sim)

    def execute(self):
        """Begins user generations to the application cluster

        csv files should be comma separated in the form interarrival,service"""

        dataReader = csv.reader(open(self.data_path, 'rb'), delimiter=',')
        user_num = 1
        for row in dataReader:
            interarrival_time = float(row[0])
            service_time = float(row[1])
            yield hold,self,interarrival_time
            L = User("User %s" % user_num, sim=self.sim)
            self.sim.activate(L, L.execute(service_time, self.sim.cluster), delay=0)
            user_num = user_num + 1
        raise NoMoreUsersException()
