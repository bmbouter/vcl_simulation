from random import expovariate

from SimPy.SimulationTrace import * ## Only change in program!

from user import User

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

    def execute(self):
        """Begins user generations to the application cluster"""

        for i in range(self.maxCustomers):
            L = User("User "+`i`,sim=self.sim)
            self.sim.activate(L, L.execute(self.mu, self.sim.cluster), delay=0)
            yield hold,self,expovariate(self.lamda)

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
            L = User("User "+`i`,sim=self.sim)
            self.sim.activate(L, L.execute(self.mu, self.sim.cluster), delay=0)
            yield hold,self,expovariate(self.lamda)
