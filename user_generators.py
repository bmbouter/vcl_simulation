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
                    return
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
            L = User("User %s" % i, sim=self.sim)
            self.sim.activate(L, L.execute(self.mu, self.sim.cluster), delay=0)
            yield hold,self,expovariate(self.lamda)
