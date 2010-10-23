from random import expovariate

from SimPy.SimulationTrace import * ## Only change in program!

class Generator(Process):

    """Generates Users which arrive at the application cluster"""

    def __init__(self, sim, maxCustomers, lamda, mu):
        """Creates a user generator
        sim -- the sim users should arrive at for service
        maxCustomers -- the number of users to simulate
        lamda -- the parameter to a Poisson distribution (in seconds)
            which defines the arrival process
        mu -- the parameter to a Poisson distribution (in seconds)
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

class User(Process):

    """Users request an application and hold it for an exponential time

    Class variables:
    NoInSystem -- The total number of users which have been generated
    NoAccepted -- The number of users who received service
    NoDenied -- The number of users who were denied service

    """
        
    NoTotal = 0
    def execute(self, stime, cluster):
        """Simulate a single user

        Parameters:
        stime -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process
        cluster -- the cluster this user is to arrive at

        """
        User.NoTotal += 1
        server = cluster.find_server()
        cluster_size = len(cluster.booting) + len(cluster.active) \
            + len(cluster.shutting_down)
        self.sim.mNumServers.observe(cluster_size)
        if server is None:
            self.sim.mBlocked.observe(1)
        else:
            yield request, self, server
            self.sim.mBlocked.observe(0)
            t = expovariate(stime)
            self.sim.msT.observe(t)
            yield hold, self, t
            yield release, self, server

