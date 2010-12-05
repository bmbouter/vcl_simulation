from random import expovariate

from SimPy.SimulationTrace import * ## Only change in program!

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
        self.arrival_time = self.sim.now()
        t = expovariate(stime)
        if server is None:
            self.sim.mBlocked.observe(1)
            self.sim.mLostServiceTimes.observe(t)
        else:
            yield request, self, server
            self.sim.mBlocked.observe(0)
            yield hold, self, t
            self.sim.msT.observe(t)
            yield release, self, server

