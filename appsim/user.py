from SimPy.SimulationTrace import * ## Only change in program!

class User(Process):

    """Users request an application and hold it for an exponential time

    Class variables:
    NoInSystem -- The total number of users which have been generated
    NoAccepted -- The number of users who received service
    NoDenied -- The number of users who were denied service

    """
        
    NoTotal = 0
    def execute(self, stime, cluster, loss_assumption):
        """Simulate a single user

        Parameters:
        stime -- the service time the user should use service for
        cluster -- the cluster this user is to arrive at
        loss_assumption -- If True, a customer who is blocked leaves the
                           system immediately. Otherwise customers stay
                           around in a FCFS queue until they can be served.
        """
        User.NoTotal += 1
        if not loss_assumption:
            yield request, self, cluster.cluster_resource
        server = cluster.find_server()
        cluster_size = len(cluster.booting) + len(cluster.active) \
            + len(cluster.shutting_down)
        self.sim.mNumServers.observe(cluster_size)
        self.arrival_time = self.sim.now()
        if server is None:
            self.sim.mBlocked.observe(1)
            self.sim.mLostServiceTimes.observe(stime)
        else:
            self.sim.mBlocked.observe(0)
            self.sim.mAcceptServiceTimes.observe(stime)
            yield request, self, server
            yield hold, self, stime
            yield release, self, server
        if not loss_assumption:
            yield release, self, cluster.cluster_resource
