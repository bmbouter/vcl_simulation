from SimPy.SimulationTrace import * ## Only change in program!


active_users = {}


class User(Process):

    """
    Users request an application and hold it for an exponential time

    Class variables:
    NoInSystem -- The total number of users which have been generated
    NoAccepted -- The number of users who received service
    NoDenied -- The number of users who were denied service
    """

    user_count = 0
    def execute(self, stime, cluster, loss_assumption):
        """Simulate a single user

        Parameters:
        stime -- the service time the user should use service for
        cluster -- the cluster this user is to arrive at
        loss_assumption -- If True, a customer who is blocked leaves the
                           system immediately. Otherwise customers stay
                           around in a FCFS queue until they can be served.
        """
        global active_users
        self.id = User.user_count
        User.user_count = User.user_count + 1
        self.arrival_time = self.sim.now()
        if not loss_assumption:
            yield request, self, cluster.cluster_resource
        server = cluster.find_server()
        if not loss_assumption and server is None:
            raise RuntimeError('This customer was not able to find a server. With loss assumption=True it should be guaranteed to find one. Something is wrong.')
        self.wait_time = self.sim.now() - self.arrival_time
        self.sim.mWaitTime.observe(self.wait_time)
        cluster_size = len(cluster.booting) + len(cluster.active) \
            + len(cluster.shutting_down)
        self.sim.mNumServers.observe(cluster_size)
        if server is None:
            self.sim.mBlocked.observe(1)
            self.sim.mLostServiceTimes.observe(stime)
        else:
            self.sim.mBlocked.observe(0)
            self.sim.mAcceptServiceTimes.observe(stime)
            request_server_time = self.sim.now()
            yield request, self, server
            if self.sim.now() != request_server_time:
                raise RuntimeError('Requesting server should take 0 seconds')
            active_users[self.id] = self.sim.now()
            yield hold, self, stime
            del active_users[self.id]
            yield release, self, server
        if not loss_assumption:
            yield release, self, cluster.cluster_resource
