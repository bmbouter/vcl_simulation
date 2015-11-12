from Queue import Queue, Empty

from SimPy.SimulationTrace import * ## Only change in program!

from appsim.feature import FeatureFlipper


active_users = {}


class User(Process):

    """
    Users request an application and hold it for an exponential time

    Class variables:
    NoInSystem -- The total number of users which have been generated
    NoAccepted -- The number of users who received service
    NoDenied -- The number of users who were denied service
    """

    prev_id = -1
    user_count = 0
    waiting = Queue()

    def execute(self, stime, cluster):
        """Simulate a single user

        Parameters:
        stime -- the service time the user should use service for
        cluster -- the cluster this user is to arrive at
        """
        global active_users
        self.id = User.user_count
        #print 'User %d arrived at %s' % (User.user_count, self.sim.now())
        User.user_count = User.user_count + 1
        self.arrival_time = self.sim.now()
        server = cluster.find_server()
        if not FeatureFlipper.loss_assumption() and server is None:
            User.waiting.put(self)
            yield passivate, self
            server = cluster.find_server()
            #print 'User %d reactivated at %s' % (self.id, self.sim.now())
            if server is None:
                print RuntimeError('This customer was reactivated but no server is available. Something is wrong.')
        if self.id <= User.prev_id:
            print RuntimeError('Users are not arriving according to a FCFS policy')
        User.prev_id = self.id
        self.wait_time = self.sim.now() - self.arrival_time
        if not FeatureFlipper.loss_assumption() and FeatureFlipper.add_capacity_for_waiting_users() and \
                        self.wait_time >= 600:
            #import pydevd
            #pydevd.settrace('localhost', port=9854, stdoutToServer=True, stderrToServer=True)
            print RuntimeError('Wait time should never exceed 600 seconds')
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
            if not FeatureFlipper.loss_assumption():
                try:
                    next_user = User.waiting.get_nowait()
                except Empty:
                    pass
                else:
                    #print 'User %d left at %s, reactivating User %d' % (self.id, self.sim.now(), next_user.id)
                    self.sim.reactivate(next_user)
