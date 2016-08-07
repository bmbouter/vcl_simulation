from collections import namedtuple
from Queue import Empty

from SimPy.Simulation import Process, hold

from appsim.cluster import Cluster
from appsim.user import User, active_users


class NotImplementedException(Exception):
    pass


State = namedtuple('State', ['n', 's', 'q'])


class Scale(Process):

    """Wake up periodically and Scale the cluster

    Scale monitors the number of customers
    in the system and releases/requests Application
    capacity from the application pool
    """

    def __init__(self, sim, scale_rate, startup_delay_func, shutdown_delay):
        """Initializes a Scale object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay_func -- A callable that returns the time a server spends
            in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state

        """

        self.sim = sim
        self.scale_rate = scale_rate
        self.n = int(300.0 / scale_rate)
        self.startup_delay_func = startup_delay_func
        self.shutdown_delay = shutdown_delay
        Process.__init__(self, name='Scaler Function', sim=self.sim)

    def execute(self):
        """Execute the scaler function. This handles all actions common to
        all scalers. This in turn calls out to scale_logic which provides
        the logic through subclassing.

        """
        self.sim.user_generator.reset_scale_counters()
        while True:
            # Put the 'ready' VMs into service
            not_to_be_shut_off_list = []

            servers_to_start, servers_to_stop = self.scaler_logic()

            self.sim.user_generator.reset_scale_counters()

            if not isinstance(servers_to_start, int):
                raise RuntimeError('Expected servers_to_start to be an int')
            if not isinstance(servers_to_stop, int):
                raise RuntimeError('Expected servers_to_stop to be an int')
            if servers_to_start != 0 and servers_to_stop != 0:
                raise RuntimeError('Starting and Stopping servers does not make sense')

            stopped_count = 0

            for server in range(servers_to_start):
                if self.sim.cluster.shutting_down:
                    new_vm = self.sim.cluster.shutting_down.pop()
                    not_to_be_shut_off_list.append(new_vm)
                    #raw_input('removing server %d from shutting down state and putting back in ACTIVE' % new_vm.rank)
                    self.sim.cluster.active.append(new_vm)
                else:
                    new_vm = self.sim.cluster.create_VM()
                    new_vm.ready_time = self.sim.now() + self.startup_delay_func()
                    new_vm.start_time = self.sim.now()
                    self.sim.cluster.booting.append(new_vm)
                    Cluster.total_prov += 1
                    #raw_input('server %s booting. It will be READY AT %s' % (new_vm.rank, new_vm.ready_time))

            ready_to_be_active = [elem for elem in self.sim.cluster.booting if self.sim.now() >= elem.ready_time]

            for s in ready_to_be_active:
                #raw_input('Server %d is now active' % s.rank)
                self.sim.cluster.booting.remove(s)
                self.sim.cluster.active.append(s)
                not_to_be_shut_off_list.append(s)

            # Look for VMs to shut off
            for server in self.sim.cluster.active:
                if server.activeQ == [] and server not in not_to_be_shut_off_list and servers_to_stop >= 1:
                    stopped_count = stopped_count + 1
                    servers_to_stop = servers_to_stop - 1
                    server.power_off_time = self.sim.now() + self.shutdown_delay
                    #raw_input('Server %d is empty, shutting it down. It will be powered off at %d' % (server.rank, server.power_off_time))
                    self.sim.cluster.active.remove(server)
                    self.sim.cluster.shutting_down.append(server)

            self.scaling_complete(stopped_count)

            # Delete shutting_down VMs 
            for server in self.sim.cluster.shutting_down:
                if self.sim.now() >= server.power_off_time:
                    #raw_input('DELETING server %d' % server.rank)
                    server_deployed_time = self.sim.now() - server.start_time
                    self.sim.mServerProvisionLength.observe(server_deployed_time, t=server.start_time)  # monitor servers provision length and start_time
                    Cluster.total_deleted += 1
                    self.sim.cluster.shutting_down.remove(server)

            self.sim.mClusterActive.observe(len(self.sim.cluster.active))  # monitor self.sim.cluster.active
            self.sim.mClusterBooting.observe(len(self.sim.cluster.booting))  # monitor self.sim.cluster.booting
            self.sim.mClusterShuttingDown.observe(len(self.sim.cluster.shutting_down))  # monitor self.sim.cluster.shutting_down
            self.sim.mClusterOccupancy.observe(self.sim.cluster.capacity - self.sim.cluster.n)  # monitor the number of customers at this scale event
            self.sim.mClusterQueueDepth.observe(User.waiting.qsize())  # monitor the number of customers waiting in the Queue for service

            # Drain the User.waiting queue into all newly available seats
            try:
                open_seats = reduce(lambda x, y: x + y.n, self.sim.cluster.active, 0)
                #print 'open_seats = %d' % open_seats
                for i in range(open_seats):
                    next_user = User.waiting.get_nowait()
                    #print 'Scaling event reactivating User %d at %s' % (next_user.id, self.sim.now())
                    self.sim.reactivate(next_user, delay=0.009) # delay so these users occurs just before new arrivals
            except Empty:
                pass

            # Wait an amount of time to allow the scale function to run periodically
            yield hold, self, self.sleep()

    def sleep(self):
        """Returns the amount of simulation time simpy should sleep for

        The first scale event should occur < 1 second early to ensure that
        arrivals and scale events that occur at the same second are not
        handled in arbitrary order
        """
        if self.sim.now() == 0:
            return self.scale_rate - 0.01
        return self.scale_rate

    def scaling_complete(self, stopped_count):
        """A callback function which is called after scaling is complete with
        the number of servers stopped.

        """
        total_system_users = User.waiting.qsize() + len(active_users)
        active_servers = len(self.sim.cluster.active)
        shutting_down_servers = len(self.sim.cluster.shutting_down)
        current_state = State(n=total_system_users, s=active_servers, q=shutting_down_servers)
        self.sim.mSystemState.observe(current_state)

    def scaler_logic(self):
        """This is where scaler_logic not common to all scalers is stored. This
        is designed to be implemented by any subclasses, and raises a
        NotImplementedException here.

        """
        raise NotImplementedException()
