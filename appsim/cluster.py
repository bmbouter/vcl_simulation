"""

An M/M/c/c queue where c is dynamically changing

Users arrive at random into a c-server system with
exponential service-time distribution. Simulate to
determine the average blocking probability. 

"""
from SimPy.Simulation import Resource, PriorityQ

class Cluster(object):

    """Logically represents an application cluster to be scale

    Constructor variables:
    sim -- the simulation object this cluster will be part of
    density -- integer representing the maximum number of concurrent
               applications per VM (default 1)
    initial_capacity -- integer representing the number of virtual
               machines in the active state at simulation time 0

    Instance variables:
    self.booting -- a list of Resource objects in the 'booting' state
    self.active -- a list of Resource objects in the 'active' state
    self.shutting_down -- a list of Resource objects shutting_down
    self.density -- The given density of app per VM (default 1)

    """

    total_prov = 0
    total_deleted = 0
    def __init__(self, sim, density=1, initial_capacity=0):
        self.booting = []
        self.active = []
        self.shutting_down = []
        self.density = density
        self.sim = sim
        for server in range(0,initial_capacity):
            self.active.append(self.create_VM())
            Cluster.total_prov += 1

    def __str__(self):
        """Prints a brief, one-liner about the Cluster

        cluster

        """
        return 'Cluster(booting=%d, active=%d, shutting_down=%d)' % \
             (len(self.booting), len(self.active), len(self.shutting_down))

    def create_VM(self):
        """Create a Resource of size self.density and return it

        The Resource created will be assigned the appropriate _next_priority

        """
        r = Resource(capacity=self.density, name='',sim=self.sim, qType=PriorityQ)
        r.rank = self._next_priority()
        #raw_input('creating VM with priority %s' % self._next_priority())
        return r

    def find_server(self):
        """Find a server according to the waterfall placement algorithm

        Finds an available seat among the active VMs according to the
        waterfall placement algorithm as follows. Given the existing
        priorities of servers in the cluster, find the lowest priority
        server that is not at full capacity.

        If a seat can not be found, for any reason, None is returned.
        This includes cases where there are no VMs, or if all VMs are
        currently at capacity

        """
        for VM in sorted(self.active, key=lambda r: r.rank):
            if VM.n > 0:
                return VM
        return None

    @property
    def capacity(self):
        """Implements the following read-only attribute

        capacity -- the total application seat capacity of this cluster

        NOTE: VMs in the shutting-down state are ignored

        """
        return (len(self.booting) + len(self.active)) * self.density

    @property
    def n(self):
        """Implements the following read-only attribute

        n -- the number of uncommited application seats for this cluster

        NOTE: VMs in the shutting-down state are ignored

        """
        return reduce(lambda x, y: x + y.n, self.booting + self.active, 0)

    def _next_priority(self):
        """Inspect existing nodes to determine the next priority

        Priority values start from zero and increase sequentially
        For example:  0,1,2,3...

        The use of negative priority values will have undefined results

        """
        in_order = sorted(self.booting + self.active + self.shutting_down, 
            key=lambda r: r.rank)
        for i in range(len(in_order)):
            if i != in_order[i].rank:
                return i
        return len(in_order)
