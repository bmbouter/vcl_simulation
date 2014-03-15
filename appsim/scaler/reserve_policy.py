import math

from scaler import Scale

class ReservePolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler uses a reserve policy to request and release server
    resources from the cluster.

    """

    def __init__(self, sim, scale_rate, startup_delay,
            shutdown_delay, reserved):
        """Initializes a ReservePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        reserved -- The number of servers to have available at the end of a
            the scale operation

        """

        self.reserved = reserved
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay=startup_delay,
                             shutdown_delay=shutdown_delay)

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        This policy scales the cluster such that the blocking
        probability is managed. The scaler uses a heuristic based
        reserve capacity approach whereby a reserve capacity, R, is
        specified. Reserve capacity, R,  is specified in terms of
        application seats. The scaler powers on and off servers such
        that the adjusted cluster size, given the same number of active
        users as before will have R additional, unused application
        seats.

        """
        diff = self.sim.cluster.n - self.reserved
        if diff > 0:
            servers_to_stop = diff / self.sim.cluster.density
        else:
            servers_to_stop = 0
        if diff < 0:
            servers_to_start = int(math.ceil(abs(float(diff) / self.sim.cluster.density)))
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop
