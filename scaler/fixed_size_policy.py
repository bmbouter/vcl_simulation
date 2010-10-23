from scaler import Scale

class FixedSizePolicy(Scale):

    """Wake up periodically and Scale the cluster

    This policy requests self.cluster_vms number of virtual machines, and
    makes no further requests to modify the cluster size.

    """

    def __init__(self, sim, scale_rate, startup_delay,
            shutdown_delay, cluster_vms):
        """Initializes a FixedSizePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
	cluster_vms -- the integer number of virtual machines that should be
            started

        """

        self.cluster_vms = cluster_vms
        self.scaler_logic_called = False
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay=startup_delay,
                             shutdown_delay=shutdown_delay)

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        """
        servers_to_stop = 0
        if self.scaler_logic_called is False:
            servers_to_start = self.cluster_vms
            self.scaler_logic_called = True
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop
