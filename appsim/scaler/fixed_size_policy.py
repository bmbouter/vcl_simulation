from scaler import Scale

class FixedSizePolicy(Scale):

    """Wake up periodically and Scale the cluster

    This policy requests self.num_vms_in_cluster number of virtual machines,
    and makes no further requests to modify the cluster size after the initial
    request.

    """

    def __init__(self, sim, scale_rate, startup_delay,
            shutdown_delay, num_vms_in_cluster):
        """Initializes a FixedSizePolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        num_vms_in_cluster -- the integer number of virtual machines that
            should be started

        """

        self.num_vms_in_cluster = num_vms_in_cluster
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
            servers_to_start = self.num_vms_in_cluster
            self.scaler_logic_called = True
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop
