import math

from scaler import Scale

def ErlangBLossEquation(offered_traffic_in_erlangs, num_servers):
    """Computes the Erlang B Blocking Probability from the closed form Erlang B
    loss equation"""

    numerator = (math.pow(offered_traffic_in_erlangs, num_servers)) / math.factorial(num_servers)
    denominator = 0
    for k in range(0, num_servers + 1):
	denominator = denominator + math.pow(offered_traffic_in_erlangs, k) / math.factorial(k)

    print 'A = %s' % offered_traffic_in_erlangs
    print 'm = %s\n' % num_servers

    return numerator / denominator


class ErlangBFormulaPolicy(Scale):

    """Wake up periodically and Scale the cluster

    This scaler policy attempts to provision based on blocking probability
    predicted from the closed-form erlang B loss equation for an M/M/C queue.
    A threshold is used to request and release server resources from the
    cluster.

    This policy is designed for steady state and does not consider the current
    number of customers in the system.  This policy sets
    self.num_vms_in_cluster to the smallest value that permits the Erlang-B
    predicted blocking probability to be less than the threshold value
    'worst_bp'.  This is set the first time the scaler is called, and no
    further requests are made to modify the cluster size afterwards.

    """

    def __init__(self, sim, scale_rate, startup_delay,
            shutdown_delay, worst_bp, lamda, mu):
        """Initializes a ErlangBFormulaPolicy object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
	worst_bp -- ensure the blocking probability does exceed this value as
            computed by the closed form Erlang B formula.
        lamda -- the parameter to a Poisson distribution (in seconds)
            which defines the arrival process
        mu -- the parameter to a Poisson distribution (in seconds)
            which defines the service time process

        """

        self.worst_bp = worst_bp
        self.lamda = lamda
        self.mu = mu
        self.offered_traffic_in_erlangs = float(self.lamda) / self.mu
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
            servers_to_start = -1
            calculated_bp = 1.0
            while (calculated_bp >= self.worst_bp):
                servers_to_start = servers_to_start + 1
                possible_capacity = servers_to_start * self.sim.cluster.density
                calculated_bp = ErlangBLossEquation(self.offered_traffic_in_erlangs, possible_capacity)
            self.scaler_logic_called = True
        else:
            servers_to_start = 0
        return servers_to_start, servers_to_stop
