from scaler import Scale

class GenericDataFileScaler(Scale):

    """Wake up periodically and Scale the cluster

    This scaler policy attempts to provision based on a schedule defined in a
    data file.  This object will loop the scaling events in a period whose
    length is specified in the constructor.

    """

    def __init__(self, sim, scale_rate, startup_delay,
            shutdown_delay, data_path, period_length):
        """Initializes a GenericDataFileScaler object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        scale_rate -- The interarrival time between scale events in seconds
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        data_path -- the full path to the data file
        period_length -- the period length of the data file

        """

        self.data_path = data_path
        self.period_length = period_length
        Scale.__init__(self, sim=sim,
                             scale_rate=scale_rate,
                             startup_delay=startup_delay,
                             shutdown_delay=shutdown_delay)

    def scaling_complete(self, stopped_count):
        """Records the number of stopped servers

        """
        self.unprovisioned = self.servers_to_stop - stopped_count

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        """
        dataReader = csv.reader(open(self.data_path, 'rb'), delimiter=',')
        for row in dataReader:
            interarrival_time = float(row[0])
            service_time = float(row[1])
            yield hold,self,interarrival_time
            L = User("User %s" % user_num, sim=self.sim)
            self.sim.activate(L, L.execute(service_time, self.sim.cluster), delay=0)
            user_num = user_num + 1
        self.servers_to_stop = servers_to_stop
        return servers_to_start, servers_to_stop
