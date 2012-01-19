import csv

from scaler import Scale

DEPROV_CHECK_RATE = 5

class GenericDataFileScaler(Scale):

    """Wake up periodically and Scale the cluster

    This scaler policy attempts to provision based on a schedule defined in a
    data file.  This object will loop the scaling events in a period whose
    length is specified in the constructor.

    """

    def __init__(self, sim, startup_delay,
            shutdown_delay, data_path):
        """Initializes a GenericDataFileScaler object

        parameters:
        sim -- The Simulation containing a cluster cluster object this scale
            function is managing
        startup_delay -- the time a server spends in the booting state
        shutdown_delay -- the time a server spends in the shutting_down state
        data_path -- the full path to the data file

        """

        self.data_path = data_path
        self.to_deprovision = 0
        self.prov_events = self.parse_provisioning_events()
        Scale.__init__(self, sim=sim,
                             scale_rate=0,
                             startup_delay=startup_delay,
                             shutdown_delay=shutdown_delay)

    def parse_provisioning_events(self):
	"""Parses the data file for provisioning and deprovisioning events"""
        events = []
        dataReader = csv.reader(open(self.data_path, 'rb'), delimiter=',')
        for row in dataReader:
            events.append((float(row[0]), 'P', ))
            events.append((float(row[1]), 'D', ))
        return sorted(events)

    def scaling_complete(self, stopped_count):
        """Records the number of stopped servers

        """
        self.to_deprovision = self.servers_to_stop - stopped_count
        if self.to_deprovision > 0:
            self.prov_events.append(('C', self.sim.now() + DEPROV_CHECK_RATE, ))
            sorted(self.prov_events)

    def sleep(self):
        """Overrides the sleep function to follow the data files sleep length"""
        return self.prov_events[0][0] - self.sim.now()

    def scaler_logic(self):
        """Implements the scaler logic specific to this scaler

        """
        servers_to_stop = 0
        servers_to_start = 0
        while (self.sim.now() == self.prov_events[0][0]):
            event = self.prov_events.pop(0)
            event_label = event[1]
            if event_label == 'P':
                servers_to_start = servers_to_start + 1
            elif event_label == 'D':
                servers_to_stop = servers_to_stop + 1
            if self.to_deprovision > 0:
                servers_to_stop = servers_to_stop  + self.to_deprovision
        self.servers_to_stop = servers_to_stop
        return servers_to_start, servers_to_stop
