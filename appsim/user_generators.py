from random import expovariate
import csv

from SimPy.SimulationTrace import * ## Only change in program!

from user import User


class GenericGenerator(Process):

    def __init__(self, sim):
        """Creates a generic Poisson user generator

        sim -- the sim users should arrive at for service
        """

        self.sim = sim
        self.user_count_since_last_scale = None
        self.last_scale_time = None
        super(GenericGenerator, self).__init__(name='User Arrival Generator', sim=self.sim)

    def reset_scale_counters(self):
        """
        Updates the user count and last scale time.

        Resets the user count to 0 so that the counter can increment it as new users arrive. Also updates the
        last scale time.

        :return: None
        """
        self.user_count_since_last_scale = 0
        self.last_scale_time = self.sim.now()

    def get_time_since_last_scale_event(self):
        """
        Return the simulation time since the last reset_scale_counters() call. The result is expected to be >= 0 and a
        RuntimeError is raised if it is negative.

        :return: The simulation time since the last reset_scale_counters() event.
        :raises RuntimeError: If the time since the last reset_scale_counters() is negative.
        :rtype: float
        """
        time_since_last_scale_point = self.sim.now() - self.last_scale_time
        if time_since_last_scale_point < 0:
            raise RuntimeError('The time since the last scale event is negative')
        return time_since_last_scale_point

    def get_time_until_next_scale_event(self):
        """
        Return the simulation time until the next reset_scale_counters() call. The result is expected to be >= 0 and a
        RuntimeError is raised if it is negative.

        :return: The simulation time until the last reset_scale_counters() call.
        :raises RuntimeError: If the time until the next reset_scale_counters() call is negative.
        :rtype: float
        """
        time_until_next_scale_event = self.sim.scaler.scale_rate - self.get_time_since_last_scale_event()
        if time_until_next_scale_event < 0:
            raise RuntimeError('The time until the next scale event is negative')
        return time_until_next_scale_event


class PoissonGenerator(GenericGenerator):
    """Generates Users at a Poisson rate of lamda"""

    def __init__(self, sim, lamda, mu):
        """Creates the Poisson user generator

        sim -- the sim users should arrive at for service
        lamda -- the parameter to a Poisson distribution
            which defines the arrival process
        mu -- the parameter to a Poisson distribution
            which defines the service time process

        """
        self.lamda = lamda
        self.mu = mu
        super(PoissonGenerator, self).__init__(sim=sim)

    def execute(self):
        """Begins user generations to the application cluster"""
        i = 0
        while True:
            yield hold, self, expovariate(self.lamda)
            self.sim.arrivalMonitor.observe(1)
            i = i + 1
            L = User("User %s" % i, sim=self.sim)
            service_time = expovariate(self.mu)
            if service_time < 0:
                raise Exception('Service time should not be less than 0')
            self.user_count_since_last_scale = self.user_count_since_last_scale + 1
            self.sim.activate(L, L.execute(service_time, self.sim.cluster), delay=0)


class DataFileGenerator(GenericGenerator):
    """Generates Users according a Data file"""

    def __init__(self, sim, data_path):
        """Creates the Data driven  user generator

        sim -- the sim users should arrive at for service
        data_path -- the full path to the data file

        """
        self.data_path = data_path
        super(DataFileGenerator, self).__init__(sim=sim)

    def execute(self):
        """Begins user generations to the application cluster

        csv files should be comma separated in the form interarrival,service
        """
        dataReader = csv.reader(open(self.data_path, 'rb'), delimiter=',')
        user_num = 1
        for row in dataReader:
            interarrival_time = float(row[0])
            service_time = float(row[1])
            yield hold, self, interarrival_time
            self.sim.arrivalMonitor.observe(1)
            if service_time < 0:
                raise Exception('Service time should not be less than 0')
            L = User("User %s" % user_num, sim=self.sim)
            user_num = user_num + 1
            self.user_count_since_last_scale = self.user_count_since_last_scale + 1
            self.sim.activate(L, L.execute(service_time, self.sim.cluster), delay=0)
