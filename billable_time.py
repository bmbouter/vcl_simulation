import math

from tools import MonitorStatistics

class NoMinimumBillablePolicy(object):
    """NoMinimumBillablePolicy is designed to handle all observing required to calculate all billable time metrics for a simulation.

    This policy does a strict summarization of all billable items

    """

    def __init__(self, sim):
        self.sim = sim

    def run(self):
        """Returns a dict containing 'billable_time', 'lost_billable_time', 'server_cost_time', and 'total_costs'"""
        self._observe_active_servers_billable_time()
        self._observe_existing_customers()
        self._adjust_mLostServiceTimes()
        billable_time = self.compute_billable_time()
        lost_billable_time = self.compute_lost_billable_time()
        server_cost_time = self.compute_server_cost_time()
	return {'billable_time': billable_time, 'lost_billable_time': lost_billable_time,
                'server_cost_time': server_cost_time}

    def compute_billable_time(self):
        """Returns the amount of billable time"""
        return sum([item[1] for item in self.sim.msT])

    def compute_lost_billable_time(self):
        """Returns the amount of lost billable time"""
        return sum([item[1] for item in self.sim.mLostServiceTimes])

    def compute_server_cost_time(self):
        """Returns the cost of all servers operated for the following"""
        returnsum([item[0] for item in self.sim.mServerProvisionLength])

    def _observe_active_servers_billable_time(self):
        """Observes the billable time of servers that are still running assuming the stop now"""
        for server in self.sim.cluster.active:
            self.sim.mServerProvisionLength.observe(self.sim.now() - server.start_time)
        for server in self.sim.cluster.booting:
            self.sim.mServerProvisionLength.observe(self.sim.now() - server.start_time)
        for server in self.sim.cluster.shutting_down:
            self.sim.mServerProvisionLength.observe(self.sim.now() - server.start_time)

    def _observe_existing_customers(self):
        """Observes the billable time of customers that are still running assuming they leave now"""
        for active in self.sim.cluster.active:
            for user in active.activeQ:
                self.sim.msT.observe(self.sim.now() - user.arrival_time)

    def _adjust_mLostServiceTimes(self):
        """Removes from mLostServiceTimes any service times that go further into the future than the simulation did"""
        current_sim_time = self.sim.now()
        for i, item in enumerate(self.sim.mLostServiceTimes):
            if sum(item) > current_sim_time:
                new_service_time = current_sim_time - item[0]
                self.sim.mLostServiceTimes[i][1] = new_service_time

class HourMinimumBillablePolicy(NoMinimumBillablePolicy):
    """A policy that rounds up to the next integer when reporting both server and customer billable time"""

    def compute_billable_time(self):
        """Returns the amount of billable time"""
        return sum([math.ceil(item[1]) for item in self.sim.msT])

    def compute_lost_billable_time(self):
        """Returns the amount of lost billable time"""
        return sum([math.ceil(item[1]) for item in self.sim.mLostServiceTimes])

    def compute_server_cost_time(self):
        """Returns the cost of all servers operated for the following"""
        return sum([math.ceil(item[0]) for item in self.sim.mServerProvisionLength])
