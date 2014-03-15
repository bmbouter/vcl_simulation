import math

from tools import MonitorStatistics

class BillablePolicy(object):
    """BillablePolicy is designed to handle all observing required to calculate all billable time metrics for a simulation.

    This policy does a strict summarization of all billable items

    """

    def __init__(self, sim):
        self.sim = sim

    def run(self):
        """Returns a dict containing 'billable_time', 'lost_billable_time', 'server_cost_time', and 'total_costs'"""
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


class HourMinimumBillablePolicy(BillablePolicy):
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
