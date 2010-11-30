import math

from tools import MonitorStatistics

class AbstractCostPolicy(object):
    """An AbstractCostPolicy designed to be the foundation for specific Cost Calculators"""

    def __init__(self, sim, cost_per_server_per_server_cost_period, profit_per_customer_per_customer_bill_period):
        self.sim = sim
        self.cost_per_server_per_server_cost_period = cost_per_server_per_server_cost_period
        self.profit_per_customer_per_customer_bill_period = profit_per_customer_per_customer_bill_period

    def _observe_active_server_cost(self):
        """Observes the cost of servers that are still running assuming the stop now"""
        for server in self.sim.cluster.active:
            self.sim.mServerProvisionLength.observe(self.sim.now() - server.start_time)
        for server in self.sim.cluster.booting:
            self.sim.mServerProvisionLength.observe(self.sim.now() - server.start_time)
        for server in self.sim.cluster.shutting_down:
            self.sim.mServerProvisionLength.observe(self.sim.now() - server.start_time)

    def _observe_existing_customers(self):
        """Observes the profits of customers that are still running assuming they leave now"""
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

class HourMinimumCostPolicy(AbstractCostPolicy):
    """A policy that charges a minimum of 1 hour for both servers and customers"""

    def run(self):
        """Returns a dict containing 'profits', 'lost_profits', 'server_costs', and 'total_costs'"""
        self._observe_active_server_cost()
        self._observe_existing_customers()
        self._adjust_mLostServiceTimes()
        profits = self.compute_profits()
        lost_profits = self.compute_lost_profits()
        server_costs = self.compute_server_costs()
	return {'profits': profits, 'lost_profits': lost_profits,
                'server_costs': server_costs, 
                'total_costs': lost_profits + server_costs}

    def compute_profits(self):
        """Returns the amount of profits based on the profit per customer per cost period"""
        return sum([math.ceil(item[1]) for item in self.sim.msT]) * self.profit_per_customer_per_customer_bill_period

    def compute_lost_profits(self):
        """Returns the amount of lost profits based on the profit per customer per cost period"""
        return sum([math.ceil(item[1]) for item in self.sim.mLostServiceTimes]) * self.profit_per_customer_per_customer_bill_period

    def compute_server_costs(self):
        """Returns the cost of all servers operated for the following"""
        server_hours = sum([math.ceil(item[0]) for item in self.sim.mServerProvisionLength])
        return server_hours * self.cost_per_server_per_server_cost_period
