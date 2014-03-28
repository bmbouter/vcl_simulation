import unittest
import os

from appsim.sim import FixedSizePolicyFixedPoissonSim, DataFilePolicyDataFileUserSim

class main(unittest.TestCase):
    def fixed_size_policy_fixed_poisson_sim(self):
        fixed = FixedSizePolicyFixedPoissonSim()
        #results = fixed.run(2, 3, 1, 5, 1, 0, 0, 50000)
        #results = fixed.run(1, 6, 1, 5, 1, 0, 0, 50000)
        results = fixed.run(3, 2, 5, 5, 2, 0, 0, 50000)
        #fixed.run(num_vms_in_cluster, density, scale_rate, lamda, mu, startup_delay, shutdown_delay, num_customers)
        print results
        self.assertEqual(results['bp'], 0.026654259718775844)
        self.assertEqual(results['utilization'], 0.401729062821548)
        self.assertEqual(results['bp_percent_error'], 0.08559767913065895)

    def test_scheduled_sim_test_1(self):
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/fixed_lambda_mu_user_schedule.csv'
        results = scheduled.run(prov_data_file_path, users_data_file_path, 6, 0, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay, shutdown_delay)
        print results
        self.assertEqual(results['bp'], 0.02857999049331432)
        self.assertEqual(results['utilization'], 0.40565218479169746)
        self.assertEqual(results['bp_percent_error'], 0.017168472597705486)


if __name__ == "__main__":
    if not os.path.isfile('fixed_capacity.py'):
        print 'Please run in the same directory as fixed_capacity.py'
        exit()
    unittest.main()
