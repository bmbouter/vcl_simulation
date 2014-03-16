import unittest

from appsim.sim import ReservePolicyFixedPoissonSim, ReservePolicyDataFileUserSim

reserved = 2
scale_rate = 1
lamda = 5
mu = 1
startup_delay = 0
shutdown_delay = 0
num_customers = 50000

class main(object):
    def reserve_policy_fixed_poisson_sim_test_1(self):
        reserve = ReservePolicyFixedPoissonSim()
        results = reserve.run(1, 1, 5, 10, 1, 0, 0, 50000)
        #results = reserve.run(reserved, density, scale_rate, lamda, mu, startup_delay, shutdown_delay, num_customers)
        print results
        self.assertEqual(results['bp'], 0.54348635235732012)
        self.assertEqual(results['bp_percent_error'], 0.034711646651472049)

    def reserve_policy_fixed_poisson_sim_test_2(self):
        reserve = ReservePolicyFixedPoissonSim()
        results = reserve.run(2, 1, 5, 10, 1, 0, 0, 50000)
        #results = reserve.run(reserved, density, scale_rate, lamda, mu, startup_delay, shutdown_delay, num_customers)
        print results
        self.assertEqual(results['bp'], 0.34009511993382963)
        self.assertEqual(results['bp_percent_error'], 0.043191989412468088)

    def reserve_policy_data_file_user_sim(self):
        reserve = ReservePolicyDataFileUserSim()
        users_data_file_path = 'data/2008_year_arrivals.txt'
        results = reserve.run(5, users_data_file_path, 2, 300, 300, 300)
        #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay, shutdown_delay):
        print results

    def reserve_policy_data_file_user_sim_table(self):
        param_name = 'R'
        self.print_results_header(param_name)
        for R in range(1,41):
            reserve = ReservePolicyDataFileUserSim()
            users_data_file_path = 'data/2008_year_arrivals.txt'
            results = reserve.run(R, users_data_file_path, 2, 300, 300, 300)
            #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay, shutdown_delay):
            results['param'] = R
            results['param_name'] = param_name
            self.write_bp_timescale_raw_to_file('reserve', results)
            #self.print_simple_results(results)
            self.print_all_results(results)

    def write_bp_timescale_raw_to_file(self, model_name, results):
        base_path = "data/bp_timescale_raw"
        filename_template = "%s_%s_%s.txt"
        bp_timescale_raw = results['bp_timescale_raw']
        param_name = results['param_name']
        param = results['param']
        for timescale in bp_timescale_raw.keys():
            filename = base_path + '/' + model_name + '/' + filename_template % (timescale, param_name, param)
            f = open(filename, 'w')
            for observation in bp_timescale_raw[timescale]:
                f.write('%s\n' % observation)
            f.close()

    def reserve_policy_data_file_user_sim_density_analysis(self):
        param_name = 'R'
        #self.print_results_header(param_name)
        for R in [5, 10, 15, 20]:
            for density in [1, 2, 3, 4, 5]:
                reserve = ReservePolicyDataFileUserSim()
                users_data_file_path = 'data/2008_year_arrivals.txt'
                results = reserve.run(R, users_data_file_path, density, 300, 300, 300)
                #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay, shutdown_delay):
                results['param'] = R
                results['param_name'] = param_name
                results['density'] = density
                self.print_simple_results(results)
                #self.print_all_results(results)

    def print_results_header(self, param_name):
        print "%s,bp_batch_mean,bp_batch_mean_delta,bp_batch_mean_percent_error,utilization,bp_by_hour_50,bp_by_hour_95,bp_by_hour_99,bp_by_hour_mean,bp_by_day_50,bp_by_day_95,bp_by_day_99,bp_by_day_mean,bp_by_week_50,bp_by_week_95,bp_by_week_99,bp_by_week_mean,bp_by_month_50,bp_by_month_95,bp_by_month_99,bp_by_month_mean,bp_by_year_50,bp_by_year_95,bp_by_year_99,bp_by_year_mean,billable_time,lost_billable_time,server_cost_time,num_servers,ns_delta" % param_name

    def print_all_results(self, results):
        r = results
        print("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (r['param'], r['bp_batch_mean'], r['bp_batch_mean_delta'], r['bp_batch_mean_percent_error'], r['utilization'], r['bp_by_hour']['bp_50percentile'], r['bp_by_hour']['bp_95percentile'], r['bp_by_hour']['bp_99percentile'], r['bp_by_hour']['bp_mean'], r['bp_by_day']['bp_50percentile'], r['bp_by_day']['bp_95percentile'], r['bp_by_day']['bp_99percentile'], r['bp_by_day']['bp_mean'], r['bp_by_week']['bp_50percentile'], r['bp_by_week']['bp_95percentile'], r['bp_by_week']['bp_99percentile'], r['bp_by_week']['bp_mean'], r['bp_by_month']['bp_50percentile'], r['bp_by_month']['bp_95percentile'], r['bp_by_month']['bp_99percentile'], r['bp_by_month']['bp_mean'], r['bp_by_year']['bp_50percentile'], r['bp_by_year']['bp_95percentile'], r['bp_by_year']['bp_99percentile'], r['bp_by_year']['bp_mean'], r['billable_time'], r['lost_billable_time'], r['server_cost_time'], r['num_servers'], r['ns_delta']))

    def print_simple_results(self, results):
        if 'density' in results:
            print('%s,%s,%.4f,%.4f,%.4f' % (results['param'], results['density'], results['bp_batch_mean'], results['bp_batch_mean_delta'], results['utilization']))
        else:
            print('%s,%.4f,%.4f,%.4f' % (results['param'], results['bp_batch_mean'], results['bp_batch_mean_delta'], results['utilization']))

if __name__ == "__main__":
    if not os.path.isfile('reserve_capacity.py'):
        print 'Please run in the same directory as reserve_capacity.py'
        exit()
    main().reserve_policy_data_file_user_sim_table()
    #main().reserve_policy_data_file_user_sim_density_analysis()
