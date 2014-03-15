import unittest
import pprint

from appsim.sim import DataFilePolicyDataFileUserSim, ErlangDataPolicyDataFileUserSim

reserved = 2
density = 4
scale_rate = 1
lamda = 5
mu = 1
startup_delay = 0
shutdown_delay = 0
num_customers = 50000

#class main(unittest.TestCase):
class main(object):
    def scheduled_sim_test_2(self):
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = '/home/bmbouter/simulations/rewrite/data/5_server_prov_schedule.csv'
        users_data_file_path = '/home/bmbouter/simulations/rewrite/data/fixed_lambda_mu_user_schedule.csv'
        results = scheduled.run(prov_data_file_path, users_data_file_path, 6, 0, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay, shutdown_delay)
        print results
        self.assertEqual(results['bp'], 1.0)
        self.assertEqual(results['bp_percent_error'], 0.0)

    def second_half_vcl_data_scheduled_sim(self):
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = '/home/bmbouter/simulations/rewrite/data/20_server_prov_schedule.csv'
        users_data_file_path = '/home/bmbouter/simulations/rewrite/data/second_half_year_arrivals.txt'
        first_results = scheduled.run(prov_data_file_path, users_data_file_path, 5, 0, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay, shutdown_delay)
        print first_results
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = '/home/bmbouter/simulations/rewrite/data/1_server_prov_schedule.csv'
        users_data_file_path = '/home/bmbouter/simulations/rewrite/data/second_half_year_arrivals.txt'
        second_results = scheduled.run(prov_data_file_path, users_data_file_path, 100, 0, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay, shutdown_delay)
        print second_results
        self.assertEqual(first_results['bp'], second_results['bp'])
        self.assertEqual(first_results['bp_percent_error'], second_results['bp_percent_error'])


    def second_half_vcl_data_predicted_from_first_half(self):
        scheduled = DataFilePolicyDataFileUserSim()
        #prov_data_file_path = '/home/bmbouter/simulations/rewrite/data/first_half_year_provisioning_schedule.csv'
        prov_data_file_path = '/home/bmbouter/simulations/rewrite/data/2008_optimal_provisioning_schedule.csv'
        users_data_file_path = '/home/bmbouter/simulations/rewrite/data/2009_year_arrivals.txt'
        results = scheduled.run(prov_data_file_path, users_data_file_path, 2, 0, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay, shutdown_delay)
        print results
        self.assertEqual(results['bp'], 1.0)
        self.assertEqual(results['bp_percent_error'], 0.0)

    def fixed_policy_user_arrivals(self):
        param_name = 'seats'
        self.print_results_header(param_name)
        for capacity in range(2,201,2):
            scheduled = DataFilePolicyDataFileUserSim()
            prov_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/1_server_prov_schedule.csv'
            users_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_year_arrivals.txt'
            results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, 0, 0)
            #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay, shutdown_delay)
            results['param'] = capacity
            results['param_name'] = param_name
            self.write_bp_timescale_raw_to_file('fixed', results)
            #self.print_simple_results(results)
            self.print_all_results(results)

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

    def write_bp_timescale_raw_to_file(self, model_name, results):
        base_path = "/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/bp_timescale_raw"
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

    def poisson_traffic_known_in_advance(self):
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_five_minute_counts.csv'
        users_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 0
        scale_rate = 300
        startup_delay = 0
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay):
        print results

    def moving_average(self):
        param_name = 'k'
        self.print_results_header(param_name)
        for k in range(1,51):
            scheduled = ErlangDataPolicyDataFileUserSim()
            pred_user_count_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/ma_arrivals/arrivals_k_%s.txt' % k
            users_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_year_arrivals.txt'
            worst_bp = 0.01
            mu = 1 / 4957.567
            density = 2
            lag = 1
            scale_rate = 300
            startup_delay = 300
            shutdown_delay = 300
            results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay)
            # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay):
            results['param'] = k
            results['param_name'] = param_name
            self.write_bp_timescale_raw_to_file('ma', results)
            #self.print_simple_results(results)
            self.print_all_results(results)

    def exponential_moving_average(self):
        alpha_values = ['0.01', '0.02', '0.03', '0.04', '0.05', '0.06', '0.07', '0.08', '0.09', '0.1', '0.11', '0.12', '0.13', '0.14', '0.15', '0.16', '0.17', '0.18', '0.19', '0.2', '0.21', '0.22', '0.23', '0.24', '0.25', '0.26', '0.27', '0.28', '0.29', '0.3', '0.31', '0.32', '0.33', '0.34', '0.35', '0.36', '0.37', '0.38', '0.39', '0.4', '0.41', '0.42', '0.43', '0.44', '0.45', '0.46', '0.47', '0.48', '0.49', '0.5', '0.51', '0.52', '0.53', '0.54', '0.55', '0.56', '0.57', '0.58', '0.59', '0.6', '0.61', '0.62', '0.63', '0.64', '0.65', '0.66', '0.67', '0.68', '0.69', '0.7', '0.71', '0.72', '0.73', '0.74', '0.75', '0.76', '0.77', '0.78', '0.79', '0.8', '0.81', '0.82', '0.83', '0.84', '0.85', '0.86', '0.87', '0.88', '0.89', '0.9', '0.91', '0.92', '0.93', '0.94', '0.95', '0.96', '0.97', '0.98', '0.99', '1']
        param_name = 'alpha'
        self.print_results_header(param_name)
        for alpha in alpha_values:
            scheduled = ErlangDataPolicyDataFileUserSim()
            pred_user_count_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/ema_arrivals/arrivals_alpha_%s.txt' % alpha
            users_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_year_arrivals.txt'
            worst_bp = 0.01
            mu = 1 / 4957.567
            density = 2
            lag = 1
            scale_rate = 300
            startup_delay = 300
            shutdown_delay = 300
            results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay)
            # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay):
            results['param'] = alpha
            results['param_name'] = param_name
            self.write_bp_timescale_raw_to_file('ema', results)
            #self.print_simple_results(results)
            self.print_all_results(results)

    def fixed_policy_density_analysis(self):
        param_name = 'seats'
        #self.print_results_header(param_name)
        for capacity in [60, 120, 180]:
            for density in [1, 2, 3, 4, 5]:
                num_servers = capacity / density
                scheduled = DataFilePolicyDataFileUserSim()
                prov_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/%s_server_prov_schedule.csv' % num_servers
                users_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_year_arrivals.txt'
                results = scheduled.run(prov_data_file_path, users_data_file_path, density, 0, 0)
                #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay, shutdown_delay)
                results['param'] = capacity
                results['param_name'] = param_name
                results['density'] = density
                self.print_simple_results(results)
                #self.print_all_results(results)


    def moving_average_policy_density_analysis(self):
        param_name = 'k'
        #self.print_results_header(param_name)
        for k in [1, 3, 10, 30, 50]:
            for density in [1, 2, 3, 4, 5]:
                scheduled = ErlangDataPolicyDataFileUserSim()
                pred_user_count_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/ma_arrivals/arrivals_k_%s.txt' % k
                users_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_year_arrivals.txt'
                worst_bp = 0.01
                mu = 1 / 4957.567
                lag = 1
                scale_rate = 300
                startup_delay = 300
                shutdown_delay = 300
                results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay)
                # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay):
                results['param'] = k
                results['param_name'] = param_name
                results['density'] = density
                self.print_simple_results(results)
                #self.print_all_results(results)

    def exponential_moving_average_policy_density_analysis(self):
        alpha_values = ['0.01', '0.05', '0.1', '0.3', '0.7', '0.9', '1']
        param_name = 'alpha'
        #self.print_results_header(param_name)
        for alpha in alpha_values:
            for density in [1, 2, 3, 4, 5]:
                scheduled = ErlangDataPolicyDataFileUserSim()
                pred_user_count_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/ema_arrivals/arrivals_alpha_%s.txt' % alpha
                users_data_file_path = '/Users/katie/Documents/octave_unzip/home/bmbouter/simulations/rewrite/data/2008_year_arrivals.txt'
                worst_bp = 0.01
                mu = 1 / 4957.567
                lag = 1
                scale_rate = 300
                startup_delay = 300
                shutdown_delay = 300
                results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay)
                # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay, shutdown_delay):
                results['param'] = alpha
                results['param_name'] = param_name
                results['density'] = density
                self.print_simple_results(results)
                #self.print_all_results(results)


if __name__ == "__main__":
    #unittest.main()
    #main().fixed_policy_user_arrivals()
    #main().moving_average()
    main().exponential_moving_average()
    #main().fixed_policy_density_analysis()
    #main().moving_average_policy_density_analysis()
    #main().exponential_moving_average_policy_density_analysis()
