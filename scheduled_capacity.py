import os
import sys

from appsim.sim import (DataFilePolicyDataFileUserSim,
                        ErlangDataPolicyDataFileUserSim)
from common import fixed_startup_delay, normal_distribution_startup_delay
from output_utils import (print_all_results, print_results_header,
                          print_simple_results)


zero_second_startup_delay = fixed_startup_delay(0)
three_hundred_second_startup_delay = fixed_startup_delay(300)


def write_bp_timescale_raw_to_file(model_name, results):
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


def second_half_vcl_data_scheduled_sim():
    scheduled = DataFilePolicyDataFileUserSim()
    prov_data_file_path = 'data/20_server_prov_schedule.csv'
    users_data_file_path = 'data/second_half_year_arrivals.txt'
    first_results = scheduled.run(prov_data_file_path, users_data_file_path, 5, zero_second_startup_delay, 0)
    #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
    print first_results
    scheduled = DataFilePolicyDataFileUserSim()
    prov_data_file_path = 'data/1_server_prov_schedule.csv'
    users_data_file_path = 'data/second_half_year_arrivals.txt'
    second_results = scheduled.run(prov_data_file_path, users_data_file_path, 100, zero_second_startup_delay, 0)
    #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
    print second_results


def second_half_vcl_data_predicted_from_first_half():
    scheduled = DataFilePolicyDataFileUserSim()
    #prov_data_file_path = 'data/first_half_year_provisioning_schedule.csv'
    prov_data_file_path = 'data/2008_optimal_provisioning_schedule.csv'
    users_data_file_path = 'data/2009_year_arrivals.txt'
    results = scheduled.run(prov_data_file_path, users_data_file_path, 2, zero_second_startup_delay, 0)
    #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
    print results


def poisson_traffic_known_in_advance():
    print_results_header('poisson_known_in_advance')
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/2008_five_minute_counts.csv'
    users_data_file_path = 'data/2008_year_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 0
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, zero_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'poisson_known_in_advance'
    print_simple_results(results)


def fixed_policy_user_arrivals():
    param_name = 'seats'
    print_results_header(param_name)
    for capacity in range(2,201,2):
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, zero_second_startup_delay, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
        results['param'] = capacity
        results['param_name'] = param_name
        #write_bp_timescale_raw_to_file('fixed', results)
        #print_simple_results(results)
        print_all_results(results)


def fixed_policy_density_analysis():
    param_name = 'seats'
    #print_results_header(param_name)
    for capacity in [60, 120, 180]:
        for density in [1, 2, 3, 4, 5]:
            num_servers = capacity / density
            scheduled = DataFilePolicyDataFileUserSim()
            prov_data_file_path = 'data/%s_server_prov_schedule.csv' % num_servers
            users_data_file_path = 'data/2008_year_arrivals.txt'
            results = scheduled.run(prov_data_file_path, users_data_file_path, density, zero_second_startup_delay, 0)
            #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
            results['param'] = capacity
            results['param_name'] = param_name
            results['density'] = density
            print_simple_results(results, timescale='weekly_99_percentile')
            #print_all_results(results)


def fixed_policy_segmented_busiest():
    param_name = 'seats'
    #print_results_header(param_name)
    for capacity in [168]:
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/day_cluster/2008_busiest_arrivals.txt'
        results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, zero_second_startup_delay, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
        results['param'] = capacity
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('fixed', results)
        print_simple_results(results)
        #print_all_results(results)


def fixed_policy_segmented_slowest():
    param_name = 'seats'
    #print_results_header(param_name)
    for capacity in [168]:
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/day_cluster/2008_slowest_arrivals.txt'
        results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, zero_second_startup_delay, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
        results['param'] = capacity
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('fixed', results)
        print_simple_results(results)
        #print_all_results(results)


def fixed_policy_segmented_mon_fri_semester():
    param_name = 'seats'
    #print_results_header(param_name)
    for capacity in [168]:
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/day_cluster/2008_mon_fri_semester_arrivals.txt'
        results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, zero_second_startup_delay, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
        results['param'] = capacity
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('fixed', results)
        print_simple_results(results)
        #print_all_results(results)


def fixed_policy_segmented_winter_spring_break():
    param_name = 'seats'
    #print_results_header(param_name)
    for capacity in [168]:
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/day_cluster/2008_winter_spring_break_arrivals.txt'
        results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, zero_second_startup_delay, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
        results['param'] = capacity
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('fixed', results)
        print_simple_results(results)
        #print_all_results(results)


def fixed_policy_not_segmented():
    param_name = 'seats'
    #print_results_header(param_name)
    for capacity in [168]:
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, zero_second_startup_delay, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
        results['param'] = capacity
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('fixed', results)
        print_simple_results(results)
        #print_all_results(results)


def moving_average():
    param_name = 'k'
    print_results_header(param_name)
    for k in range(1,51):
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = k
        results['param_name'] = param_name
        #write_bp_timescale_raw_to_file('ma', results)
        print_simple_results(results)
        #print_all_results(results)


def moving_average_policy_density_analysis():
    param_name = 'k'
    #print_results_header(param_name)
    for k in [1, 3, 10, 30, 50]:
        for density in [1, 2, 3, 4, 5]:
            scheduled = ErlangDataPolicyDataFileUserSim()
            pred_user_count_file_path = 'data/ma_arrivals/arrivals_k_%s.txt' % k
            users_data_file_path = 'data/2008_year_arrivals.txt'
            worst_bp = 0.01
            mu = 1 / 4957.567
            lag = 1
            scale_rate = 300
            shutdown_delay = 300
            results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
            # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
            results['param'] = k
            results['param_name'] = param_name
            results['density'] = density
            print_simple_results(results, timescale='weekly_99_percentile')
            #print_all_results(results)


def moving_average_policy_startup_delay_analysis():
    startup_delay_sigma_values = range(0, 401, 50)
    param_name = 'sigma'
    k = 1
    density = 2
    for sigma in startup_delay_sigma_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        startup_mean = 300
        startup_delay_func = normal_distribution_startup_delay(startup_mean, sigma)
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = sigma
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def moving_average_policy_shutdown_delay_analysis():
    shutdown_delay_values = range(0, 1001, 100)
    param_name = 'shutdown_delay'
    k = 1
    density = 2
    for shutdown_delay in shutdown_delay_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = shutdown_delay
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def moving_average_policy_segmented_busiest():
    param_name = 'k'
    #print_results_header(param_name)
    for k in [1]:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/busiest/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/day_cluster/2008_busiest_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = k
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ma', results)
        print_simple_results(results)
        #print_all_results(results)


def moving_average_policy_segmented_slowest():
    param_name = 'k'
    #print_results_header(param_name)
    for k in [1]:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/slowest/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/day_cluster/2008_slowest_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = k
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ma', results)
        print_simple_results(results)
        #print_all_results(results)


def moving_average_policy_segmented_mon_fri_semester():
    param_name = 'k'
    #print_results_header(param_name)
    for k in [1]:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/mon_fri_semester/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/day_cluster/2008_mon_fri_semester_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = k
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ma', results)
        print_simple_results(results)
        #print_all_results(results)


def moving_average_policy_segmented_winter_spring_break():
    param_name = 'k'
    #print_results_header(param_name)
    for k in [1]:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/winter_spring_break/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/day_cluster/2008_winter_spring_break_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = k
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ma', results)
        print_simple_results(results)
        #print_all_results(results)


def moving_average_policy_not_segmented():
    param_name = 'k'
    #print_results_header(param_name)
    for k in [1]:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ma_arrivals/arrivals_k_%s.txt' % k
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = k
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ma', results)
        print_simple_results(results)
        #print_all_results(results)


def exponential_moving_average():
    alpha_values = ['0.01', '0.02', '0.03', '0.04', '0.05', '0.06', '0.07', '0.08', '0.09', '0.1', '0.11', '0.12', '0.13', '0.14', '0.15', '0.16', '0.17', '0.18', '0.19', '0.2', '0.21', '0.22', '0.23', '0.24', '0.25', '0.26', '0.27', '0.28', '0.29', '0.3', '0.31', '0.32', '0.33', '0.34', '0.35', '0.36', '0.37', '0.38', '0.39', '0.4', '0.41', '0.42', '0.43', '0.44', '0.45', '0.46', '0.47', '0.48', '0.49', '0.5', '0.51', '0.52', '0.53', '0.54', '0.55', '0.56', '0.57', '0.58', '0.59', '0.6', '0.61', '0.62', '0.63', '0.64', '0.65', '0.66', '0.67', '0.68', '0.69', '0.7', '0.71', '0.72', '0.73', '0.74', '0.75', '0.76', '0.77', '0.78', '0.79', '0.8', '0.81', '0.82', '0.83', '0.84', '0.85', '0.86', '0.87', '0.88', '0.89', '0.9', '0.91', '0.92', '0.93', '0.94', '0.95', '0.96', '0.97', '0.98', '0.99', '1']
    param_name = 'alpha'
    print_results_header(param_name)
    for alpha in alpha_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = alpha
        results['param_name'] = param_name
        #write_bp_timescale_raw_to_file('ema', results)
        #print_simple_results(results)
        print_all_results(results)


def exponential_moving_average_policy_density_analysis():
    alpha_values = ['0.01', '0.05', '0.1', '0.3', '0.7', '0.9', '1']
    param_name = 'alpha'
    #print_results_header(param_name)
    for alpha in alpha_values:
        for density in [1, 2, 3, 4, 5]:
            scheduled = ErlangDataPolicyDataFileUserSim()
            pred_user_count_file_path = 'data/ema_arrivals/arrivals_alpha_%s.txt' % alpha
            users_data_file_path = 'data/2008_year_arrivals.txt'
            worst_bp = 0.01
            mu = 1 / 4957.567
            lag = 1
            scale_rate = 300
            shutdown_delay = 300
            results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
            # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
            results['param'] = alpha
            results['param_name'] = param_name
            results['density'] = density
            print_simple_results(results, timescale='weekly_99_percentile')
            #print_all_results(results)


def exponential_moving_average_policy_startup_delay_analysis():
    startup_delay_sigma_values = range(0, 401, 50)
    param_name = 'sigma'
    #print_results_header(param_name)
    alpha = 0.16
    density = 2
    for sigma in startup_delay_sigma_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        startup_mean = 300
        startup_delay_func = normal_distribution_startup_delay(startup_mean, sigma)
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = sigma
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def exponential_moving_average_policy_shutdown_delay_analysis():
    shutdown_delay_values = range(0, 1001, 100)
    param_name = 'shutdown_delay'
    #print_results_header(param_name)
    alpha = 0.16
    density = 2
    for shutdown_delay in shutdown_delay_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = shutdown_delay
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def exponential_moving_average_policy_segmented_busiest():
    param_name = 'alpha'
    #print_results_header(param_name)
    for alpha in ['0.16']:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/busiest/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/day_cluster/2008_busiest_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = alpha
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ema', results)
        print_simple_results(results)
        #print_all_results(results)


def exponential_moving_average_policy_segmented_slowest():
    param_name = 'alpha'
    #print_results_header(param_name)
    for alpha in ['0.16']:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/slowest/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/day_cluster/2008_slowest_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = alpha
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ema', results)
        print_simple_results(results)
        #print_all_results(results)


def exponential_moving_average_policy_segmented_mon_fri_semester():
    param_name = 'alpha'
    #print_results_header(param_name)
    for alpha in ['0.16']:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/mon_fri_semester/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/day_cluster/2008_mon_fri_semester_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = alpha
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ema', results)
        print_simple_results(results)
        #print_all_results(results)


def exponential_moving_average_policy_segmented_winter_spring_break():
    param_name = 'alpha'
    #print_results_header(param_name)
    for alpha in ['0.16']:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/day_cluster/winter_spring_break/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/day_cluster/2008_winter_spring_break_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = alpha
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ema', results)
        print_simple_results(results)
        #print_all_results(results)


def exponential_moving_average_policy_not_segmented():
    param_name = 'alpha'
    #print_results_header(param_name)
    for alpha in ['0.16']:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/ema_arrivals/arrivals_alpha_%s.txt' % alpha
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        density = 2
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = alpha
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('ema', results)
        print_simple_results(results)
        #print_all_results(results)


def autoregressive():
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/auto_regressive/yearlong_autoregressive_five_minute_counts.txt'
    users_data_file_path = 'data/2008_year_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 1
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'AR(2)'
    print_simple_results(results)
    #print_all_results(results)


def autoregressive_density():
    for density in [1, 2, 3, 4, 5]:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/auto_regressive/yearlong_autoregressive_five_minute_counts.txt'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = 'AR(2)'
        results['density'] = density
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def autoregressive_policy_startup_delay_analysis():
    startup_delay_sigma_values = range(0, 401, 50)
    param_name = 'sigma'
    #print_results_header(param_name)
    density = 2
    for sigma in startup_delay_sigma_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/auto_regressive/yearlong_autoregressive_five_minute_counts.txt'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        startup_mean = 300
        startup_delay_func = normal_distribution_startup_delay(startup_mean, sigma)
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = sigma
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def autoregressive_policy_shutdown_delay_analysis():
    shutdown_delay_values = range(0, 1001, 100)
    param_name = 'shutdown_delay'
    #print_results_header(param_name)
    density = 2
    for shutdown_delay in shutdown_delay_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/auto_regressive/yearlong_autoregressive_five_minute_counts.txt'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = shutdown_delay
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def autoregressive_policy_segmented_busiest():
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/day_cluster/busiest/autoregressive_five_min_counts.txt'
    users_data_file_path = 'data/day_cluster/2008_busiest_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 1
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'AR(2)'
    print_simple_results(results)
    #print_all_results(results)


def autoregressive_policy_segmented_slowest():
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/day_cluster/slowest/autoregressive_five_min_counts.txt'
    users_data_file_path = 'data/day_cluster/2008_slowest_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 1
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'AR(2)'
    print_simple_results(results)
    #print_all_results(results)


def autoregressive_policy_segmented_mon_fri_semester():
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/day_cluster/mon_fri_semester/autoregressive_five_min_counts.txt'
    users_data_file_path = 'data/day_cluster/2008_mon_fri_semester_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 1
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'AR(2)'
    print_simple_results(results)
    #print_all_results(results)


def autoregressive_policy_segmented_winter_spring_break():
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/day_cluster/winter_spring_break/autoregressive_five_min_counts.txt'
    users_data_file_path = 'data/day_cluster/2008_winter_spring_break_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 1
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'AR(2)'
    print_simple_results(results)
    #print_all_results(results)


def autoregressive_policy_not_segmented():
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/auto_regressive/yearlong_autoregressive_five_minute_counts.txt'
    users_data_file_path = 'data/2008_year_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 1
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'AR(2)'
    print_simple_results(results)
    #print_all_results(results)


def mixed_autoregressive():
    scheduled = ErlangDataPolicyDataFileUserSim()
    pred_user_count_file_path = 'data/auto_regressive/mixed_autoregressive_five_minute_counts.txt'
    users_data_file_path = 'data/2008_year_arrivals.txt'
    worst_bp = 0.01
    mu = 1 / 4957.567
    density = 2
    lag = 1
    scale_rate = 300
    shutdown_delay = 300
    results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
    # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
    results['param'] = 'mixed_AR(2)'
    print_simple_results(results)
    #print_all_results(results)


def mixed_autoregressive_density():
    for density in [1, 2, 3, 4, 5]:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/auto_regressive/mixed_autoregressive_five_minute_counts.txt'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = 'mixed_AR(2)'
        results['density'] = density
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def mixed_autoregressive_policy_startup_delay_analysis():
    startup_delay_sigma_values = range(0, 401, 50)
    param_name = 'sigma'
    #print_results_header(param_name)
    density = 2
    for sigma in startup_delay_sigma_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/auto_regressive/mixed_autoregressive_five_minute_counts.txt'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        shutdown_delay = 300
        startup_mean = 300
        startup_delay_func = normal_distribution_startup_delay(startup_mean, sigma)
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = sigma
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def mixed_autoregressive_policy_shutdown_delay_analysis():
    shutdown_delay_values = range(0, 1001, 100)
    param_name = 'shutdown_delay'
    #print_results_header(param_name)
    density = 2
    for shutdown_delay in shutdown_delay_values:
        scheduled = ErlangDataPolicyDataFileUserSim()
        pred_user_count_file_path = 'data/auto_regressive/mixed_autoregressive_five_minute_counts.txt'
        users_data_file_path = 'data/2008_year_arrivals.txt'
        worst_bp = 0.01
        mu = 1 / 4957.567
        lag = 1
        scale_rate = 300
        results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, three_hundred_second_startup_delay, shutdown_delay)
        # results = scheduled.run(worst_bp, pred_user_count_file_path, mu, users_data_file_path, lag, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = shutdown_delay
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def select_model_from_environment_var():
    method_name = os.getenv('METHOD_TO_RUN', None)
    if method_name:
        fun = getattr(sys.modules[__name__], method_name)
        fun()


if __name__ == "__main__":
    if not os.path.isfile('scheduled_capacity.py'):
        print 'Please run in the same directory as scheduled_capacity.py'
        exit()

    #poisson_traffic_known_in_advance()

    #fixed_policy_user_arrivals()
    #fixed_policy_density_analysis()

    #fixed_policy_segmented_busiest()
    #fixed_policy_segmented_slowest()
    #fixed_policy_segmented_mon_fri_semester()
    #fixed_policy_segmented_winter_spring_break()
    #fixed_policy_not_segmented()

    #moving_average()
    #moving_average_policy_density_analysis()
    #moving_average_policy_startup_delay_analysis()
    #moving_average_policy_shutdown_delay_analysis()

    #moving_average_policy_segmented_busiest()
    #moving_average_policy_segmented_slowest()
    #moving_average_policy_segmented_mon_fri_semester()
    #moving_average_policy_segmented_winter_spring_break()
    #moving_average_policy_not_segmented()

    #exponential_moving_average()
    #exponential_moving_average_policy_density_analysis()
    #exponential_moving_average_policy_startup_delay_analysis()
    #exponential_moving_average_policy_shutdown_delay_analysis()

    #exponential_moving_average_policy_segmented_busiest()
    #exponential_moving_average_policy_segmented_slowest()
    #exponential_moving_average_policy_segmented_mon_fri_semester()
    #exponential_moving_average_policy_segmented_winter_spring_break()
    #exponential_moving_average_policy_not_segmented()

    #autoregressive()
    #autoregressive_density()
    #autoregressive_policy_startup_delay_analysis()
    #autoregressive_policy_shutdown_delay_analysis()

    #autoregressive_policy_segmented_busiest()
    #autoregressive_policy_segmented_slowest()
    #autoregressive_policy_segmented_mon_fri_semester()
    #autoregressive_policy_segmented_winter_spring_break()
    #autoregressive_policy_not_segmented()

    #mixed_autoregressive()
    #mixed_autoregressive_density()
    #mixed_autoregressive_policy_startup_delay_analysis()
    #mixed_autoregressive_policy_shutdown_delay_analysis()

    #select_model_from_environment_var()
