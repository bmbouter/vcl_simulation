import os

from appsim.sim import ReservePolicyDataFileUserSim, TimeVaryReservePolicyDataFileUserSim
from common import fixed_startup_delay, normal_distribution_startup_delay
from output_utils import print_results_header, print_all_results, print_simple_results

reserved = 2
scale_rate = 1
lamda = 5
mu = 1
startup_delay = 0
shutdown_delay = 0
num_customers = 50000

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


def reserve_policy_data_file_user_sim():
    reserve = ReservePolicyDataFileUserSim()
    users_data_file_path = 'data/2008_year_arrivals.txt'
    results = reserve.run(5, users_data_file_path, 2, 300, three_hundred_second_startup_delay, 300)
    #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
    print results


def reserve_policy_data_file_user_sim_table():
    param_name = 'R'
    print_results_header(param_name)
    for R in range(1,41):
        reserve = ReservePolicyDataFileUserSim()
        users_data_file_path = 'data/2008_year_arrivals.txt'
        results = reserve.run(R, users_data_file_path, 2, 300, three_hundred_second_startup_delay, 300)
        #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = R
        results['param_name'] = param_name
        write_bp_timescale_raw_to_file('reserve', results)
        #print_simple_results(results)
        print_all_results(results)


def reserve_policy_data_file_user_sim_density_analysis():
    param_name = 'R'
    #print_results_header(param_name)
    for R in [5, 10, 15, 20]:
        for density in [1, 2, 3, 4, 5]:
            reserve = ReservePolicyDataFileUserSim()
            users_data_file_path = 'data/2008_year_arrivals.txt'
            results = reserve.run(R, users_data_file_path, density, 300, three_hundred_second_startup_delay, 300)
            #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
            results['param'] = R
            results['param_name'] = param_name
            results['density'] = density
            print_simple_results(results, timescale='weekly_99_percentile')
            #print_all_results(results)


def reserve_policy_data_file_user_sim_startup_delay_analysis():
    startup_delay_sigma_values = range(0, 401, 50)
    param_name = 'sigma'
    #print_results_header(param_name)
    R = 32
    density = 2
    for sigma in startup_delay_sigma_values:
        reserve = ReservePolicyDataFileUserSim()
        users_data_file_path = 'data/2008_year_arrivals.txt'
        scale_rate = 300
        shutdown_delay = 300
        startup_mean = 300
        startup_delay_func = normal_distribution_startup_delay(startup_mean, sigma)
        results = reserve.run(R, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
        #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
        results['param'] = sigma
        results['param_name'] = param_name
        print_simple_results(results, timescale='weekly_99_percentile')
        #print_all_results(results)


def time_vary_reserve_policy_data_file_user_sim_table():
    param_name = 'percentile,window_size'
    print_results_header(param_name)
    five_minute_counts_file = 'data/2008_five_minute_counts.csv'
    users_data_file_path = 'data/2008_year_arrivals.txt'
    for percentile in [0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99, 1.0]:
        for window_size in [12, 144, 288, 2016]:
            time_vary_reserve = TimeVaryReservePolicyDataFileUserSim()
            results = time_vary_reserve.run(window_size, percentile, five_minute_counts_file, users_data_file_path, 2, 300, three_hundred_second_startup_delay, 300)
            #reserve.run(window_size, arrival_percentile, five_minute_counts_file, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
            results['param'] = '%s,%s' % (percentile, window_size)
            #results['param_name'] = '%s,%s' % ('percentile', param_name)
            #write_bp_timescale_raw_to_file('reserve', results)
            #print_simple_results(results)
            print_all_results(results)


def time_vary_reserve_policy_data_file_user_sim_density_analysis():
    param_name = 'Q'
    W = 288
    five_minute_counts_file = 'data/2008_five_minute_counts.csv'
    users_data_file_path = 'data/2008_year_arrivals.txt'
    #print_results_header(param_name)
    for Q in [0.9, 0.925, 0.95, 0.975, 1.0]:
        for density in [1, 2, 3, 4, 5]:
            time_vary_reserve = TimeVaryReservePolicyDataFileUserSim()
            results = time_vary_reserve.run(W, Q, five_minute_counts_file, users_data_file_path, density, 300, three_hundred_second_startup_delay, 300)
            #reserve.run(window_size, arrival_percentile, five_minute_counts_file, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
            results['param'] = Q
            results['param_name'] = param_name
            results['density'] = density
            print_simple_results(results)
            #print_all_results(results)


if __name__ == "__main__":
    if not os.path.isfile('reserve_capacity.py'):
        print 'Please run in the same directory as reserve_capacity.py'
        exit()
    #reserve_policy_data_file_user_sim()
    #reserve_policy_data_file_user_sim_table()
    #reserve_policy_data_file_user_sim_density_analysis()
    #reserve_policy_data_file_user_sim_startup_delay_analysis()

    #time_vary_reserve_policy_data_file_user_sim_table()
    #time_vary_reserve_policy_data_file_user_sim_density_analysis()
