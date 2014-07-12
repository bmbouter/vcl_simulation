import os

from appsim.sim import DataFilePolicyDataFileUserSim
from appsim.sim import ReservePolicyDataFileUserSim
from common import fixed_startup_delay
from data.traffic_transforms.traffic_transforms import traffic_condition_iterator


zero_second_startup_delay = fixed_startup_delay(0)
three_hundred_second_startup_delay = fixed_startup_delay(300)


def print_header():
    print 'arrival_scale,service_scale,weekly_99th_bp'


def fixed_capacity():
    capacity = 168
    print_header()
    for arrival_scale, service_scale, filename in traffic_condition_iterator():
        scheduled = DataFilePolicyDataFileUserSim()
        prov_data_file_path = 'data/1_server_prov_schedule.csv'
        users_data_file_path = 'data/traffic_transforms/%s' % filename
        results = scheduled.run(prov_data_file_path, users_data_file_path, capacity, zero_second_startup_delay, 0)
        #results = scheduled.run(prov_data_file_path, users_data_file_path, density, startup_delay_func, shutdown_delay)
        print '%s,%s,%s' % (arrival_scale, service_scale, results['bp_by_week']['bp_99percentile'])


def reserve_capacity():
    R = 32
    print_header()
    for arrival_scale, service_scale, filename in traffic_condition_iterator():
        reserve = ReservePolicyDataFileUserSim()
        users_data_file_path = 'data/traffic_transforms/%s' % filename
        results = reserve.run(R, users_data_file_path, 2, 300, three_hundred_second_startup_delay, 300)
        #reserve.run(reserved, users_data_file_path, density, scale_rate, startup_delay_func, shutdown_delay)
        print '%s,%s,%s' % (arrival_scale, service_scale, results['bp_by_week']['bp_99percentile'])


if __name__ == "__main__":
    if not os.path.isfile('traffic_variations.py'):
        print 'Please run in the same directory as traffic_variations.py'
        exit()
    #fixed_capacity()
    #reserve_capacity()
